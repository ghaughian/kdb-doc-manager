# -*- encoding: utf-8 -*-
# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test KDB using the synchronizer, i.e. as it would be used by an user
"""

import logging
import os
import sys
import time

from bson import SON

from mongo_connector.compat import u
from mongo_connector.connector import Connector
from mongo_connector.test_utils import ReplicaSet, assert_soon
from mongo_connector.util import retry_until_ok

from qpython import qconnection
from qpython.qtype import QException 

sys.path[0:0] = [""]

from mongo_connector.doc_managers.kdb_doc_manager import DocManager
from tests import unittest, kdb_url


class KdbTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kdb_conn = qconnection.QConnection(host=kdb_url.split(':')[0], port=int(kdb_url.split(':')[1])) 
        cls.kdb_conn.open()
        cls.docman = DocManager(kdb_url, unique_key='id')

    def setUp(self):
        # Create test database in KDB+ 
        self.kdb_conn.sync("`.test.test set ([]id:(); ts:`long$(); ns:(); name:(); title:(); description:(); subject:(); data:(); a:`int$(); b_0_c:`int$(); b_10_c:`int$(); b_0_e:`int$(); b_1_d:`int$(); b_1_f:`int$(); b_2_e:`int$(); billing_address_street:(); billing_address_state:(); numbers_0:(); numbers_1:(); numbers_2:(); characters_0_name:(); characters_0_color:(); characters_1_name:(); characters_1_color:(); characters_2:(); popularity:`int$());")

    def _search(self, query):
        return self.docman._stream_search(query)

    def _remove(self):
        self.kdb_conn.sync("![`.test;();0b;enlist`test];`.test.test set ([]id:(); ts:`long$(); ns:(); name:(); title:(); description:(); subject:(); data:(); a:`int$(); b_0_c:`int$(); b_10_c:`int$(); b_0_e:`int$(); b_1_d:`int$(); b_1_f:`int$(); b_2_e:`int$(); billing_address_street:(); billing_address_state:(); numbers_0:(); numbers_1:(); numbers_2:(); characters_0_name:(); characters_0_color:(); characters_1_name:(); characters_1_color:(); characters_2:(); popularity:`int$());")
        

class TestKdb(KdbTestCase):
    """ Tests Kdb
    """

    @classmethod
    def setUpClass(cls):
        KdbTestCase.setUpClass()
        cls.repl_set = ReplicaSet().start()
        cls.conn = cls.repl_set.client()

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        cls.repl_set.stop()

    def setUp(self):
        self._remove()
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        open("oplog.timestamp", "w").close()
        docman = DocManager(kdb_url, unique_key='id')
        self.connector = Connector(
            mongo_address=self.repl_set.uri,
            ns_set=['test.test'],
            doc_managers=(docman,),
        )
        retry_until_ok(self.conn.test.test.drop)
        self._remove()
        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)

    def tearDown(self):
        self.connector.join()

    def test_insert(self):
        """Tests insert
        """

        self.conn['test']['test'].insert_one({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.kdb_conn.sync('?[.test.test;();0b;()]')) > 0)
        result_set_1 = self.kdb_conn.sync('?[.test.test;enlist(~\:;`name;"paulie");0b;()]')
        self.assertEqual(len(result_set_1), 1)
        result_set_2 = self.conn['test']['test'].find_one()
        for item in result_set_1:
            doc = {} 
            for k, v in item.items():
                doc[k] = v
            self.assertEqual(doc['id'], str(result_set_2['_id']))
            self.assertEqual(doc['name'], result_set_2['name'])

    def test_remove(self):
        """Tests remove
        """
        self.conn['test']['test'].insert_one({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.kdb_conn.sync("?[.test.test;();0b;()]")) == 1)
        self.conn['test']['test'].delete_one({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.kdb_conn.sync("?[.test.test;();0b;()]")) == 0)

    def test_update(self):
        """Test update operations on Kdb.

        Need to have the following fields defined in schema.q:

        a:`int$(); b_0_c:`int$(); b_10_c:`int$(); b_0_e:`int$(); 
        b_1_d:`int$(); b_1_f:`int$(); b_2_e:`int$()
        """
        docman = self.connector.doc_managers[0]

        self.conn.test.test.insert_one({"a": 0})
        assert_soon(lambda: sum(1 for _ in self._search("()")) == 1)

        def check_update(update_spec):
            updated = self.conn.test.command(
                SON([('findAndModify', 'test'),
                     ('query', {"a": 0}),
                     ('update', update_spec),
                     ('new', True)]))['value']

            # Stringify _id to match what will be retrieved from Kdb
            updated[u('_id')] = u(updated['_id'])
            # Flatten the MongoDB document to match Kdb
            updated = docman._clean_doc(updated, 'dummy.namespace', 0)

            def update_worked():
                replicated = list(self._search("ns:test.test;a=0"))[0]
                return replicated == updated

            # Allow some time for update to propagate
            assert_soon(update_worked)

        # Update by adding a field.
        # Note that Kdb can't mix types within an array
        check_update({"$set": {"b": [{"c": 10}, {"d": 11}]}})

        # Update by setting an attribute of a sub-document beyond end of array.
        check_update({"$set": {"b.10.c": 42}})

        # Update by changing a value within a sub-document (contains array)
        check_update({"$inc": {"b.0.c": 1}})

        # Update by changing the value within an array
        check_update({"$inc": {"b.1.f": 12}})

        # Update by adding new bucket to list
        check_update({"$push": {"b": {"e": 12}}})

        # Update by replacing an entire sub-document
        check_update({"$set": {"b.0": {"e": 4}}})

        # Update by adding a sub-document
        check_update({"$set": {"b": {"0": {"c": 100}}}})

        # Update whole document
        check_update({"a": 0, "b": {"1": {"d": 10000}}})

    def test_rollback(self):
        """Tests rollback. We force a rollback by inserting one doc, killing
            primary, adding another doc, killing the new primary, and
            restarting both the servers.
        """

        primary_conn = self.repl_set.primary.client()

        self.conn['test']['test'].insert_one({'name': 'paul'})
        assert_soon(
            lambda: self.conn.test.test.find({'name': 'paul'}).count() == 1)
        assert_soon(
            lambda: sum(1 for _ in self.kdb_conn.sync('?[`.test.test;();0b;()]')) == 1)
        self.repl_set.primary.stop(destroy=False)

        new_primary_conn = self.repl_set.secondary.client()
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        retry_until_ok(self.conn.test.test.insert_one, {'name': 'pauline'})
        assert_soon(lambda: sum(1 for _ in self.kdb_conn.sync('?[`.test.test;();0b;()]')) == 2)

        result_set_1 = list(self.kdb_conn.sync('?[`.test.test;enlist(~\:;`name;"pauline");0b;()]'))
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
        self.repl_set.secondary.stop(destroy=False)

        self.repl_set.primary.start()

        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        self.repl_set.secondary.start()

        time.sleep(2)
        result_set_1 = self.kdb_conn.sync('?[`.test.test;enlist(~\:;`name;"pauline");0b;()]')
        self.assertEqual(sum(1 for _ in result_set_1), 0)
        result_set_2 = self.kdb_conn.sync('?[`.test.test;enlist(~\:;`name;"paul");0b;()]')
        self.assertEqual(sum(1 for _ in result_set_2), 1)

    def test_valid_fields(self):
        """ Tests documents with field definitions
        """
        inserted_obj = self.conn['test']['test'].insert_one(
            {'name': 'test_valid'}).inserted_id
        self.conn['test']['test'].update_one(
            {'_id': inserted_obj},
            {'$set': {'popularity': 1}}
        )

        docman = self.connector.doc_managers[0]
        assert_soon(lambda: sum(1 for _ in self._search("()")) > 0)
        result = docman.get_last_doc()
        self.assertIn('popularity', result)
        self.assertEqual(sum(1 for _ in self._search("name ~\:\"test_valid\"")), 1)

    def test_invalid_fields(self):
        """ Tests documents without field definitions
        """
        inserted_obj = self.conn['test']['test'].insert_one(
            {'name': 'test_invalid'}).inserted_id
        self.conn['test']['test'].update_one(
            {'_id': inserted_obj},
            {'$set': {'break_this_test': 1}}
        )

        docman = self.connector.doc_managers[0]
        assert_soon(lambda: sum(1 for _ in self._search("()")) > 0)

        result = docman.get_last_doc()
        self.assertNotIn('break_this_test', result)
        self.assertEqual(sum(1 for _ in self._search(
            "name ~\:\"test_invalid\"")), 1)

    def test_nested_fields(self):
        """Test indexing fields that are sub-documents in MongoDB

        The following fields are defined in the provided schema.q:

        billing_address_street:(); billing_address_state:();
        numbers_0:(); numbers_1:(): numbers_2:()
        characters_0_name:(); characters_0_color:();
        characters_1_name:(); characters_1_color:(); 
        characters_2:()
        """

        # Connector is already running
        self.conn["test"]["test"].insert_one({
            "name": "Jeb",
            "billing": {
                "address": {
                    "street": "12345 Mariposa Street",
                    "state": "California"
                }
            }
        })
        self.conn["test"]["test"].insert_one({
            "numbers": ["one", "two", "three"],
            "characters": [
                {"name": "Big Bird",
                 "color": "yellow"},
                {"name": "Elmo",
                 "color": "red"},
                "Cookie Monster"
            ]
        })

        assert_soon(lambda: sum(1 for _ in self.kdb_conn.sync("?[`.test.test;();0b;()]")) > 0,
                    "documents should have been replicated to Kdb")

        # Search for first document
        results = self.kdb_conn.sync("?[`.test.test;enlist(~\:;`billing_address_street;\"12345 Mariposa Street\");0b;()]")
        self.assertEqual(len(results), 1)
        self.assertEqual(next(iter(results))["billing_address_state"],
                         "California")

        # Search for second document
        results = self.kdb_conn.sync("?[`.test.test;enlist(~\:;`characters_1_color;\"red\");0b;()]")
        self.assertEqual(len(results), 1)
        self.assertEqual(next(iter(results))["numbers.2"], "three")
        results = self.kdb_conn.sync("?[`.test.test;enlist(~\:;`characters_2;\"Cookie Monster\");0b;()]")
        self.assertEqual(len(results), 1)

if __name__ == '__main__':
    unittest.main()
