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

import datetime
import sys
import time

from mongo_connector.command_helper import CommandHelper
from mongo_connector.doc_managers.kdb_doc_manager import DocManager
from mongo_connector.test_utils import TESTARGS

sys.path[0:0] = [""]

from tests import unittest, kdb_url
from tests.test_kdb import KdbTestCase

class TestKdbDocManager(KdbTestCase):
    """Test class for KdbDocManager
    """

    def setUp(self):
        """Empty KDB at the start of every test
        """
        self._remove()

    def test_update(self):
        doc_id = '1'
        doc = {"id": doc_id, "title": "abc", "description": "def"}
        self.docman.upsert(doc, *TESTARGS)
        # $set only
        update_spec = {"$set": {"title": "qaz", "description": "wsx"}}
        doc = self.docman.update(doc_id, update_spec, *TESTARGS)
        expected = {"id": doc_id, "title": "qaz", "description": "wsx"}
        for k, v in expected.items():
            self.assertEqual(doc[k], v)

        # $unset only
        update_spec = {"$unset": {"title": True}}
        doc = self.docman.update(doc_id, update_spec, *TESTARGS)
        expected = {"id": '1', "description": "wsx"}
        for k, v in expected.items():
            self.assertEqual(doc[k], v)
        self.assertNotIn("title", doc)

        # mixed $set/$unset
        update_spec = {"$unset": {"description": True},
                       "$set": {"subject": "edc"}}
        doc = self.docman.update(doc_id, update_spec, *TESTARGS)
        expected = {"id": '1', "subject": "edc"}
        for k, v in expected.items():
            self.assertEqual(doc[k], v)
        self.assertNotIn("description", doc)

    def test_replacement_unique_key(self):
        docman = DocManager(kdb_url, unique_key='id')
        # Document coming from kdb. 'id' is the unique key.
        from_kdb = {'id': 1, 'title': 'unique key replacement test'}
        # Replacement coming from an oplog entry in MongoDB. Still has '_id'.
        replacement = {'id': 1, 'title': 'unique key replaced!'}
        replaced = docman.apply_update(from_kdb, replacement)
        self.assertEqual('unique key replaced!', replaced['title'])

    def test_upsert(self):
        """Ensure we can properly insert into Kdb via DocManager.
        """
        #test upsert
        docc = {'id': '1', 'name': 'John'}
        self.docman.upsert(docc, *TESTARGS)
        res = self.kdb_conn.sync('select from .test.test')
        for doc in res:
            self.assertTrue(doc is not None)
            self.assertTrue(doc['id'] == '1' and doc['name'] == 'John')

        docc = {'id': '1', 'name': 'Paul'}
        self.docman.upsert(docc, *TESTARGS)
        res = self.kdb_conn.sync('select from .test.test')
        for doc in res:
            self.assertTrue(doc['id'] == '1' and doc['name'] == 'Paul')

    def test_bulk_upsert(self):
        """Ensure we can properly insert many documents at once into
        Kdb via DocManager

        """
        self.docman.bulk_upsert([], *TESTARGS)

        docs = ({"id": i} for i in range(1000))
        self.docman.bulk_upsert(docs, *TESTARGS)

        res = sorted(int(x["id"])
                     for x in self.kdb_conn.sync('?[.test.test;enlist(<;`i;1001);0b;()]'))
        self.assertEqual(len(res), 1000)
        for i, r in enumerate(res):
            self.assertEqual(r, i)

        docs = ({"id": i, "weight": 2*i} for i in range(1000))
        self.docman.bulk_upsert(docs, *TESTARGS)

        res = sorted(int(x["weight"])
                     for x in self.kdb_conn.sync('?[.test.test;enlist(<;`i;1001);0b;()]'))
        self.assertEqual(len(res), 1000)
        for i, r in enumerate(res):
            self.assertEqual(r, 2*i)

    def test_remove(self):
        """Ensure we can properly delete from kdb via DocManager.
        """
        #test remove
        docc = {'id': '1', 'name': 'John'}
        self.docman.upsert(docc, *TESTARGS)
        res = self.kdb_conn.sync('select from .test.test')
        self.assertEqual(len(res), 1)

        self.docman.remove(docc['id'], *TESTARGS)
        res = self.kdb_conn.search('()')
        self.assertEqual(len(res), 0)

    def test_search(self):
        """Query KDB for docs in a timestamp range.

        We use API and DocManager's search(start_ts,end_ts), and then compare.
        """
        #test search
        docc = {'id': '1', 'name': 'John'}
        self.docman.upsert(docc, 'test.test', 5767301236327972865)
        docc = {'id': '2', 'name': 'John Paul'}
        self.docman.upsert(docc, 'test.test', 5767301236327972866)
        docc = {'id': '3', 'name': 'Paul'}
        self.docman.upsert(docc, 'test.test', 5767301236327972870)
        search = list(self.docman.search(5767301236327972865,
                                         5767301236327972866))
        self.assertEqual(2, len(search),
                         'Should find two documents in timestamp range.')
        result_names = [result.get("name") for result in search]
        self.assertIn('John', result_names)
        self.assertIn('John Paul', result_names)

    def test_get_last_doc(self):
        """Insert documents, Verify the doc with the latest timestamp.
        """
        #test get last doc
        docc = {'id': '4', 'name': 'Hare'}
        self.docman.upsert(docc, 'test.test', 2)
        docc = {'id': '5', 'name': 'Tortoise'}
        self.docman.upsert(docc, 'test.test', 1)
        doc = self.docman.get_last_doc()
        self.assertTrue(doc is not None)
        self.assertTrue(doc['id'] == '4')

        docc = {'id': '6', 'name': 'HareTwin', 'ts': '2'}
        doc = self.docman.get_last_doc()
        self.assertTrue(doc['id'] == '4' or doc['id'] == '6')

    def test_commands(self):
        self.docman.command_helper = CommandHelper()

        def count_ns(ns):
            return sum(1 for _ in self._search("ns:%s" % ns))

        self.docman.upsert({'id': '1', 'test': 'data'}, *TESTARGS)
        self.assertEqual(count_ns("test.test"), 1)

        self.docman.handle_command({'drop': 'test'}, *TESTARGS)
        time.sleep(1)
        self.assertEqual(count_ns("test.test"), 0)

        self.docman.upsert({'id': '2', 'test': 'data'}, 'test.test2', '2')
        self.docman.upsert({'id': '3', 'test': 'data'}, 'test.test3', '3')
        self.docman.handle_command({'dropDatabase': 1}, 'test.$cmd', 1)
        time.sleep(1)
        self.assertEqual(count_ns("test.test2"), 0)
        self.assertEqual(count_ns("test.test3"), 0)


if __name__ == '__main__':
    unittest.main()
