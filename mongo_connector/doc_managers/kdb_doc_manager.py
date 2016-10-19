# Author: Gerard Haughian 
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

"""Receives documents from the oplog worker threads and indexes them
into the backend.

This file is a document manager for Kdb+.
"""
import json
import logging
import os
import re

from itertools import izip

from qpython import qconnection
from qpython.qcollection import qlist, QDictionary
from qpython.qtype import * 

from mongo_connector import errors
from mongo_connector.compat import u
from mongo_connector.constants import DEFAULT_MAX_BULK
from mongo_connector.compat import (
    Request, urlopen, urlencode, URLError, HTTPError)
from mongo_connector.util import exception_wrapper, retry_until_ok
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DocumentFlattener

wrap_exceptions = exception_wrapper({
    QException: errors.OperationFailed,
    URLError: errors.ConnectionFailed,
    HTTPError: errors.ConnectionFailed
})


decoder = json.JSONDecoder()


class DocManager(DocManagerBase):
    """The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(self, url, unique_key='id', chunk_size=DEFAULT_MAX_BULK, **kwargs):
        """Verify KDB URL and establish a connection.
        """
        self.url = url
        self.q = qconnection.QConnection(host=url.split(':')[0], port=int(url.split(':')[1])) 
        self.q.open()
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self.field_list = {}
        self.field_conversion = {}
        self._formatter = DocumentFlattener()

    def _convert_types(self, field_conversion, field, value):
        if not field_conversion.has_key(field):
            return value.encode('utf-8',errors='ignore')
        elif 'b' == field_conversion[field]:
            return numpy.bool(value)
        elif 'x' == field_conversion[field]:
            return numpy.byte(value)
        elif 'h' == field_conversion[field]:
            return numpy.int16(value)
        elif 'i' == field_conversion[field]:
            return numpy.int32(value)
        elif 'j' == field_conversion[field]:
            return numpy.int64(value)
        elif 'e' == field_conversion[field]:
            return numpy.float32(value)
        elif 'f' == field_conversion[field]:
            return numpy.float64(value)
        elif 's' == field_conversion[field]:
            return numpy.string_(value)
        elif field_conversion[field] in 'p': 
            return numpy.datetime64[ns](value)
        elif field_conversion[field] in 'm': 
            return numpy.datetime64[M](value)
        elif field_conversion[field] in 'd': 
            return numpy.datetime64[D](value)
        elif field_conversion[field] in 'z': 
            return numpy.datetime64[ms](value)
        elif field_conversion[field] in 'n': 
            return numpy.timedelta64[ns](value)
        elif field_conversion[field] in 'u': 
            return numpy.timedelta64[m](value)
        elif field_conversion[field] in 'v': 
            return numpy.timedelta64[s](value)
        elif field_conversion[field] in 't': 
            return numpy.timedelta64[ms](value)
        else:
            return value.encode('utf-8',errors='ignore')

    def _clean_doc(self, doc, namespace, timestamp):
        """Reformats the given document before insertion into KDB.

        This method reformats the document in the following ways:
          - removes extraneous fields that aren't defined in schema.xml
          - unwinds arrays in order to find and later flatten sub-documents
          - flattens the document so that there are no sub-documents, and every
            value is associated with its dot-separated path of keys
          - inserts namespace and timestamp metadata into the document in order
            to handle rollbacks

        An example:
          {"a": 2,
           "b": { "c": { "d": 5 } },
           "e": [6, 7, 8]
          }

        becomes:
          {"a": 2, "b_c_d": 5, "e_0": 6, "e_1": 7, "e_2": 8}

        """

        # Translate the _id field to whatever unique key we're using.
        # _id may not exist in the doc, if we retrieved it from KDB
        # as part of update.
        if '_id' in doc:
            doc[self.unique_key] = u(doc.pop("_id"))

        # Update namespace and timestamp metadata
        if 'ns' in doc or 'ts' in doc:
            raise errors.OperationFailed(
                'Need to set "ns" and "ts" fields, but these fields already '
                'exist in the document %r!' % doc)
        doc['ns'] = namespace.encode('UTF8')
        doc['ts'] = u(timestamp)

        # KDB cannot store fields within sub-documents, so flatten documents
        # with the dot-separated path to each value as the respective key
        flat_doc = self._formatter.format_document(doc)

        if not self.field_list.has_key(namespace):
            self.field_list[namespace] = list(u(f) for f in self.q.sync('exec c from meta .{}'.format(namespace)))
            self.field_conversion[namespace] = dict(zip(list(self.q.sync('exec c from meta .{} where not t in (" cC")'.format(namespace))), list(self.q.sync('exec t from meta .{} where not t in (" cC")'.format(namespace)))))

        # Only include fields that are explicitly provided in the schema 
        field_list = self.field_list[namespace]
        field_conversion = self.field_conversion[namespace]
        if len(field_list) > 0:
            flat_doc = dict((k.replace('.','_',10), self._convert_types(field_conversion,k,v)) for k, v in flat_doc.items() if k in field_list)
            print("#### flattened doc looks like: {}".format(flat_doc))
            return flat_doc
        return flat_doc

    def stop(self):
        """ Stops the instance
        """
        pass

    @wrap_exceptions
    def handle_command(self, doc, namespace, timestamp):
        db, _ = namespace.split('.', 1)
        if doc.get('dropDatabase'):
            for new_db in self.command_helper.map_db(db):
                self.q.sync('![`.{0};();0b;]each enlist each tables[`.{0}]'.format(new_db))

        if doc.get('renameCollection'):
            raise errors.OperationFailed(
                "kdb_doc_manager does not support replication of "
                " renameCollection")

        if doc.get('create'):
            # nothing to do
            pass

        if doc.get('drop'):
            new_db, coll = self.command_helper.map_collection(db, doc['drop'])
            if new_db:
                self.q.sync('![`.{0};();0b;enlist `{1}]'.format(new_db,coll))

    def apply_update(self, doc, update_spec, namespace=''):
        """Override DocManagerBase.apply_update to have flat documents."""
        # Replace a whole document
        if not '$set' in update_spec and not '$unset' in update_spec:
            # update_spec contains the new document.
            # Update the key in kdb based on the unique_key mentioned as
            # parameter.
            update_spec['_id'] = doc[self.unique_key]
            return update_spec
        for to_set in update_spec.get("$set", []):
            value = update_spec['$set'][to_set]
            field_list = self.field_list[namespace]
            # Find dotted-path to the value, remove that key from doc, then
            # put value at key:
            keys_to_pop = []
            if to_set in field_list:
                for key in doc:
                    if key.startswith(to_set):
                        if key == to_set or key[len(to_set)] == '.':
                            keys_to_pop.append(key)
                for key in keys_to_pop:
                    doc.pop(key)
                doc[to_set] = value
        for to_unset in update_spec.get("$unset", []):
            # MongoDB < 2.5.2 reports $unset for fields that don't exist within
            # the document being updated.
            keys_to_pop = []
            for key in doc:
                if key.startswith(to_unset):
                    if key == to_unset or key[len(to_unset)] == '.':
                        keys_to_pop.append(key)
            for key in keys_to_pop:
                doc.pop(key)
        return doc

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.
        """
        # first select the row that matchs and drop the `ns and `ts columns
        results = self.q.sync('first ![;();0b;`ns`ts]?[`.{0};enlist(~\:;`{1};"{2}");0b;();1]'.format(namespace, self.unique_key, u(document_id)))
        doc = {}
        if isinstance(results, QDictionary):
            for k, v in results.items():
                doc[k]=v
        # Results should be a KDB dict containing only 1 result
        #for row in results:
            print("######## row to update: {}".format(doc))
            updated = self.apply_update(doc, update_spec, namespace)
            self.upsert(updated, namespace, timestamp)
            return updated

    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp):
        """Update or insert a document into KDB

        This method should call whatever add/insert/update method exists for
        the backend engine and add the document in there. The input will
        always be one mongo document, represented as a Python dictionary.
        """
        doc = self._clean_doc(doc, namespace, timestamp)
        self.q.sync('insert', numpy.string_('.{}'.format(namespace)), QDictionary(qlist([numpy.string_(x) for x in doc.keys()],qtype=QSYMBOL_LIST), doc.values()))

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp):
        """Update or insert multiple documents into KDB

        docs may be any iterable
        """
        if not docs:
            print("########## NO DOCS!!!! #########")
            return
        cleaned = (self._clean_doc(d, namespace, timestamp) for d in docs)
        if self.chunk_size > 0:
            batch = list(next(cleaned) for i in range(self.chunk_size))
            while batch:
                print('#### in the bulk_upsert_1 function ####')
                for rec in batch:
                    print(rec)
                    self.q.sync('insert', numpy.string_('.{}'.format(namespace)), QDictionary(qlist([numpy.string_(x) for x in rec.keys()],qtype=QSYMBOL_LIST), rec.values()))
                    #self.q.sync('insert', numpy.string_('.{}'.format(namespace)), QDictionary(qlist([numpy.string_(x) for x in doc.keys()],qtype=QSYMBOL_LIST), doc.values()))
                    #self.q.sync('insert', '.{}'.format(namespace), QDictionary(qlist(rec.keys(),qtype=QGENERAL_LIST), qlist([x.encode('utf-8') for x in rec.values()],qtype=QGENERAL_LIST)))
                batch = list(next(cleaned)
                             for i in range(self.chunk_size))
        else:
            #self.q.sync('insert', namespace, QDictionary(qlist(cleaned.keys(),qtype=QSYMBOL_LIST), cleaned.values()))
            print('#### in the bulk_upsert_2 function ####')
            self.q.sync('insert', '.{}'.format(namespace), cleaned)


    @wrap_exceptions
    def insert_file(self, f, namespace, timestamp):
        raise errors.OperationFailed("kdb_doc_manager does not support replication of insert_file")

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        """Removes documents from KDB+

        The input is a python dictionary that represents a mongo document.
        """
        self.q.sync('![`.{0};enlist(~\:;`{1};\"{2}\");0b;`symbol$()]'.format(namespace, self.unique_key, u(document_id)))
        return 0

    def _ns_and_qry(self, query):
        """Helper method for getting the kdb database name to apply query to.
             e.g. query = "ns:test.nest;a=0"
        """
        ns, qry = query.split('ns:',1)[1].split(';', 1)
        if qry is None:
           qry='()'
        return ns.lower(), qry

    def _get_tables(self):
        return self.q.sync('raze {`$(string[x],"."),/:string tables[x]} each `$".",/:string except[key`;`q`Q`h`j`o]')
    
    @wrap_exceptions
    def _stream_search(self, query):
        """Helper method for iterating over KDB+ results."""
        kdb_query=''
        if query.startswith('ns:'):
            ns, qry = self._ns_and_qry(query)
            if '()' == qry:
                kdb_query = '?[`.{0};();0b;();1000000]'.format(ns)
            else:
                kdb_query = '?[`.{0};enlist parse"{1}";0b;();1000000]'.format(ns, qry)
        else:
            tabs = self._get_tables()
            if len(tabs) == 1:
                tab = 'enlist {}'.format(tabs[0])
            else:
                tab = '({})'.format(";".join(tabs))
            if '()' == query:
                kdb_query = 'raze ?[;();0b;();1000000] each {0}'.format(,tab)
            else:
                kdb_query = 'raze ?[;enlist parse"{0}";0b;();1000000] each {1}'.format(query,tab)
        
        for doc in self.q.sync(kdb_query):
            subdoc = {}
            if isinstance(doc, QDictionary):
                for k, v in doc.items():
                    subdoc[k]=v
                if self.unique_key != "_id":
                    subdoc["_id"] = subdoc.pop(self.unique_key)
                yield subdoc

    @wrap_exceptions
    def search(self, start_ts, end_ts):
        """Called to query KDB for documents in a time range."""
        query = 'ts within ({0};{1})'.format(start_ts, end_ts)
        return self._stream_search(query)

    @wrap_exceptions
    def commit(self):
        pass

    @wrap_exceptions
    def get_last_doc(self):
        """Returns the last document stored in the KDB engine.
        """
        #search everything, sort by descending timestamp, return 1 row
        try:
            tabs = self._get_tables()
            if len(tabs) == 1:
                tab = 'enlist {}'.format(tabs[0])
            else:
                tab = '({})'.format(";".join(tabs))
            #result = self.q.sync('select max[ts] from raze ?[;enlist(=;`ts;(max;`ts));0b;()] each ({})'.format(";".join(tabs)))
            result = self.q.sync('?[;enlist(=;`ts;(max;`ts));0b;()] uj/[?[;enlist(=;`ts;(max;`ts));0b;()] each {}]'.format(tab))
        except ValueError:
            return None

        for r in result:
            doc = {}
            if isinstance(r, QDictionary):
                for k, v in r.items():
                    doc[k]=v
                doc['_id'] = doc.pop(self.unique_key)
                return doc
