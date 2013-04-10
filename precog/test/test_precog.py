# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

from precog import *
from pytest import config
from base64 import urlsafe_b64encode
import sys

def setup_module(m):
    ROOTKEY = config.getvalue('apiKey')
    HOST = config.getvalue('host')
    PORT = config.getvalue('port')

    m.TestPrecog.root = Precog(ROOTKEY, None, None, host=HOST, port=PORT)

    accountid = m.TestPrecog.root.search_account("test-py@precog.com")[0]['accountId']
    apikey = m.TestPrecog.root.account_details("test-py@precog.com", "password", accountid)['apiKey']

    m.TestPrecog.api = Precog(apikey, accountid, accountid, host=HOST, port=PORT)
    m.TestPrecog.accountid = accountid
    m.TestPrecog.apikey = apikey
    print 'ok ok', m.TestPrecog.api

def teardown_module(m):
    pass

class TestPrecog:
    def test_ingest_csv(self):
        data = "blah\n1\n2\n3\n"
        response = self.api.append_all_from_string("blah", Format.csv, data)
        assert response.get('errors') == []
        assert response.get('ingested') == 3
    
    def test_ingest_json(self):
        event = {"a": "foo", "b": {"nested": True}, "c": [1,2,3], "d": 4}
        response = self.api.append("blah", event)
        assert response.get('errors') == []
        assert response.get('ingested') == 1
    
    def test_ingest_json_many(self):
        events = [
            {"a": "foo", "b": {"nested": True}, "c": [1,2,3], "d": 4},
            {"a": "foo", "b": {"nested": True}, "c": [1,2,3], "d": 4},
        ]
        response = self.api.append_all("blah", events)
        assert response.get('errors') == []
        assert response.get('ingested') == 2
    
    def test_ingest_json_array(self):
        response = self.api.append("blah", [1,2,3,4])
        assert response.get('errors') == []
        assert response.get('ingested') == 1
    
    def test_query(self):
        response = self.api.query("count(//nonexistent)")
        assert response == [0]
