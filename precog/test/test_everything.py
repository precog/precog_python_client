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

    m.TestEverything.root = Precog(ROOTKEY, None, None, host=HOST, port=PORT)

    accountid = m.TestEverything.root.search_account("test-py@precog.com")[0]['accountId']
    apikey = m.TestEverything.root.account_details("test-py@precog.com", "password", accountid)['apiKey']

    m.TestEverything.api = Precog(apikey, accountid, accountid, host=HOST, port=PORT)
    m.TestEverything.accountid = accountid
    m.TestEverything.apikey = apikey
    print 'ok ok', m.TestEverything.api

def teardown_module(m):
    pass

class TestEverything:
    def queryUntil(self, bp, q, expected, timeout=30):
        t0 = time.time()
        res = None
        while time.time() - t0 < timeout:
            print "  trying..."
            res = self.api.query(q, bp)
            if res == expected:
                return
            time.sleep(0.5)
        assert res == expected, res

    def test_csv(self):
        # csv
        csvdata = "foo,bar,qux\n1,2,3\n4,5,6\n"
        response = self.api.append_all_from_string('foo', Format.csv, csvdata)
        assert response.get('errors') == [], response
        assert response.get('ingested') == 2, response
        print 'append_all_from_string(csv): ok'
        
    def test_json(self):
        # json
        jsondata = {"a": "foo", "b": {"nested": True}, "c": [1,2,3], "d": 4}
        response = self.api.append_all("foo", jsondata)
        assert response.get('errors') == [], response
        assert response.get('ingested') == 1, response
        print 'append_all: ok'
        
    def test_append(self):
        response = self.api.append("bar", [1,2,3,4])
        assert response.get('ingested') == 1, response
        print 'append: ok'
        
    def test_query1(self):
        response = self.api.query("count(//nonexistent)", "qux")
        assert response == [0], response
        print 'empty count: ok'
        
    def test_query2(self):
        response = self.api.query("count(//nonexistent)", "qux", detailed=True)
        assert response == {'serverWarnings': [], 'serverErrors': [], 'errors': [], 'data': [0], 'warnings': []}, response
        print 'detailed empty count: ok'

    def test_populate1(self):
        self.api.delete("qux/test")
        self.queryUntil("qux", "count(//test)", [0])
        print "delete qux/test: ok"
        
        objs = []
        for i in range(0, 100): objs.append({"i": i, "j": i % 13, "k": "foo"})
        response = self.api.append_all("qux/test", objs)
        assert response['ingested'] == 100, response
        print "populate qux/test: ok"
        
        self.queryUntil("qux", "count(//test)", [100])
        print "count qux/test: ok"

    def test_upload(self):
        newer = []
        for i in range(0, 60):
            newer.append({"iii": i, "newer": True})
        s = json.dumps(newer)
        response = self.api.upload_string("qux/test", Format.json, s)
        assert response['ingested'] == 60, response
        print "upload new qux/test: ok"
        
        self.queryUntil("qux", "count(//test)", [60])
        print "count qux/test again: ok"
        
