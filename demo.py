# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

import time
from precog import *

host = "devapi.precog.com"
port = 443
rootkey = 'A3BC1539-E8A9-4207-BB41-3036EC2C6E6D'
root = Precog(rootkey, None, None, host=host, port=port)

assert root.search_account("does-not-exist-23455@precog.com") == []
print 'search not found: ok'

accountid = root.search_account("test-py@precog.com")[0]['accountId']
assert accountid is not None
print 'account found: ok'

d = root.account_details("test-py@precog.com", "password", accountid)
assert d['email'] == "test-py@precog.com", d
assert d['accountId'] == accountid, d
apikey = d['apiKey']
api = Precog(apikey, accountid, accountid, host=host, port=port)
print 'api: ok'

# csv
csvdata = "foo,bar,qux\n1,2,3\n4,5,6\n"
response = api.append_all_from_string('foo', Format.csv, csvdata)
assert response.get('errors') == [], response
assert response.get('ingested') == 2, response
print 'append_all_from_string(csv): ok'

# json
jsondata = {"a": "foo", "b": {"nested": True}, "c": [1,2,3], "d": 4}
response = api.append_all("foo", jsondata)
assert response.get('errors') == [], response
assert response.get('ingested') == 1, response
print 'append_all: ok'

response = api.append("bar", [1,2,3,4])
assert response.get('ingested') == 1, response
print 'append: ok'

response = api.query("qux", "count(//nonexistent)")
assert response == [0], response
print 'empty count: ok'

response = api.query("qux", "count(//nonexistent)", detailed=True)
assert response == {'serverErrors': [], 'errors': [], 'data': [0], 'warnings': []}, response
print 'detailed empty count: ok'

def queryUntil(bp, q, expected, timeout=30):
    t0 = time.time()
    res = None
    while time.time() - t0 < timeout:
        print "  trying..."
        res = api.query(bp, q)
        if res == expected:
            return
        time.sleep(0.5)
    assert res == expected, res

api.delete("qux/test")
queryUntil("qux", "count(//test)", [0])
print "delete qux/test: ok"

objs = []
for i in range(0, 100): objs.append({"i": i, "j": i % 13, "k": "foo"})
response = api.append_all("qux/test", objs)
assert response['ingested'] == 100, response
print "populate qux/test: ok"

queryUntil("qux", "count(//test)", [100])
print "count qux/test: ok"

newer = []
for i in range(0, 60):
    newer.append({"iii": i, "newer": True})
s = json.dumps(newer)
response = api.upload_string("qux/test", Format.json, s)
assert response['ingested'] == 60, response
print "upload new qux/test: ok"

queryUntil("qux", "count(//test)", [60])
print "count qux/test again: ok"
