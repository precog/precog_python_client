# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

from precog import Precog

host = "devapi.precog.com"
port = 443
rootkey = 'A3BC1539-E8A9-4207-BB41-3036EC2C6E6D'
root = Precog(rootkey, host, port)

assert(root.search_account("does-not-exist-23455@precog.com") is None)
print 'search not found: ok'

accountid = root.search_account("test-py@precog.com")
assert(accountid is not None)
print 'account found: ok'

d = root.describe_account("test-py@precog.com", "password", accountid)
assert(d['email'] == "test-py@precog.com")
assert(d['accountId'] == accountid)
apikey = d['apiKey']
api = Precog(apikey, host, port)
print 'api: ok'

# csv
csvdata = "foo,bar,qux\n1,2,3\n4,5,6\n"
response = api.ingestcsv(accountid, csvdata)
assert response.get('errors') == []
assert response.get('ingested') == 2
print 'csv: ok'

# json
jsondata = {"a": "foo", "b": {"nested": True}, "c": [1,2,3], "d": 4}
response = api.ingestjson(accountid, jsondata)
assert response.get('errors') == []
assert response.get('ingested') == 1
print 'json ok'

response = api.ingestjson(accountid, jsondata, ownerid=accountid)
assert response.get('errors') == []
assert response.get('ingested') == 1
print 'json with owner ok'

response = api.ingestjson(accountid, [1,2,3,4], async=True)
assert response.get('errors') == []
assert response.get('ingested') == 1
print 'async json ok'

response = api.store(accountid, {"animal" : 'bear'})
assert response.get('ingested') == 1
print 'store ok'

response = api.query(accountid, "count(//nonexistent)")
assert response == [0]
print 'null count ok'

## def test_tokens(self):
##     response = self.root.tokens()
##     assert type(response) is list
##     assert self.test_token_id in response
#
## def test_children(self):
##     response = self.test_api.children(path='/')
##     assert type(response) is list
##     assert '.pytest' in response
#
## def test_children_with_type_path(self):
##     response = self.test_api.children(path='/', type='path')
##     assert type(response) is list
##     assert u'py-client' in response
#
## def test_children_with_type_property(self):
##     response = self.test_api.children(path='/', type='property')
##     assert type(response) is list
##     assert '.pytest' in response
#
## def test_children_with_property(self):
##     response = self.test_api.children(path='/', property='pytest')
##     assert type(response) is list
##     assert '.pyprop' in response
#
## def test_property_count(self):
##     response = self.test_api.property_count(path='/', property='pytest')
##     assert type(response) is int
##     assert response > 0
#
## def test_property_series(self):
##     response = self.test_api.property_series(path='/', property='pytest')
##     assert type(response) is list
##     #assert precog.Periodicity.Eternity in response
##     #assert type(response[precog.Periodicity.Eternity]) is list
##     #assert len(response[precog.Periodicity.Eternity]) > 0
#
## def test_property_values(self):
##     response = self.test_api.property_values(path='/', property='pytest.pyprop')
##     assert type(response) is list
##     assert 123 in response
#
## def test_property_value_count(self):
##     response = self.test_api.property_value_count(path='/', property='pytest.pyprop', value=123)
##     assert type(response) is int
##     assert response > 0
#
## def test_rollup_property_value_count(self):
##     response = self.test_api.property_value_count(path='/', property='pytest.pyprop', value=456)
##     assert type(response) is int
##     assert response > 0
#
## def test_property_value_series(self):
##     response = self.test_api.property_value_series(path='/', property='pytest.pyprop', value=123)
##     assert type(response) is list
##     #assert precog.Periodicity.Eternity in response
##     #assert type(response[precog.Periodicity.Eternity]) is list
##     #assert len(response[precog.Periodicity.Eternity]) > 0
#
## def test_search_count(self):
##     response = self.test_api.search_count(path='/', where=[{"variable":".pytest.pyprop", "value":123}])
##     assert type(response) is int
##     assert response > 0
#
## def test_search_series(self):
##     response = self.test_api.search_series(path='/', where=[{"variable":".pytest.pyprop", "value":123}])
##     assert type(response) is list
##     #assert precog.Periodicity.Eternity in response
##     #assert type(response[precog.Periodicity.Eternity]) is list
##     #assert len(response[precog.Periodicity.Eternity]) > 0
#
#def test_from_token(self):
#    token=base64.urlsafe_b64encode("user1:password1:beta.host.com:12345:AAAAA-BBBBB-CCCCCC-DDDDD:/00001234/")
#    values=precog.from_token(token)
#    assert values['user']=="user1"
#    assert values['pwd']=="password1"
#    assert values['host']=="beta.host.com"
#    assert values['accountid']=="12345"
#    assert values['api_key']=="AAAAA-BBBBB-CCCCCC-DDDDD"
#    assert values['root_path']=="/00001234/"
#
#def test_to_token(self):
#    token=precog.to_token("user","password","beta.host.com","12345","AAAAA-BBBBB-CCCCCC-DDDDD","/00001234/")
#    assert token == base64.urlsafe_b64encode("user:password:beta.host.com:12345:AAAAA-BBBBB-CCCCCC-DDDDD:/00001234/")
