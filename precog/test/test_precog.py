# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

from precog import Precog
from pytest import config
from base64 import urlsafe_b64encode

def setup_module(m):
    ROOTKEY = config.getvalue('apiKey')
    HOST = config.getvalue('host')
    PORT = config.getvalue('port')

    m.TestPrecog.root = Precog(ROOTKEY, HOST, PORT)

    account_id = m.TestPrecog.root.search_account("test-py@precog.com")
    api_key = m.TestPrecog.root.describe_account("test-py@precog.com", "password", account_id)['apiKey']

    m.TestPrecog.api = Precog(api_key, HOST, PORT)
    m.TestPrecog.account_id = account_id
    m.TestPrecog.api_key = api_key

def teardown_module(m):
    pass

class TestPrecog:
    def test_ingest_csv(self):
        data = "blah\n1\n2\n3\n"
        response = self.api.ingestcsv(self.account_id, data)
        assert response.get('errors') == []
        assert response.get('ingested') == 3
    
    def test_ingest_json(self):
        data = {"a": "foo", "b": {"nested": True}, "c": [1,2,3], "d": 4}
        response = self.api.ingestjson(self.account_id, data)
        assert response.get('errors') == []
        assert response.get('ingested') == 1
    
    def test_ingest_with_owner_id(self):
        data = {"a": "foo", "b": {"nested": True}, "c": [1,2,3], "d": 4}
        response = self.api.ingestjson(self.account_id, data, ownerid=self.account_id)
        assert response.get('errors') == []
        assert response.get('ingested') == 1
    
    def test_ingest_sync(self):
        response = self.api.ingestjson(self.account_id, [1,2,3,4], async=False)
        assert response.get('errors') == []
        assert response.get('ingested') == 1
    
    def test_store(self):
        response = self.api.store(self.account_id, {"animal" : 'bear'})
        assert response.get('ingested') == 1
    
    def test_query(self):
        response = self.api.query(self.account_id, "count(//nonexistent)")
        assert response == [0]
    
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
    #    assert values['account_id']=="12345"
    #    assert values['api_key']=="AAAAA-BBBBB-CCCCCC-DDDDD"
    #    assert values['root_path']=="/00001234/"
    #
    #def test_to_token(self):
    #    token=precog.to_token("user","password","beta.host.com","12345","AAAAA-BBBBB-CCCCCC-DDDDD","/00001234/")
    #    assert token == base64.urlsafe_b64encode("user:password:beta.host.com:12345:AAAAA-BBBBB-CCCCCC-DDDDD:/00001234/")
