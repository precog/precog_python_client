# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

import precog
import time
import pytest
import base64

def setup_module(module):
    ROOT_API_KEY = pytest.config.getvalue('apiKey')
    HOST = pytest.config.getvalue('host')
    PORT = pytest.config.getvalue('port')

    print("Starting tests with the following settings: ")
    print("""  host  = %s
    port  = %d
    token = %s""" % (HOST, PORT, ROOT_API_KEY))

    module.TestPrecog.root_api = precog.Precog(ROOT_API_KEY, HOST, PORT)

    response = module.TestPrecog.root_api.create_account("test-py@precog.com","password")
    module.TestPrecog.account_id=response['accountId']
    response =module.TestPrecog.root_api.describe_account("test-py@precog.com","password",module.TestPrecog.account_id)
    module.TestPrecog.api_key=response['apiKey']
    module.TestPrecog.api = precog.Precog(module.TestPrecog.api_key, HOST, PORT)
    
    # response = module.TestPrecog.root_api.new_token(path='/python_test')

    # assert len(response) == len (ROOT_API_KEY)

    # module.TestPrecog.test_token_id = response
    # module.TestPrecog.test_api = precog.Precog(module.TestPrecog.test_token_id, HOST, PORT)

    # # Wait to allow the token to propagate
    # time.sleep(1)

    # module.TestPrecog.test_api.track(
    #     path='/',
    #     name='pytest',
    #     properties={'pyprop': 123},
    #     rollup=False)

    # module.TestPrecog.test_api.track(
    #     path='/py-client',
    #     name='pytest',
    #     properties={'pyprop': 456},
    #     rollup=True)

    # Wait to allow the events to propagate
    #time.sleep(1)


def teardown_module(module):
    pass
    # module.TestPrecog.root_api.delete_token(token_id=module.TestPrecog.test_token_id)
    # # Wait to allow the deletion to propagage
    # time.sleep(1)
    # assert module.TestPrecog.test_token_id not in module.TestPrecog.root_api.tokens()

class TestPrecog: 
    def test_ingest_csv(self):
        data = "blah\n\n"
        response = self.api.ingest(self.account_id, data, "csv")
        assert response.get('errors') == []
        assert response.get('ingested') == 1

    def test_ingest_json(self):
        data = '{"a": "foo", "b": {"nested": true}, "c": [1,2,3], "d": 4}'
        response = self.api.ingest(self.account_id, data, "json")
        assert response.get('errors') == []
        assert response.get('ingested') == 1
    
    def test_ingest_with_owner_id(self):
        data = '{"a": "foo", "b": {"nested": true}, "c": [1,2,3], "d": 4}'
        options = {"ownerAccountId": self.account_id}
        response = self.api.ingest(self.account_id, data, "json", options)
        assert response.get('errors') == []
        assert response.get('ingested') == 1

    def test_ingest_async(self):
        response = self.api.ingest(self.account_id, "[1,2,3,4]", "json", {'async': True})
        assert response.get('errors') == []
        assert response.get('ingested') == 1

    def test_store(self):
        response = self.api.store(self.account_id, {"animal" : 'bear'})
        assert response.get('ingested') == 1

    #def test_query(self):
    #    #just test the query was sent and executed sucessfully
    #    response=self.api.query(self.account_id, "count(//%s) " % (self.account_id))
    #    assert 0==response[0]
    #
    ## def test_query(self):
    ##     #just test the query was sent and executed sucessfully
    ##     response=self.api.query(self.account_id, "count(//"+self.account_id+")")
    ##     #assert_equal Array, response.class
    ##     assert 0 == response[0]
    #
    ## def test_token(self):
    ##     response = self.root_api.token(self.test_token_id)
    ##     assert type(response) is dict
    ##     assert 'tokenId' in response
    ##     assert response['tokenId'] == self.test_token_id
    #
    ## def test_tokens(self):
    ##     response = self.root_api.tokens()
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
    #def test_from_heroku(self):
    #    api=precog.Precog.from_heroku("user:password:beta.host.com:12345:AAAAA-BBBBB-CCCCCC-DDDDD:/00001234/")
    #    assert type(api) is Precog
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



