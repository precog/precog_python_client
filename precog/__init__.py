# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

import json
import logging
import posixpath
import time
import datetime

from httplib import HTTPSConnection, HTTPConnection
from urllib import urlencode, quote_plus
from base64 import urlsafe_b64encode, urlsafe_b64decode, standard_b64encode

__app_name__     = 'precog'
__version__      = '0.2.0'
__author__       = 'Gabriel Claramunt'
__author_email__ = 'gabriel@precog.com'
__description__  = 'Python client library for Precog (http://www.precog.com)'
__url__          = 'https://github.com/reportgrid/client-libraries/precog/python'

class PrecogError(Exception):
    """Base exception for all Precog errors"""

class HttpResponseError(PrecogError):
    """Raised on HTTP response errors"""

class HttpClient(object):
    """Simple HTTP client for Precog API"""

    def __init__(self, apikey, host, port, https=True):
        """Initialize an HTTP connection"""
        self.apikey = apikey
        self.host   = host
        self.port   = port
        self.https  = True

    def connect(self):
        s = "%s:%s" % (self.host, self.port)
        if self.https:
            return HTTPSConnection(s)
        else:
            return HTTPConnection(s)

    def post(self, path, body='', params={}, headers={}):
        return self.doit('POST', self.connect(), path, body, params, headers)

    def get(self, path, body='', params={}, headers={}):
        return self.doit('GET', self.connect(), path, body, params, headers)

    def delete(self, path, body='', params={}, headers={}):
        return self.doit('DELETE', self.connect(), path, body, params, headers)

    def doit(self, name, conn, path, body, params, headers):
        path = "%s?%s" % (path, urlencode(params.items()))

        # Send request and get response
        conn.request(name, path, body, headers)
        response = conn.getresponse()
        data = response.read()

        debugurl = "%s:%s%s" % (self.host, self.port, path)

        # Check HTTP status code
        if response.status not in [200, 202]:
            fmt = "%s body=%r params=%r headers=%r returned non-200 status (%d): %s [%s]"
            msg = fmt % (debugurl, body, params, headers, response.status, response.reason, data)
            raise HttpResponseError(msg)

        # Try parsing JSON response
        try:
            return json.loads(data)
        except ValueError, e:
            raise HttpResponseError('invalid json response %r' % data)

    def auth(self, user, password):
        s = standard_b64encode("%s:%s" % (user, password))
        return {"Authorization": "Basic %s" % s}

class Precog(object):
    """Precog base class"""

    gzip = 'application/x-gzip'
    zip_ = 'application/zip'
    json_ = 'application/json'
    csv_ = 'text/csv'

    ingest_types = set([gzip, zip_, json_, csv_])

    ingest_aliases = {
        'gz': gzip,
        'gzip': gzip,
        'zip': zip_,
        'json': json_,
        'csv': csv_,
    }

    def __init__(self, apikey, host='api.precog.io', port=443):
        """Initialize an API client"""
        self.api = HttpClient(apikey=apikey, host=host, port=port)

    def create_account(self, email, password):
        """Create a new account ID.
    
        Given an email address and password, this method will create a
        new account, and return the account ID.
        """
        body = json.dumps({"email": email, "password": password})
        return self.api.post('/accounts/v1/accounts/', body=body)

    def search_account(self, email):
        """Create a new account ID.

        Given an email address and password, this method will search
        for an existing account, and return the account ID if found, or
        None if the account is not found.
        """
        try:
            d = self.api.get('/accounts/v1/accounts/search', params={"email": email})
            return d['accountId']
        except HttpResponseError, e:
            return None

    def describe_account(self, email, password, accountId):
        """Return details about an account.

        The resulting dictionary will contain information about the
        account, including the master API key.
        """
        d = self.api.auth(email, password)
        return self.api.get('/accounts/v1/accounts/%s' % accountId, headers=d)

    def ingestjson(self, path, data, async=False, ownerid=None):
        return self.ingestraw(path, 'json', json.dumps(data), async, ownerid)

    def ingestcsv(self, path, data, async=False, ownerid=None):
        return self.ingestraw(path, 'csv', data, async, ownerid)

    def ingestraw(self, path, type_, bytes, async=False, ownerid=None):
        """Ingests csv or json data at the specified path"""
        if not bytes:
            raise PrecogError("no bytes to ingest")

        type_ = self.ingest_aliases.get(type_, type_)

        if type_ not in self.ingest_types:
            raise PrecogError("invalid ingest type %r" % type_)

        sync = 'async' if async else 'sync'
        path = '/ingest/v1/%s/fs/%s' % (sync, path)

        params = {'apiKey': self.api.apikey, 'type': type_}
        if ownerid is not None:
            params['ownerAccountId'] = ownerid

        headers = {'Content-Type': type_}

        return self.api.post(path, bytes, params=params, headers=headers)

    def store(self, path, event, opts={}):
        """Store a record at the specified path"""
        return self.ingestjson(path, json.dumps(event), "json", opts)

    def delete(self, path):
        return self.api.delete('/ingest/v1/sync/fs/%s' % path)

    def query(self, path, query):
        """Evaluate a query.
        
        Run a Quirrel query against specified base path, and return the
        resulting set.
        """
        return self.api.get('/analytics/v1/fs/%s' % path, params={"q": query, 'apiKey': self.api.apikey})

def to_token(user, pwd, host, accountid, apikey, root_path):
    s = "%s:%s:%s:%s:%s:%s" % (user, pwd, host, accountid, apikey, root_path)
    return urlsafe_b64encode(s)

fields = ['user', 'pwd', 'host', 'accountid', 'apikey', 'root_path']
def from_token(token):
    s = urlsafe_b64decode(token)
    toks = s.split(":")
    if len(toks) != len(fields):
        raise PrecogError("invalid token: %r (%r)" % (s, token))
    return dict(zip(fields, toks))
