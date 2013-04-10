# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

import json
import logging
import posixpath
import time
import datetime
import sys

from httplib import HTTPSConnection, HTTPConnection
from urllib import urlencode, quote_plus
from base64 import urlsafe_b64encode, urlsafe_b64decode, standard_b64encode

__app_name__     = 'precog'
__version__      = '0.2.0'
__author__       = 'Gabriel Claramunt'
__author_email__ = 'gabriel@precog.com'
__description__  = 'Python client library for Precog (http://www.precog.com)'
__url__          = 'https://github.com/reportgrid/client-libraries/precog/python'

def ujoin(p1, p2):
    if not p1: return p2
    if not p2: return p1
    s1 = p1[:-1] if p1.endswith('/') else p1
    s2 = p2[1:] if p2.startswith('/') else p2
    return s1 + '/' + s2

def ujoins(*ps):
    pt = ""
    for p in ps: pt = ujoin(pt, p)
    return pt

class PrecogError(Exception):
    """Base exception for all Precog errors"""

class HttpResponseError(PrecogError):
    """Raised on HTTP response errors"""

class Format(object):
    @classmethod
    def make(cls, mime):
        return {'mime': mime, 'params': {}}
    @classmethod
    def makecsv(cls, delim=',', quote='"', escape='"'):
        params = {'delim': delim, 'quote': quote, 'escape': escape}
        return {'params': params, 'mime': 'text/csv'}

Format.json       = Format.make('application/json')
Format.jsonstream = Format.make('application/x-json-stream')
Format.csv        = Format.makecsv()
Format.tsv        = Format.makecsv(delim='\t')
Format.ssv        = Format.makecsv(delim=';')

class Precog(object):
    """Precog base class"""

    def __init__(self, apikey, accountid, basepath, host='beta.precog.com', port=443, https=True):
        """Initialize an API client"""
        ###self.api = HttpClient(apikey, accountid, basepath, host, port, https)
        self.apikey    = apikey
        self.accountid = accountid
        self.basepath  = basepath
        self.host      = host
        self.port      = port
        self.https     = https

    def connect(self):
        s = "%s:%s" % (self.host, self.port)
        if self.https:
            return HTTPSConnection(s)
        else:
            return HTTPConnection(s)

    def _post(self, path, body='', params={}, headers={}):
        return self._doit('POST', self.connect(), path, body, params, headers)

    def _get(self, path, body='', params={}, headers={}):
        return self._doit('GET', self.connect(), path, body, params, headers)

    def _delete(self, path, body='', params={}, headers={}):
        return self._doit('DELETE', self.connect(), path, body, params, headers, void=True)

    def _doit(self, name, conn, path, body, params, headers, void=False):
        path = "%s?%s" % (path, urlencode(params.items()))

        # Send request and get response
        conn.request(name, path, body, headers)
        response = conn.getresponse()
        data = response.read()

        debugurl = "%s:%s%s" % (self.host, self.port, path)
        #print "%s body=%r params=%r headers=%r returned %r" % (debugurl, body, params, headers, data)

        # Check HTTP status code
        if response.status not in [200, 202]:
            fmt = "%s body=%r params=%r headers=%r returned non-200 status (%d): %s [%s]"
            msg = fmt % (debugurl, body, params, headers, response.status, response.reason, data)
            raise HttpResponseError(msg)

        if void:
            return None

        # Try parsing JSON response
        try:
            #print "(data was %r)" % data
            return json.loads(data)
        except ValueError, e:
            raise HttpResponseError('invalid json response %r' % data)

    def _auth(self, user, password):
        s = standard_b64encode("%s:%s" % (user, password))
        return {"Authorization": "Basic %s" % s}

    def create_account(self, email, password, profile):
        """Create a new account ID.
    
        Given an email address and password, this method will create a
        new account, and return the account ID.
        """
        body = json.dumps({"email": email, "password": password})
        return self._post('/accounts/v1/accounts/', body=body)

    def account_details(self):
        raise Exception("fixme")

    def search_account(self, email):
        """Create a new account ID.

        Given an email address and password, this method will search
        for an existing account, and return the account ID if found, or
        None if the account is not found.
        """
        return self._get('/accounts/v1/accounts/search', params={"email": email})

    def account_details(self, email, password, accountId):
        """Return details about an account.

        The resulting dictionary will contain information about the
        account, including the master API key.
        """
        d = self._auth(email, password)
        return self._get('/accounts/v1/accounts/%s' % accountId, headers=d)

    def append(self, dest, obj):
        return self._ingest(dest, Format.json, json.dumps([obj]), mode='batch', receipt='true')

    def append_all(self, dest, objs):
        return self._ingest(dest, Format.json, json.dumps(objs), mode='batch', receipt='true')

    def append_all_from_file(self, dest, format, src):
        if type(src) == str or type(src) == unicode:
            f = open(src, 'r')
            s = f.read()
            f.close()
        else:
            s = src.read()
        return self.append_all_from_string(dest, format, s)

    def append_all_from_string(self, dest, format, src):
        return self._ingest(dest, format, src, mode='batch', receipt='true')

    def upload_file(self, dest, format, src):
        if type(src) == str or type(src) == unicode:
            f = open(src, 'r')
            s = f.read()
            f.close()
        else:
            s = src.read()
        self.delete(dest)
        return self._ingest(dest, format, src, mode='batch', receipt='true')

    def upload_string(self, dest, format, src):
        self.delete(dest)
        return self._ingest(dest, format, src, mode='batch', receipt='true')

    def _ingest(self, path, format, bytes, mode, receipt):
        """Ingests csv or json data at the specified path"""
        if not bytes:
            raise PrecogError("no bytes to ingest")

        fullpath = ujoins('/ingest/v1/fs', self.basepath, path)
        params = {'apiKey': self.apikey, 'mode': mode, 'receipt': receipt}
        params.update(format['params'])
        headers = {'Content-Type': format['mime']}

        return self._post(fullpath, bytes, params=params, headers=headers)

    def delete(self, path):
        params = {'apiKey': self.apikey}
        fullpath = ujoins('/ingest/v1/fs', self.basepath, path)
        return self._delete(fullpath, params=params)

    def query(self, query, path="", detailed=False):
        """Evaluate a query.
        
        Run a Quirrel query against specified base path, and return the
        resulting set.
        """
        fullpath = ujoins('/analytics/v1/fs', self.basepath, path)
        params = {"q": query, 'apiKey': self.apikey, 'format': 'detailed'}
        d = self._get(fullpath, params=params)
        if detailed: return d
        errors = d.get('errors', [])
        if errors:
            raise PrecogError("query had errors: %r" % errors)
        servererrors = d.get('serverErrors', [])
        if servererrors:
            raise PrecogError("server had errors: %r" % errors)
        for w in d.get('warnings', []):
            sys.stderr.write("warning: %s" % w)
        return d.get('data', None)

    #def async_query(self, path, query):
    #    basepath = ujoin(self.basepath, path)
    #    params = {"q": query, 'apiKey': self.apikey, 'basePath': basepath}
    #    return self.get('/analytics/v1/queries', params=params)

    #def async_status(self, queryid):
    #    fullpath = ujoins('/analytics/v1/queries/%d/status' % queryid)
    #    return self.get(fullpath, params={'apiKey': self.apikey})

    #def async_results(self, queryid):
    #    fullpath = '/analytics/v1/queries/%d' % queryid
    #    return self.get(fullpath, params={'apiKey': self.apikey})


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
