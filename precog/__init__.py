# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

import json
import logging
import posixpath
import time
import datetime
import sys

from httplib import HTTPSConnection, HTTPConnection
from urllib import urlencode, quote_plus, pathname2url
from base64 import urlsafe_b64encode, urlsafe_b64decode, standard_b64encode

__app_name__     = 'precog'
__version__      = '0.2.0'
__author__       = 'Erik Osheim'
__author_email__ = 'erik@precog.com'
__description__  = 'Python client library for Precog (http://www.precog.com)'
__url__          = 'https://github.com/precog/precog_python_client'

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

class PrecogClientError(Exception):
    """
Raised on all errors detected or reported by the Precog platform.

This error usually suggests an error in the user's code.
    """

class PrecogServiceError(Exception):
    """
Raised on HTTP response errors.

This error may suggest a bug in the client or platform.
    """

class Format(object):
    """
Format contains the data formats supported by the Precog client. Methods like
``Precog.append`` require a format in order to parse data from a string or file.

 * json: Data is a single JSON value.
 * jsonstream: Data is a stream of JSON values separated by optional whitespace.
 * csv: Data is stored as comma-separated values.
 * tsv: Data is stored as tab-separated values.
 * ssv: Data is stored as semicolon-separated values.
    """
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
    """
Client for the Precog API. This class contains all the functionality provided
by this module.

Arguments:
 * apikey (str): String containing the Precog API key
   (e.g. "6789ABCD-EF12-3456-68AC-123456789ABC").
 * accountid (str): String containing the Precog Account ID
   (e.g. "00000011345").

Keyword Arguments:
 * basepath (str): Base path to use (defaults to accountid).
 * host (str): Host name to connect to.
 * port (int): Port to connect to.
 * https (bool): Whether to connect to host using HTTPS.
    """
    def __init__(self, apikey, accountid, basepath=None, host='beta.precog.com', port=443, https=True):
        if basepath is None: basepath = accountid
        self.apikey    = apikey
        self.accountid = accountid
        self.basepath  = basepath
        self.host      = host
        self.port      = port
        self.https     = https

    def _connect(self):
        s = "%s:%s" % (self.host, self.port)
        if self.https:
            return HTTPSConnection(s)
        else:
            return HTTPConnection(s)

    def _post(self, path, body='', params={}, headers={}):
        return self._doit('POST', self._connect(), path, body, params, headers)

    def _get(self, path, body='', params={}, headers={}):
        return self._doit('GET', self._connect(), path, body, params, headers)

    def _delete(self, path, body='', params={}, headers={}):
        return self._doit('DELETE', self._connect(), path, body, params, headers, void=True)

    def _doit(self, name, conn, path, body, params, headers, void=False):
        path = "%s?%s" % (pathname2url(path), urlencode(params.items()))

        # Send request and get response
        conn.request(name, path, body, headers)
        response = conn.getresponse()
        data = response.read()

        debugurl = "%s:%s%s" % (self.host, self.port, path)

        # Check HTTP status code
        if response.status not in [200, 202]:
            fmt = "%s body=%r params=%r headers=%r returned non-200 status (%d): %s [%s]"
            msg = fmt % (debugurl, body, params, headers, response.status, response.reason, data)
            raise PrecogServiceError(msg)

        if void:
            return None

        # Try parsing JSON response
        try:
            return json.loads(data)
        except ValueError, e:
            raise PrecogServiceError('invalid json response %r' % data)

    def _auth(self, user, password):
        s = standard_b64encode("%s:%s" % (user, password))
        return {"Authorization": "Basic %s" % s}

    def create_account(self, email, password):
        """
Create a new account.

Given an email address and password, this method will create a
new account, and return the account ID.

Arguments:
 * email (str): The email address for the new account.
 * password (str): The password for the new account.
        """
        body = json.dumps({"email": email, "password": password})
        headers = { 'Content-Type': Format.json["mime"] }
        return self._post('/accounts/v1/accounts/', body=body, headers=headers)

    def search_account(self, email):
        """
Search for an existing account.

Given an email address and password, this method will search
for an existing account, and return the account ID if found, or
None if the account is not found.

Arguments:
 * email (str): The email address for this account.
        """
        return self._get('/accounts/v1/accounts/search', params={"email": email})

    def account_details(self, email, password, accountId):
        """
Return details about an account.

The resulting dictionary will contain information about the
account, including the master API key.

Arguments:
 * email (str): The email address for this account.
 * password (str): The password for this account.
 * accountid (str): The ID for this account.
        """
        d = self._auth(email, password)
        return self._get('/accounts/v1/accounts/%s' % accountId, headers=d)

    def append(self, dest, obj):
        """
Append a single JSON object to the destination path. The object must be a
Python object representing a single JSON value: a dictionary, list, number,
string, boolean, or None.

Arguments:
 * dest (str): Precog path to append the object to.
 * obj (json): The Python object to be appended.
        """
        return self._ingest(dest, Format.json, json.dumps([obj]), mode='batch', receipt='true')

    def append_all(self, dest, objs):
        """
Appends an list of JSON object to the destination path. Each object must be a
Python object representing a single JSON value: a dictionary, list, number,
string, boolean, or None. The objects should be provided in a list.

Arguments:
 * dest (str): Precog path to append the object to.
 * objs (list): The list of Python objects to be appended.
        """
        return self._ingest(dest, Format.json, json.dumps(objs), mode='batch', receipt='true')

    def append_all_from_file(self, dest, format, src):
        """
Given a file and a format, append all the data from the file to the destination
path. The ``format`` should be one of those provided by the ``precog.Format``
class (e.g. ``Format.json``).

Arguments:
 * dest (str): Precog path to append the object to.
 * format (dict): A dictionary defining the format to be used. See
   ``precog.Format`` for the supported formats.
 * src (str or file): Either a path (as a string) or a file object read from.
        """
        if type(src) == str or type(src) == unicode:
            f = open(src, 'r')
            s = f.read()
            f.close()
        else:
            s = src.read()
        return self.append_all_from_string(dest, format, s)

    def append_all_from_string(self, dest, format, src):
        """
Given a string of data and a format, append all the data to the destination
path. The ``format`` should be one of those provided by the ``precog.Format``
class (e.g. ``Format.json``).

Arguments:
 * dest (str): Precog path to append the object to.
 * format (dict): A dictionary defining the format to be used. See
   ``precog.Format`` for the supported formats.
 * src (str): The data to be read.
        """
        return self._ingest(dest, format, src, mode='batch', receipt='true')

    def upload_file(self, dest, format, src):
        """
Given a file and a format, append all the data from the file to the destination
path. This will replace any data that previously existed.The ``format`` should
be one of those provided by the ``precog.Format`` class (e.g. ``Format.json``).

Arguments:
 * dest (str): Precog path to append the object to.
 * format (dict): A dictionary defining the format to be used. See
   ``precog.Format`` for the supported formats.
 * src (str or file): Either a path (as a string) or a file object read from.
        """
        if type(src) == str or type(src) == unicode:
            f = open(src, 'r')
            s = f.read()
            f.close()
        else:
            s = src.read()
        self.delete(dest)
        return self._ingest(dest, format, src, mode='batch', receipt='true')

    def upload_string(self, dest, format, src):
        """
Given a string of data and a format, append all the data to the destination
path. This will replace any data that previously existed.The ``format`` should
be one of those provided by the ``precog.Format`` class (e.g. ``Format.json``).

Arguments:
 * dest (str): Precog path to append the object to.
 * format (dict): A dictionary defining the format to be used. See
   ``precog.Format`` for the supported formats.
 * src (str): The data to be read.
        """
        self.delete(dest)
        return self._ingest(dest, format, src, mode='batch', receipt='true')

    def _ingest(self, path, format, bytes, mode, receipt):
        """Ingests csv or json data at the specified path"""
        if not bytes:
            raise PrecogClientError("no bytes to ingest")

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
        """
Evaluate a query.

Run a Quirrel query against specified base path, and return the resulting set.

Arguments:
 * query (str): The Quirrel query to perform.

Keyword Arguments:
 * path (str): Optional base path to add for this query.
 * detailed (bool): If true, result will be a dictionary containing more
   information about how the query was performed.
        """
        fullpath = ujoins('/analytics/v1/fs', self.basepath, path)
        params = {"q": query, 'apiKey': self.apikey, 'format': 'detailed'}
        d = self._get(fullpath, params=params)
        if detailed: return d
        errors = d.get('errors', [])
        if errors:
            raise PrecogClientError("query had errors: %r" % errors)
        servererrors = d.get('serverErrors', [])
        if servererrors:
            raise PrecogClientError("server had errors: %r" % errors)
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


#def to_token(user, pwd, host, accountid, apikey, root_path):
#    s = "%s:%s:%s:%s:%s:%s" % (user, pwd, host, accountid, apikey, root_path)
#    return urlsafe_b64encode(s)
#
#fields = ['user', 'pwd', 'host', 'accountid', 'apikey', 'root_path']
#def from_token(token):
#    s = urlsafe_b64decode(token)
#    toks = s.split(":")
#    if len(toks) != len(fields):
#        raise PrecogClientError("invalid token: %r (%r)" % (s, token))
#    return dict(zip(fields, toks))
