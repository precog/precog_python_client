# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

import httplib
import json
import logging
import posixpath
import time
import datetime
import base64

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

# TODO: urllib2

class HttpClient(object):
    """Simple HTTP client for Precog API"""

    def __init__(self, api_key, host, port):
        """Initialize an HTTP connection"""
        self.log         = logging.getLogger(__name__)
        self.api_key     = api_key
        self.host        = host
        self.port        = port
        self.version     = 1

    # TODO: we really shouldn't be using the __getattr__ machinery here.
    def __getattr__(self, name):
        """Send an HTTP request"""

        # New connection per request, to avoid issues with servers closing
        # connections. We ran into this while testing against the dev cluster.
        # Tests would fail with a CannotSendRequest.
        if self.port == 443:
            connection = httplib.HTTPSConnection
        else:
            connection = httplib.HTTPConnection
        self.conn = connection("%s:%s" % (self.host, self.port))
        self.name = name.upper()

        # TODO: we should not be manually constructing HTTP requests :/

        def do(self, service='', action='', body='', parameters={}, headers={},
               content_type='application/json'):

            path = self.sanitize_path("%s/v1/%s" % (service, action))

            # Add token id to path and set headers
            path = "%s?apiKey=%s" % (path, self.api_key)
            for key, value in parameters.items():
                path = "%s&%s=%s" % (path, key, value)

            if headers == None:
                headers={'Content-Type': content_type}
            else:
                headers.update({'Content-Type': content_type})

            # Set up message
            message = ("%s to %s:%s%s with headers (%s)" %
                       (name, self.host, self.port, path, headers))
            if body:
                if content_type=='application/json':
                    body = json.dumps(body)
                message += " and body (%s)" % body

            # Send request and get response
            try:
                self.conn.request(name, path, body, headers)
                response = self.conn.getresponse()
                response_data = response.read()
            except StandardError, e:
                message += " failed (%s)" % (e)
                raise HttpResponseError(message)

            # Check HTTP status code
            if response.status not in [200, 202]:
                message += (" returned non-200 status (%d): %s [%s]" %
                            (response.status, response.reason, response_data))
                raise HttpResponseError(message)

            # Try parsing JSON response
            if len(response_data) > 0:
                try:
                    response_data = json.loads(response_data)
                except ValueError, e:
                    message += (" returned invalid JSON (%s): %s" %
                                (e, response_data))
                    raise HttpResponseError(message)

            message += " returned: %s" % response_data
            self.log.info(message)

            return response_data

        return do.__get__(self)

    def sanitize_path(self, path):
        """Sanitize a URL path"""

        normpath = posixpath.normpath(path)
        if path.endswith('/') and not normpath.endswith('/'):
            normpath += '/'
        return normpath

    def basic_auth(self, user, password):
        s = base64.standard_b64encode("%s:%s" % (user, password))
        return {"Authorization": "Basic %s" % s}

class Precog(object):
    """Precog base class"""

    def __init__(self, api_key='', host='api.precog.io', port=443):
        """Initialize an API client"""
        self.api = HttpClient(api_key=api_key, host=host, port=port)

    def create_account(self, email, password):
        """Create a new account ID.

        Given an email address and password, this method will create a
        new account, and return the account ID. If the account already
        exists, its account ID will be returned.
        """
        body = {"email": email, "password": password}
        return self.api.post('/accounts', "accounts/", body=body)

    def describe_account(self, email, password, accountId):
        """Return details about an account.

        The resulting dictionary will contain information about the
        account, including the master API key.
        """
        s = self.api.basic_auth(email, password)
        return self.api.get('/accounts', "accounts/%s" % accountId, headers=s)

    def property_count(self, path, prop, start = None, end = None):
        """Return count of the specified property"""

        params = {}
        if start and end:
            params = self.__time_parameters(start, end)

        prop = self.__sanitize_property(prop)
        path = '/analytics/%s/%s/count' % (path, prop)
        return self.api.get(self.__sanitize_path(path), parameters=params)
    
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

    def ingest(self, path, content, type_, options={}):
        """Ingests csv or json data at the specified path"""
        if not content:
            raise PrecogError("empty content")

        type_ = type_.lower()
        type_ = self.ingest_aliases.get(type_, type_)

        if type_ not in self.ingest_types:
            raise PrecogError("invalid content type %r" % type_)

        if options.get('async'):
            sync = 'async'
        else:
            sync = 'sync'
        p = "%s/fs/%s" % (sync, path)
        action = self.api.sanitize_path(p)
        return self.api.post('/ingest', action, content, options, {}, type_)

    def store(self, path, event, options = {}):
        """Store a record at the specified path"""
        s = json.dumps(event)
        return self.ingest(path, s, "application/json", options)

    def delete(self, path):
        return self.api.delete('/ingest', "sync/fs/%s" % path)

    def query(self, path, query):
        """Evaluate a query.
        
        Run a Quirrel query against specified base path, and return the
        resulting set.
        """
        if not path.startswith('/fs'):
            path = "/fs/%s" % path
        return self.api.get('/analytics', path, parameters={"q": query})


    def sanitize_property(self, prop):
        """Properties must always be prefixed with a period"""

        if prop and not prop.startswith('.'):
            prop = '.%s' % prop
        return prop

    @classmethod
    def from_heroku(cls, token):
        d = from_token(token)
        return cls(d['api_key'], d['host'])

def to_token(user, pwd, host, account_id, api_key, root_path):
    s = "%s:%s:%s:%s:%s:%s" % (user, pwd, host, account_id, api_key, root_path)
    return base64.urlsafe_b64encode(s)

fields = ['user', 'pwd', 'host', 'account_id', 'api_key', 'root_path']
def from_token(token):
    vs = base64.urlsafe_b64decode(token).split(":")
    return dict(zip(self.fields, vs))
