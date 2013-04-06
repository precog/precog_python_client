## Precog Python Client

This library provides some sample Python code to get you started using Precog
in your Python programs. You will want to be sure to visit the
[Precog API Documention](http://www.precog.com/rest-apis/accounts) so you
understand how the service works.

Currently, the Python client is being refactored, and so there may
be bugs. If you notice any, please open a ticket on the
[Github page](https://github.com/precog/precog_python_client).

### Installing the Client

To install the library, run `python setup.py install`.

*NOTE*: For now, please do not install the library. We're in the process of
refactoring it and currently it will probably not install correctly. For now,
you can rename `precog/__init__.py` to `precog.py` and copy it into your own
projects.

### Testing the Client

To test the client, first install [py.test](http://pytest.org/latest/getting-started.html#getstarted).

Next, run `py.test -q`. If you see a series of dots, everything is working. If
you see a capital F or other output, then tests are failing.

### License

The client code is available to you under the MIT license.

See the `LICENSE` file for more details.

Copyright 2011-2013 (c) ReportGrid, Inc.
