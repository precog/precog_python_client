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

### Testing the Client

To test the client, first install [py.test](http://pytest.org/latest/getting-started.html#getstarted).

Run the tests using `python setup.py test`.

To get more control over how the tests run, you can also run the tests using
the `py.test` command, which supports a wide variety of options.

### Packaging the Client

To package the client, run `python setup.py sdist`. This will create a
compressed file (.tar.gz) containing this file, the license and the source
code. The file will be named something like `dist/precog-0.2.0.tar.gz`.

### License

The client code is available to you under the MIT license.

See the `LICENSE` file for more details.

Copyright 2011-2013 (c) ReportGrid, Inc.
