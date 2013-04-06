# This file is subject to the terms and conditions defined in LICENSE.
# (c) 2011-2013, ReportGrid Inc. All rights reserved.

import precog
import pytest

def pytest_addoption(parser):
    parser.addoption("--host", action="store", default="devapi.precog.com",
                     help="The hostname used for the unit test")
    parser.addoption("--port", action="store", default=443, type=int,
                     help="The port used for the unit test")
    parser.addoption("--apiKey", action="store",
                     default='A3BC1539-E8A9-4207-BB41-3036EC2C6E6D',
                     help="The token used for the unit test")
