from distutils.core import setup

setup(
    name = "precog",
    ##packages = ["precog"],
    version = "0.2.0",
    description = "Client for the Precog API",
    author = 'Gabriel Claramunt',
    author_email = 'gabriel@precog.com',
    url = "http://precog.com",
    classifiers = [
        "Programming Language :: Python :: 2.7",
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules"
        ],
    long_description = """\
Sample Client for the Precog API
--------------------------------

This client implements some basic functionality to get you started using
the Precog data science platform. It includes the ability to:

 * Create an account ID
 * Load data into the service
 * Execute Quirrel queries
 * Append more data to an existing path

You are free to use this client directly, or to include any of its code in
your own services and applications.
"""
)
