from distutils.core import setup, Command
import os, pytest, shutil

class SimpleCommand(Command):
    user_options = []
    def initialize_options(self): pass
    def finalize_options(self): pass

class TestCommand(SimpleCommand):
    def run(self):
        n = pytest.main(['-q'])
        if n == 0:
            print "tests passed"
        else:
            print "tests failed"

class CleanCommand(SimpleCommand):
    def run(self):
        for path in ['build', 'dist', 'MANIFEST']:
            if os.path.isdir(path):
                print "+ cleaning %s" % path
                shutil.rmtree(path)
            elif os.path.exists(path):
                print "+ cleaning %s" % path
                os.remove(path)

setup(
    name = "precog",
    packages = ["precog"],
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
""",
    cmdclass = {'test': TestCommand, 'clean': CleanCommand},
)
