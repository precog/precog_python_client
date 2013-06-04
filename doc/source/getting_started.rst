Getting Started
***************

.. highlight:: python

If you already have a Precog account, the first thing you'll want to do is to
create a client object::

        from precog import Precog

        apikey = '01231234-2345-3579-468A-456789ABCDEF' #apikey
        accountid = '0000000123' #accountid
        basepath = accountid # this is a good default
        host = 'devapi.precog.com' #host
        port = '443' #port
        client = Precog(apikey, accountid, accountid, host, port)

Next we'll want to load some data into Precog. Here are four different methods
for adding equivalent data.

Uploading Files
===============

Comma Separated Values
----------------------

Imagine that we have the following data in a file called `songs.csv`::

        song,artist,album,tracknum,duration
        Peaches En Regalia,Frank Zappa,Hot Rats,1,218
        The Advent of Panurge,Gentle Giant,Octopus,1,281
        One More Red Nightmare,King Crimson,Red,3,427
        Jack The Ripper,Univers Zero,Heresie,2,809
        Vuh,Popol Vuh,In den Gärten Pharaos,2,1191

Using the previous snippet, the following code would load that into
the path `mysongs`::

        client.upload_file('mysongs', Format.csv, 'songs.csv')

We could also do the same work using a file object instead of a path::

        with open('songs.csv', 'r') as f:
            client.upload_file('mysongs', Format.csv, f)

By default CSV files are delimited by commas (`,`), use double-quotes
(`"`) to surround data that may contain commas, and escape
double-quotes using an extra double quote (`""`). It's possible to
read other formats. For instance, to read tab-separated values use
:const:`precog.Format.tsv` and to read semi-colon-separeated values use
:const:`precog.Format.ssv`.

You can also choose a custom delimiter, or custom-escaping, via the
:func:`~precog.Format.makecsv` function of the :class:`~precog.Format` class. Given a pipe-separeted
file called `foo.csv`::

        name|age
        anne|36
        bob|34
        cathy|29

The data could be ingested using::

        client.upload_file(foo, Format.makecsv(delim='|'), "pipe-separated.csv")

Javascript Object Notation
--------------------------

Any data stored in CSV can be converted to JSON instead. While the CSV
reprsentation is often more compact, it can only represent flat
objects. So if you have nested data (objects within objects), you'll
want to use JSON.

Here's a JSON representation of the previous data for the file
`songs.json`:

.. code-block:: javascript

        [{"song": "Peaches En Regalia",
          "artist": "Frank Zappa",
          "album": "Hot Rats",
          "tracknum": 1,
          "duration": 218},
         {"song": "The Advent of Panurge",
          "artist": "Gentle Giant",
          "album": "Octopus",
          "tracknum": 1,
          "duration": 281},
         {"song": "One More Red Nightmare",
          "artist": "King Crimson",
          "album": "Red",
          "tracknum": 3,
          "duration": 427},
         {"song": "Jack The Ripper",
          "artist": "Univers Zero",
          "album": "Heresie",
          "tracknum": 2,
          "duration": 809},
         {"song": "Vuh",
          "artist": "Popol Vuh",
          "album": "In den Gärten Pharaos",
          "tracknum": 2,
          "duration": 1191}
        ]

Using the previous snippet, the following code would load that into
the path `mysongs`::

        client.upload_file('mysongs', Format.json, 'songs.json')

Appending Data
==============

The previous commands will upload an entire file to a path, replacing
whatever is already there. But in many cases you might want to append
extra data without removing what was already there. In these cases
you'll want the family of append functions: :func:`~precog.Precog.append`, :func:`~precog.Precog.append_all`,
:func:`~precog.Precog.append_all_from_file`, and :func:`~precog.Precog.append_all_from_string`.

You can add events one at a time as Python objects using :func:`~precog.Precog.append`::

        song = {"song": "Meeting of the Spirits",
                "artist": "Mahavishnu Orchestra",
                "album": "The Inner Mounting Flame",
                "tracknum": 1,
                "duration": 412}
        client.append('mysongs', song)

This can also be done in bulk using :func:`~precog.Precog.append_all` and an array::

        songs = [
            {"song": "Moonshake",
             "artist": "Can",
             "album": "Future Days",
             "tracknum": 3,
             "duration": 184,
            },
            {"song": "Untitled",
             "artist": "Faust",
             "album": "The Faust Tapes",
             "tracknum": 1,
             "duration": 1357,
            }
        ]
        client.append_all('mysongs', songs)

Be careful! If you were to call :func:`~precog.Precog.append` instead of :func:`~precog.Precog.append_all` with
an array, the method would succeed but you would have imported the
array a single event containing multiple objects, instead of multiple
events.

If your data is stored in a string or file you can call
:func:`~precog.Precog.append_all_from_string` or :func:`~precog.Precog.append_all_from_file` which are somewhat
similar to :func:`~precog.Precog.upload_file`.

.. code-block:: python

        # this adds the data from extrasongs.json
        with open('extra.json', 'r') as f:
            client.append_all_from_file('mysongs', Format.json, f)
            
        # ...so does this
        client.append_all_from_file('mysongs', Format.json, 'extra.json')

        # ...and this does too!
        s = open('extra.json', 'r').read()
        client.append_all_from_string('mysongs', Format.json, s)

Running Queries
===============

Now that we've loaded all our songs in `mysongs`, we can learn things
about our music collection. The following Python snippet runs a
Quirrel query to find the total duration of all our songs::

        quirrel = """
          songs := //mysongs
          sum(songs.duration)
        """
        n = client.query(quirrel)[0]
        print "we have %s seconds of music!" % n

Queries always result in a set of results. So even when running a `count`,
`sum`, or other reduction, it's important to remember that you will get back
an array containing a number, not just a number.

Quirrel results will be translated into standard Python objects,
strings, numbers, and so on. If there is an error with the query we'll
get a :exc:`~precog.PrecogClientError` exception. There may be additional output if there
were (non-fatal) warnings sent back.

In some cases we don't want exception, but want more details about the
query execution. In these instances we can use the `detailed` keyword
parameter to get more information::

        quirrel = """
          songs := //mysongs
          sum(songs.duration)
        """
        result = client.query(quirrel, detailed=True)
        print "our result was %s" % result

The result will look something like the following::

        {'serverErrors': [], 'errors': [], 'data': 235235, 'warnings': []}

Obviously if there were errors or warnings we'd get a list of
those. In this case the query executed successfully and the `data`
parameter contains our result (the number of seconds in our music
library).

License
=======

The client code is available to you under the MIT license.

See the `LICENSE` file for more details.

Copyright 2011-2013 (c) ReportGrid, Inc.
