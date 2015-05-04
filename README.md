[![Build Status](https://travis-ci.org/jelmer/dulwich.png?branch=master)](https://travis-ci.org/jelmer/dulwich)

This is the Dulwich project.

It aims to provide an interface to git repos (both local and remote) that
doesn't call out to git directly but instead uses pure Python.

Homepage: https://samba.org/~jelmer/dulwich/
Author: Jelmer Vernooij <jelmer@samba.org>

The project is named after the part of London that Mr. and Mrs. Git live in
in the particular Monty Python sketch.

Installation
------------

By default, Dulwich' setup.py will attempt to build and install the optional C
extensions. The reason for this is that they significantly improve the performance
since some low-level operations that are executed often are much slower in CPython.

If you don't want to install the C bindings, specify the --pure argument to setup.py::

    $ python setup.py --pure install

or if you are installing from pip:

    $ pip install dulwich --global-option="--pure"

Further documentation
---------------------

The dulwich documentation can be found in doc/ and on the web:

http://www.samba.org/~jelmer/dulwich/docs/

The API reference can be generated using pydoctor, by running "make pydoctor", or on the web:

http://www.samba.org/~jelmer/dulwich/apidocs

Help
----

There is a #dulwich IRC channel on Freenode, and a dulwich mailing list at
https://launchpad.net/~dulwich-users.

Python3
-------

The process of porting to Python3 is ongoing. At the moment most of Dulwich runs
on Python3, with the exception of some of ``dulwich.patch`` and some of
the C git compatibility tetsts. Relevant tests are currently being skipped on
Python 3, and the full testsuite passing is *not* an indication that the port
is complete.
