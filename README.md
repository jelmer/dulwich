[![Build Status](https://travis-ci.org/jelmer/dulwich.png?branch=master)](https://travis-ci.org/jelmer/dulwich)

This is the Dulwich project.

It aims to provide an interface to git repos (both local and remote) that
doesn't call out to git directly but instead uses pure Python.

Main website: https://www.dulwich.io/

License: Apache License, version 2 or GNU General Public License, version 2 or later.

The project is named after the part of London that Mr. and Mrs. Git live in
in the particular Monty Python sketch.

Installation
------------

By default, Dulwich' setup.py will attempt to build and install the optional C
extensions. The reason for this is that they significantly improve the performance
since some low-level operations that are executed often are much slower in CPython.

If you don't want to install the C bindings, specify the --pure argument to setup.py::

    $ python setup.py --pure install

or if you are installing from pip::

    $ pip install dulwich --global-option="--pure"

Further documentation
---------------------

The dulwich documentation can be found in doc/ and on the web:

https://www.dulwich.io/docs/

The API reference can be generated using pydoctor, by running "make pydoctor", or on the web:

https://www.dulwich.io/apidocs

Help
----

There is a #dulwich IRC channel on Freenode, and a dulwich mailing list at
https://launchpad.net/~dulwich-users.

Supported versions of Python
----------------------------

At the moment, Dulwich supports (and is tested on) CPython 2.7, 3.4, 3.5 and Pypy.
