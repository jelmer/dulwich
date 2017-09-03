[![Build Status](https://travis-ci.org/jelmer/dulwich.png?branch=master)](https://travis-ci.org/jelmer/dulwich)
[![Windows Build status](https://ci.appveyor.com/api/projects/status/mob7g4vnrfvvoweb?svg=true)](https://ci.appveyor.com/project/jelmer/dulwich/branch/master)

This is the Dulwich project.

It aims to provide an interface to git repos (both local and remote) that
doesn't call out to git directly but instead uses pure Python.

**Main website**: [www.dulwich.io](https://www.dulwich.io/)

**License**: Apache License, version 2 or GNU General Public License, version 2 or later.

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

Getting started
---------------

Dulwich comes with both a lower-level API and higher-level plumbing ("porcelain").

For example, to use the lower level API to access the commit message of the
last commit:

    >>> from dulwich.repo import Repo
    >>> r = Repo('.')
    >>> r.head()
    '57fbe010446356833a6ad1600059d80b1e731e15'
    >>> c = r[r.head()]
    >>> c
    <Commit 015fc1267258458901a94d228e39f0a378370466>
    >>> c.message
    'Add note about encoding.\n'

And to print it using porcelain:

    >>> from dulwich import porcelain
    >>> porcelain.log('.', max_entries=1)
    --------------------------------------------------
    commit: 57fbe010446356833a6ad1600059d80b1e731e15
    Author: Jelmer Vernooĳ <jelmer@jelmer.uk>
    Date:   Sat Apr 29 2017 23:57:34 +0000

    Add note about encoding.

Further documentation
---------------------

The dulwich documentation can be found in docs/ and
[on the web](https://www.dulwich.io/docs/).

The API reference can be generated using pydoctor, by running "make pydoctor",
or [on the web](https://www.dulwich.io/apidocs).

Help
----

There is a *#dulwich* IRC channel on the [Freenode](https://www.freenode.net/), and
[dulwich-announce](https://groups.google.com/forum/#!forum/dulwich-announce)
and [dulwich-discuss](https://groups.google.com/forum/#!forum/dulwich-discuss)
mailing lists.

Contributing
------------

For a full list of contributors, see the git logs or [AUTHORS](AUTHORS).

If you'd like to contribute to Dulwich, see the [CONTRIBUTING](CONTRIBUTING.md)
file and [list of open issues](https://github.com/jelmer/dulwich/issues).

Supported versions of Python
----------------------------

At the moment, Dulwich supports (and is tested on) CPython 2.7, 3.3, 3.4, 3.5, 3.6 and Pypy.
