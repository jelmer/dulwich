This is the Dulwich project.

[![Build Status](https://travis-ci.org/jelmer/dulwich.png?branch=master)](https://travis-ci.org/jelmer/dulwich)

It aims to provide an interface to git repos (both local and remote) that
doesn't call out to git directly but instead uses pure Python.

Homepage: https://samba.org/~jelmer/dulwich/
Author: Jelmer Vernooij <jelmer@samba.org>

The project is named after the part of London that Mr. and Mrs. Git live in
in the particular Monty Python sketch.

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

The process of porting to Python3 is ongoing. Please not that although the
test suite pass in python3, this is due to the tests of features that are not
yet ported being skipped, and *not* an indication that the port is complete.
