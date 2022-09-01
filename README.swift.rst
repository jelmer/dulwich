Openstack Swift as backend for Dulwich
======================================
Fabien Boucher <fabien.boucher@enovance.com>

The module dulwich/contrib/swift.py implements dulwich.repo.BaseRepo
in order to being compatible with Openstack Swift.
We can then use Dulwich as server (Git server) and instead of using
a regular POSIX file system to store repository objects we use the
object storage Swift via its own API.

c Git client <---> Dulwich server <---> Openstack Swift API

This implementation is still a work in progress and we can say that
is a Beta version so you need to be prepared to find bugs.

Configuration file
------------------

We need to provide some configuration values in order to let Dulwich
talk and authenticate against Swift. The following config file must
be used as template::

    [swift]
    # Authentication URL (Keystone or Swift)
    auth_url = http://127.0.0.1:5000/v2.0
    # Authentication version to use
    auth_ver = 2
    # The tenant and username separated by a semicolon
    username = admin;admin
    # The user password
    password = pass
    # The Object storage region to use (auth v2) (Default RegionOne)
    region_name = RegionOne
    # The Object storage endpoint URL to use (auth v2) (Default internalURL)
    endpoint_type = internalURL
    # Concurrency to use for parallel tasks (Default 10)
    concurrency = 10
    # Size of the HTTP pool (Default 10)
    http_pool_length = 10
    # Timeout delay for HTTP connections (Default 20)
    http_timeout = 20
    # Chunk size to read from pack (Bytes) (Default 12228)
    chunk_length = 12228
    # Cache size (MBytes) (Default 20)
    cache_length = 20


Note that for now we use the same tenant to perform the requests
against Swift. Therefore there is only one Swift account used
for storing repositories. Each repository will be contained in
a Swift container.

How to start unittest
---------------------

There is no need to have a Swift cluster running to run the unitests.
Just run the following command in the Dulwich source directory::

    $ PYTHONPATH=. python -m dulwich.contrib.test_swift

How to start functional tests
-----------------------------

We provide some basic tests to perform smoke tests against a real Swift
cluster. To run those functional tests you need a properly configured
configuration file. The tests can be run as follow::

    $ DULWICH_SWIFT_CFG=/etc/swift-dul.conf PYTHONPATH=. python -m dulwich.contrib.test_swift_smoke

How to install
--------------

Install the Dulwich library via the setup.py. The dependencies will be
automatically retrieved from pypi::

    $ python ./setup.py install

How to run the server
---------------------

Start the server using the following command::

    $ python -m dulwich.contrib.swift daemon -c /etc/swift-dul.conf -l 127.0.0.1

Note that a lot of request will be performed against the Swift
cluster so it is better to start the Dulwich server as close
as possible of the Swift proxy. The best solution is to run
the server on the Swift proxy node to reduce the latency.

How to use
----------

Once you have validated that the functional tests is working as expected and
the server is running we can init a bare repository. Run this
command with the name of the repository to create::

    $ python -m dulwich.contrib.swift init -c /etc/swift-dul.conf edeploy

The repository name will be the container that will contain all the Git
objects for the repository. Then standard c Git client can be used to
perform operations against this repository.

As an example we can clone the previously empty bare repository::

    $ git clone git://localhost/edeploy

Then push an existing project in it::

    $ git clone https://github.com/enovance/edeploy.git edeployclone
    $ cd edeployclone
    $ git remote add alt git://localhost/edeploy
    $ git push alt master
    $ git ls-remote alt
    9dc50a9a9bff1e232a74e365707f22a62492183e        HEAD
    9dc50a9a9bff1e232a74e365707f22a62492183e        refs/heads/master

The other Git commands can be used the way you do usually against
a regular repository.

Note the daemon subcommands starts a Git server listening for the
Git protocol. Therefore there is no authentication or encryption
at all between the cGIT client and the GIT server (Dulwich).

Note on the .info file for pack object
--------------------------------------

The Swift interface of Dulwich relies only on the pack format
to store Git objects. Instead of using only an index (pack-sha.idx)
along with the pack, we add a second file (pack-sha.info). This file
is automatically created when a client pushes some references on the
repository. The purpose of this file is to speed up pack creation
server side when a client fetches some references. Currently this
.info format is not optimized and may change in future.
