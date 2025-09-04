All functionality should be available in pure Python. Optional Rust
implementations may be written for performance reasons, but should never
replace the Python implementation.

Where possible include updates to NEWS along with your improvements.

New functionality and bug fixes should be accompanied by matching unit tests.

Installing development dependencies
-----------------------------------

Contributing to Dulwich requires several more dependencies than are required to install
the base package; they are used to run tests and various other checks.

First, make sure your system has the Rust compiler and Cargo (the Rust package manager)
installed. As this is a system-level requirement and not a Python library, the right way
to install it depends on your platform. Please consult the `Rust documentation
<https://www.rust-lang.org/learn/get-started>`__ to find out more.

Next, you will need to set up your Python environment for Dulwich. An easy way to get
started is to install the checked out Dulwich package in editable mode with ``dev``
extras, preferably in a new virtual environment:

.. code:: console

   $ cd ~/path/to/checkouts/dulwich
   # Create and activate a virtual environment via your favourite method, such as pyenv,
   # uv, built-in venv module...
   $ python -m venv .venv && . .venv/bin/activate
   # Now install Dulwich and the required dependencies
   $ pip install -e ".[dev]"

This will ensure the tools needed to test your changes are installed. It is not necessary
to install Dulwich in editable mode (``-e``), but doing so is convenient for development,
as code changes will be visible immediately, without requiring a reinstall (although any
running Python processes will need to be reloaded to see the updated module
definitions). Editable mode only applies to Python code; if you modify any of the Rust
extension code, you will need to reinstall the package for the extensions to be
recompiled.

There are also other, optional dependencies which are needed to run the full test suite,
implement optional features, and provide the full typing information. They are however not
strictly necessary; the above is sufficient to start developing and have your PR pass the
tests in most cases. Please consult the ``[project.optional-dependencies]`` section in
``pyproject.toml``.

Coding style
------------
The code follows the PEP8 coding style. There are ``ruff`` rules in place that define the
exact code style, please run it to make sure your changes are conformant. See also "Style
and typing checks" below for details on running style checkers.

Public methods, functions and classes should all have doc strings. Please use
Google style docstrings to document parameters and return values.
You can generate the documentation by running ``pydoctor --docformat=google dulwich``
from the root of the repository, and then opening
``apidocs/index.html`` in your web browser.

String Types
~~~~~~~~~~~~
Like Linux, Git treats filenames as arbitrary bytestrings. There is no prescribed
encoding for these strings, and although it is fairly common to use UTF-8, any
raw byte strings are supported.

For this reason, the lower levels in Dulwich treat git-based filenames as
bytestrings. It is up to the Dulwich API user to encode and decode them if
necessary. The porcelain may accept unicode strings and convert them to
bytestrings as necessary on the fly (using 'utf-8').

* on-disk filenames: regular strings, or ideally, pathlib.Path instances
* git-repository related filenames: bytes
* object sha1 digests (20 bytes long): bytes
* object sha1 hexdigests (40 bytes long): str (bytestrings on python2, strings
  on python3)

Exceptions
~~~~~~~~~~
When catching exceptions, please catch the specific exception type rather than
a more generic type (like OSError, IOError, Exception, etc.). This will
ensure that you do not accidentally catch unrelated exceptions.
The only exception is when you are reraising an exception, e.g. when
re-raising an exception after logging it.

Do not catch bare except, although ruff will warn you about this.

Keep the code within a try/except block as small as possible, so
that you do not accidentally catch unrelated exceptions.

Deprecating functionality
~~~~~~~~~~~~~~~~~~~~~~~~~
Dulwich uses the `dissolve` package to manage deprecations. If you want to deprecate
functionality, please use the `@replace_me` decorator from the root of the
dulwich package. This will ensure that the deprecation is handled correctly:

* It will be logged as a warning
* When the version of Dulwich is bumped, the deprecation will be removed
* Users can use `dissolve migrate` to automatically replace deprecated
  functionality in their code

Tests
~~~~~

Dulwich has two kinds of tests:

* Unit tests, which test individual functions and classes
* Compatibility tests, which test that Dulwich behaves in a way that is
    compatible with C Git

The former should never invoke C Git, while the latter may do so. This is
to ensure that it is possible to run the unit tests in an environment
where C Git is not available.

Tests should not depend on the internet, or any other external services.

Avoid using mocks if at all possible; rather, design your code to be easily
testable without them. If you do need to use mocks, please use the
``unittest.mock`` module.

Running the tests
-----------------
To run the testsuite, you should be able to run ``dulwich.tests.test_suite``.
This will run the tests using unittest.

.. code:: console

   $ python -m unittest dulwich.tests.test_suite

The compatibility tests that verify Dulwich behaves in a way that is compatible
with C Git are the slowest, so you may want to avoid them while developing:

.. code:: console

   $ python -m unittest dulwich.tests.nocompat_test_suite

testr and tox configuration is also present.

Style and typing checks
-----------------------

Several static analysis tools are used to ensure code quality and consistency.

* Use ``ruff check`` to run all style-related checks.
* Use ``ruff format --check`` to check code formatting.
* Use ``mypy dulwich`` for typing checks.
* Use ``codespell`` to check for common misspellings.

Those checks are *mandatory*, a PR will not pass tests and will not be merged if
they aren't successful.

.. code:: console

   $ ruff check
   $ ruff format --check
   $ mypy dulwich
   $ codespell

In some cases you can automatically fix issues found by these tools. To do so, you can run:

.. code:: console

   $ ruff check --fix  # or pass --unsafe-fixes to apply more aggressive fixes
   $ ruff format
   $ codespell --config .codespellrc -w

Merge requests
--------------
Please either send pull requests to the maintainer (jelmer@jelmer.uk) or create
new pull requests on GitHub.

Licensing
---------
All contributions should be made under the same license that Dulwich itself
comes under: both Apache License, version 2.0 or later and GNU General Public
License, version 2.0 or later.
