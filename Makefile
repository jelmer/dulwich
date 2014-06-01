PYTHON = python
PYLINT = pylint
SETUP = $(PYTHON) setup.py
PYDOCTOR ?= pydoctor
ifeq ($(shell $(PYTHON) -c "import sys; print(sys.version_info >= (2, 7))"),True)
TESTRUNNER ?= unittest
else
TESTRUNNER ?= unittest2.__main__
endif
RUNTEST = PYTHONPATH=.:$(PYTHONPATH) $(PYTHON) -m $(TESTRUNNER)

DESTDIR=/

all: build

doc:: pydoctor

pydoctor::
	$(PYDOCTOR) --make-html -c dulwich.cfg

build::
	$(SETUP) build
	$(SETUP) build_ext -i

install::
	$(SETUP) install --root="$(DESTDIR)"

check:: build
	$(RUNTEST) dulwich.tests.test_suite

check-tutorial:: build
	$(RUNTEST) dulwich.tests.tutorial_test_suite

check-nocompat:: build
	$(RUNTEST) dulwich.tests.nocompat_test_suite

check-compat:: build
	$(RUNTEST) dulwich.tests.compat_test_suite

check-pypy:: clean
	$(MAKE) check-noextensions PYTHON=pypy

check-noextensions:: clean
	$(RUNTEST) dulwich.tests.test_suite

check-all: check check-pypy check-noextensions

clean::
	$(SETUP) clean --all
	rm -f dulwich/*.so

lint::
	$(PYLINT) --rcfile=.pylintrc --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" dulwich
