PYTHON = python
SETUP = $(PYTHON) setup.py
PYDOCTOR ?= pydoctor
ifeq ($(shell $(PYTHON) -c "import sys; print sys.version_info >= (2, 7)"),True)
TESTRUNNER ?= unittest
else
TESTRUNNER ?= unittest2.__main__
endif
RUNTEST = PYTHONPATH=.:$(PYTHONPATH) $(PYTHON) -m $(TESTRUNNER)

all: build

doc:: pydoctor

pydoctor::
	$(PYDOCTOR) --make-html -c dulwich.cfg

build::
	$(SETUP) build
	$(SETUP) build_ext -i

install::
	$(SETUP) install

check:: build
	$(RUNTEST) dulwich.tests.test_suite

check-nocompat:: build
	$(RUNTEST) dulwich.tests.nocompat_test_suite

check-noextensions:: clean
	$(RUNTEST) dulwich.tests.test_suite

clean::
	$(SETUP) clean --all
	rm -f dulwich/*.so
