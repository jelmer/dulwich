PYTHON = python
SETUP = $(PYTHON) setup.py
PYDOCTOR ?= pydoctor
TESTRUNNER = $(shell which nosetests)
TESTFLAGS =

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
	PYTHONPATH=.:$(PYTHONPATH) $(PYTHON) $(TESTRUNNER) dulwich
	which git > /dev/null && PYTHONPATH=.:$(PYTHONPATH) $(PYTHON) $(TESTRUNNER) $(TESTFLAGS) -i compat

check-noextensions:: clean
	PYTHONPATH=.:$(PYTHONPATH) $(PYTHON) $(TESTRUNNER) $(TESTFLAGS) dulwich

check-compat:: build
	PYTHONPATH=.:$(PYTHONPATH) $(PYTHON) $(TESTRUNNER) $(TESTFLAGS) -i compat

clean::
	$(SETUP) clean --all
	rm -f dulwich/*.so

coverage:: build
	PYTHONPATH=.:$(PYTHONPATH) $(PYTHON) $(TESTRUNNER) --cover-package=dulwich --with-coverage --cover-erase --cover-inclusive dulwich

coverage-annotate: coverage
	python-coverage -a -o /usr
