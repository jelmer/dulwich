PYTHON = python
SETUP = $(PYTHON) setup.py
TESTRUNNER = $(shell which trial)

all: build 

build::
	$(SETUP) build
	$(SETUP) build_ext -i

install::
	$(SETUP) install

check:: build
	PYTHONPATH=. $(PYTHON) $(TESTRUNNER) dulwich

check-noextensions:: clean
	PYTHONPATH=. $(PYTHON) $(TESTRUNNER) dulwich

clean::
	$(SETUP) clean --all
	rm -f dulwich/*.so
