PYTHON = python
SETUP = $(PYTHON) setup.py
TRIAL = $(shell which trial)

all: build 

build::
	$(SETUP) build
	$(SETUP) build_ext -i

install::
	$(SETUP) install

check:: build
	PYTHONPATH=. $(PYTHON) $(TRIAL) dulwich

check-noextensions:: clean
	PYTHONPATH=. $(PYTHON) $(TRIAL) dulwich

clean::
	$(SETUP) clean
