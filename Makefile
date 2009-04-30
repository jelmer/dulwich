PYTHON = python
SETUP = $(PYTHON) setup.py
TRIAL = trial

all: build 

build::
	$(SETUP) build
	$(SETUP) build_ext -i

install::
	$(SETUP) install

check:: build
	PYTHONPATH=. $(TRIAL) dulwich

check-noextensions:: clean
	PYTHONPATH=. $(TRIAL) dulwich

clean::
	$(SETUP) clean
