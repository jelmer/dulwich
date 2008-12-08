PYTHON = python
SETUP = $(PYTHON) setup.py
TRIAL = trial

all: build build-inplace

build::
	$(SETUP) build

install::
	$(SETUP) install

check::
	PYTHONPATH=. $(TRIAL) dulwich

clean::
	$(SETUP) clean
