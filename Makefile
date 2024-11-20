PYTHON = python3
RUFF ?= $(PYTHON) -m ruff 
SETUP = $(PYTHON) setup.py
TESTRUNNER ?= unittest
RUNTEST = PYTHONHASHSEED=random PYTHONPATH=$(shell pwd)$(if $(PYTHONPATH),:$(PYTHONPATH),) $(PYTHON) -m $(TESTRUNNER) $(TEST_OPTIONS)
COVERAGE = python3-coverage

DESTDIR=/

all: build

doc:: sphinx

sphinx::
	$(MAKE) -C docs html

build::
	$(SETUP) build
	$(SETUP) build_ext -i

install::
	$(SETUP) install --root="$(DESTDIR)"

check:: build
	$(RUNTEST) tests.test_suite

check-tutorial:: build
	$(RUNTEST) tests.tutorial_test_suite

check-nocompat:: build
	$(RUNTEST) tests.nocompat_test_suite

check-compat:: build
	$(RUNTEST) tests.compat_test_suite

check-pypy:: clean
	$(MAKE) check-noextensions PYTHON=pypy

check-noextensions:: clean
	$(RUNTEST) tests.test_suite

check-contrib:: clean
	$(RUNTEST) -v dulwich.contrib.test_suite

check-all: check check-pypy check-noextensions

typing:
	$(PYTHON) -m mypy dulwich

clean::
	$(SETUP) clean --all
	rm -f dulwich/*.so

style:
	$(RUFF) check .

coverage:
	$(COVERAGE) run -m unittest tests.test_suite dulwich.contrib.test_suite

coverage-html: coverage
	$(COVERAGE) html

.PHONY: apidocs

apidocs:
	pydoctor --intersphinx http://urllib3.readthedocs.org/en/latest/objects.inv --intersphinx http://docs.python.org/3/objects.inv --docformat=google dulwich --project-url=https://www.dulwich.io/ --project-name=dulwich

fix:
	ruff check --fix .

reformat:
	ruff format .

.PHONY: codespell

codespell:
	codespell --config .codespellrc .
