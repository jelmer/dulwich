PYTHON = python
PYFLAKES = pyflakes
PEP8 = pep8
FLAKE8 ?= flake8
SETUP = $(PYTHON) setup.py
PYDOCTOR ?= pydoctor
TESTRUNNER ?= unittest
RUNTEST = PYTHONHASHSEED=random PYTHONPATH=.:$(PYTHONPATH) $(PYTHON) -m $(TESTRUNNER) $(TEST_OPTIONS)
COVERAGE = python3-coverage

DESTDIR=/

all: build

doc:: pydoctor
doc:: sphinx

sphinx::
	$(MAKE) -C docs html

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

flakes:
	$(PYFLAKES) dulwich

pep8:
	$(PEP8) dulwich

style:
	$(FLAKE8) --exclude=build,.git,build-pypy,.tox

before-push: check
	git diff origin/master | $(PEP8) --diff

coverage:
	$(COVERAGE) run --source=dulwich -m unittest dulwich.tests.test_suite dulwich.contrib.test_suite

coverage-html: coverage
	$(COVERAGE) html
