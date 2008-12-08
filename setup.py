#!/usr/bin/python
# Setup file for bzr-git
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>

from distutils.core import setup

setup(name='dulwich',
      description='Pure-Python Git Library',
      keywords='git',
      version='0.0.1',
      url='http://launchpad.net/dulwich',
      download_url='http://launchpad.net/dulwich',
      license='GPL',
      author='Jelmer Vernooij',
      author_email='jelmer@samba.org',
      long_description="""
      Simple Pure-Python implementation of the Git file formats and 
      protocols. Dulwich is the place where Mr. and Mrs. Git live 
      in one of the Monty Python sketches.
      """,
      packages=['dulwich', 'dulwich.tests'],
      )
