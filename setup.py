#!/usr/bin/python
# Setup file for bzr-git
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>

from distutils.core import setup
from distutils.extension import Extension

dulwich_version_string = '0.3.0'

setup(name='dulwich',
      description='Pure-Python Git Library',
      keywords='git',
      version=dulwich_version_string,
      url='http://launchpad.net/dulwich',
      download_url='http://samba.org/~jelmer/dulwich/dulwich-%s.tar.gz' % dulwich_version_string,
      license='GPLv2 or later',
      author='Jelmer Vernooij',
      author_email='jelmer@samba.org',
      long_description="""
      Simple Pure-Python implementation of the Git file formats and 
      protocols. Dulwich is the place where Mr. and Mrs. Git live 
      in one of the Monty Python sketches.
      """,
      packages=['dulwich', 'dulwich.tests'],
      scripts=['bin/dulwich', 'bin/dul-daemon'],
      ext_modules=[
          Extension('dulwich/_objects', ['dulwich/_objects.c']),
          Extension('dulwich/_pack', ['dulwich/_pack.c']),
          ],
      )
