#!/usr/bin/python
# Setup file for bzr-git
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
from distutils.extension import Extension

dulwich_version_string = '0.4.1'

include_dirs = []
# Windows MSVC support
import sys
if sys.platform == 'win32':
    include_dirs.append('dulwich')


setup(name='dulwich',
      description='Pure-Python Git Library',
      keywords='git',
      version=dulwich_version_string,
      url='http://samba.org/~jelmer/dulwich',
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
      ext_modules=[
          Extension('dulwich._objects', ['dulwich/_objects.c'],
                    include_dirs=include_dirs),
          Extension('dulwich._pack', ['dulwich/_pack.c'],
                    include_dirs=include_dirs),
          ],
      )
