#!/usr/bin/python
# Setup file for dulwich
# Copyright (C) 2008-2010 Jelmer Vernooij <jelmer@samba.org>

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension
from distutils.core import Distribution

dulwich_version_string = '0.7.1'

include_dirs = []
# Windows MSVC support
import sys
if sys.platform == 'win32':
    include_dirs.append('dulwich')


class DulwichDistribution(Distribution):

    def is_pure(self):
        if self.pure:
            return True

    def has_ext_modules(self):
        return not self.pure

    global_options = Distribution.global_options + [
        ('pure', None, 
            "use pure (slower) Python code instead of C extensions")]

    pure = False

        
setup(name='dulwich',
      description='Python Git Library',
      keywords='git',
      version=dulwich_version_string,
      url='http://samba.org/~jelmer/dulwich',
      download_url='http://samba.org/~jelmer/dulwich/dulwich-%s.tar.gz' % dulwich_version_string,
      license='GPLv2 or later',
      author='Jelmer Vernooij',
      author_email='jelmer@samba.org',
      long_description="""
      Simple Python implementation of the Git file formats and
      protocols. Dulwich is the place where Mr. and Mrs. Git live
      in one of the Monty Python sketches.

      All functionality is available in pure Python, but (optional)
      C extensions are also available for better performance.
      """,
      packages=['dulwich', 'dulwich.tests'],
      scripts=['bin/dulwich', 'bin/dul-daemon', 'bin/dul-web'],
      ext_modules = [
          Extension('dulwich._objects', ['dulwich/_objects.c'],
                    include_dirs=include_dirs),
          Extension('dulwich._pack', ['dulwich/_pack.c'],
              include_dirs=include_dirs),
          Extension('dulwich._diff_tree', ['dulwich/_diff_tree.c'],
              include_dirs=include_dirs),
          ],
      distclass=DulwichDistribution,
      )
