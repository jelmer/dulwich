#!/usr/bin/python
# Setup file for bzr-git
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension
from distutils.core import Distribution

dulwich_version_string = '0.5.0'

include_dirs = []
# Windows MSVC support
import sys
if sys.platform == 'win32':
    include_dirs.append('dulwich')

ext_modules = [
    Extension('dulwich._objects', ['dulwich/_objects.c'],
              include_dirs=include_dirs),
    Extension('dulwich._pack', ['dulwich/_pack.c'],
              include_dirs=include_dirs),
    ]

try:
    from setuptools import Feature
except ImportError:
    speedups = None
    mandatory_ext_modules = ext_modules
else:
    mandatory_ext_modules = []
    speedups = Feature(
        "optional C speed-enhancements",
        standard = True,
        ext_modules=ext_modules,
    )


class dulwichDistribution(Distribution):
    def is_pure(self):
        if self.pure:
            return True
    def has_ext_modules(self):
        if self.pure:
            return False
        
dulwichDistribution.pure = 0
dulwichDistribution.global_options.append(('pure', None, "use pure (slow) Python "
"code instead of C extensions"))
        

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
      scripts=['bin/dulwich', 'bin/dul-daemon', 'bin/dul-web'],
      features = {'speedups': speedups},
      ext_modules = mandatory_ext_modules,
      distclass=dulwichDistribution,
      )
