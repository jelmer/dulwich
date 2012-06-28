#!/usr/bin/python
# Setup file for dulwich
# Copyright (C) 2008-2011 Jelmer Vernooij <jelmer@samba.org>

try:
    from setuptools import setup, Extension
    has_setuptools = True
except ImportError:
    from distutils.core import setup, Extension
    has_setuptools = False
from distutils.core import Distribution

dulwich_version_string = '0.8.7.wavii'

include_dirs = []
# Windows MSVC support
import os
import sys
if sys.platform == 'win32':
    include_dirs.append('dulwich')


class DulwichDistribution(Distribution):

    def is_pure(self):
        if self.pure:
            return True

    def has_ext_modules(self):
        return not self.pure and not '__pypy__' in sys.modules

    global_options = Distribution.global_options + [
        ('pure', None, 
            "use pure Python code instead of C extensions (slower on CPython)")]

    pure = False

if sys.platform == 'darwin' and os.path.exists('/usr/bin/xcodebuild'):
    # XCode 4.0 dropped support for ppc architecture, which is hardcoded in
    # distutils.sysconfig
    import subprocess
    p = subprocess.Popen(
        ['/usr/bin/xcodebuild', '-version'], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, env={})
    out, err = p.communicate()
    for l in out.splitlines():
        # Also parse only first digit, because 3.2.1 can't be parsed nicely
        if (l.startswith('Xcode') and
            int(l.split()[1].split('.')[0]) >= 4):
            os.environ['ARCHFLAGS'] = ''

setup_kwargs = {}

if has_setuptools:
    setup_kwargs['test_suite'] = 'dulwich.tests'

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
      **setup_kwargs
      )
