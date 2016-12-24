#!/usr/bin/python
# encoding: utf-8
# Setup file for dulwich
# Copyright (C) 2008-2016 Jelmer Vernooĳ <jelmer@jelmer.uk>

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension
from distutils.core import Distribution

dulwich_version_string = '0.16.0'

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
        return not self.pure

    global_options = Distribution.global_options + [
        ('pure', None, "use pure Python code instead of C "
                       "extensions (slower on CPython)")]

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
        l = l.decode("utf8")
        # Also parse only first digit, because 3.2.1 can't be parsed nicely
        if l.startswith('Xcode') and int(l.split()[1].split('.')[0]) >= 4:
            os.environ['ARCHFLAGS'] = ''

tests_require = ['fastimport']
if not '__pypy__' in sys.modules and not sys.platform == 'win32':
    tests_require.extend([
        'gevent', 'geventhttpclient', 'mock', 'setuptools>=17.1'])

if sys.version_info[0] > 2 and sys.platform == 'win32':
    # C Modules don't build for python3 windows, and prevent tests from running
    ext_modules = []
else:
    ext_modules = [
        Extension('dulwich._objects', ['dulwich/_objects.c'],
                  include_dirs=include_dirs),
        Extension('dulwich._pack', ['dulwich/_pack.c'],
                  include_dirs=include_dirs),
        Extension('dulwich._diff_tree', ['dulwich/_diff_tree.c'],
                  include_dirs=include_dirs),
    ]


setup(name='dulwich',
      description='Python Git Library',
      keywords='git',
      version=dulwich_version_string,
      url='https://www.dulwich.io/',
      license='Apachev2 or later or GPLv2',
      author='Jelmer Vernooĳ',
      author_email='jelmer@jelmer.uk',
      long_description="""
      Python implementation of the Git file formats and protocols,
      without the need to have git installed.

      All functionality is available in pure Python. Optional
      C extensions can be built for improved performance.

      The project is named after the part of London that Mr. and Mrs. Git live in
      in the particular Monty Python sketch.
      """,
      packages=['dulwich', 'dulwich.tests', 'dulwich.tests.compat', 'dulwich.contrib'],
      package_data={'': ['../docs/tutorial/*.txt']},
      scripts=['bin/dulwich', 'bin/dul-receive-pack', 'bin/dul-upload-pack'],
      classifiers=[
          'Development Status :: 4 - Beta',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: Implementation :: CPython',
          'Programming Language :: Python :: Implementation :: PyPy',
          'Operating System :: POSIX',
          'Topic :: Software Development :: Version Control',
      ],
      ext_modules=ext_modules,
      test_suite='dulwich.tests.test_suite',
      tests_require=tests_require,
      distclass=DulwichDistribution,
      include_package_data=True,
      )
