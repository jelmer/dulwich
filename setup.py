#!/usr/bin/python
# Setup file for dulwich
# Copyright (C) 2008-2011 Jelmer Vernooij <jelmer@samba.org>

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension
from distutils.core import Distribution

dulwich_version_string = '0.10.1a'

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

if sys.version_info[0] == 2:
    tests_require = ['fastimport', 'mock']
    if not '__pypy__' in sys.modules:
        tests_require.extend(['gevent', 'geventhttpclient'])
else:
    # fastimport, gevent, geventhttpclient are not available for PY3
    # mock only used for test_swift, which requires gevent/geventhttpclient
    tests_require = []
if sys.version_info < (2, 7):
    tests_require.append('unittest2')

setup(name='dulwich',
      description='Python Git Library',
      keywords='git',
      version=dulwich_version_string,
      url='https://samba.org/~jelmer/dulwich',
      license='GPLv2 or later',
      author='Jelmer Vernooij',
      author_email='jelmer@samba.org',
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
          'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Operating System :: POSIX',
          'Topic :: Software Development :: Version Control',
      ],
      ext_modules=[
          Extension('dulwich._objects', ['dulwich/_objects.c'],
                    include_dirs=include_dirs),
          Extension('dulwich._pack', ['dulwich/_pack.c'],
              include_dirs=include_dirs),
          Extension('dulwich._diff_tree', ['dulwich/_diff_tree.c'],
              include_dirs=include_dirs),
      ],
      test_suite='dulwich.tests.test_suite',
      tests_require=tests_require,
      distclass=DulwichDistribution,
      include_package_data=True,
      )
