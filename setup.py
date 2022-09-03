#!/usr/bin/python3
# encoding: utf-8
# Setup file for dulwich
# Copyright (C) 2008-2022 Jelmer Vernooĳ <jelmer@jelmer.uk>

from setuptools import setup, Extension, Distribution
import io
import os
import sys


if sys.version_info < (3, 6):
    raise Exception(
        'Dulwich only supports Python 3.6 and later. '
        'For 2.7 support, please install a version prior to 0.20')


dulwich_version_string = '0.20.45'


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
    for line in out.splitlines():
        line = line.decode("utf8")
        # Also parse only first digit, because 3.2.1 can't be parsed nicely
        if (line.startswith('Xcode')
                and int(line.split()[1].split('.')[0]) >= 4):
            os.environ['ARCHFLAGS'] = ''

tests_require = ['fastimport']


if '__pypy__' not in sys.modules and sys.platform != 'win32':
    tests_require.extend([
        'gevent', 'geventhttpclient', 'setuptools>=17.1'])


ext_modules = [
    Extension('dulwich._objects', ['dulwich/_objects.c']),
    Extension('dulwich._pack', ['dulwich/_pack.c']),
    Extension('dulwich._diff_tree', ['dulwich/_diff_tree.c']),
]

scripts = ['bin/dul-receive-pack', 'bin/dul-upload-pack']


with io.open(os.path.join(os.path.dirname(__file__), "README.rst"),
             encoding="utf-8") as f:
    description = f.read()

setup(name='dulwich',
      author="Jelmer Vernooij",
      author_email="jelmer@jelmer.uk",
      url="https://www.dulwich.io/",
      long_description=description,
      description="Python Git Library",
      version=dulwich_version_string,
      license='Apachev2 or later or GPLv2',
      project_urls={
          "Bug Tracker": "https://github.com/dulwich/dulwich/issues",
          "Repository": "https://www.dulwich.io/code/",
          "GitHub": "https://github.com/dulwich/dulwich",
      },
      keywords="git vcs",
      packages=['dulwich', 'dulwich.cloud', 'dulwich.tests',
                'dulwich.tests.compat', 'dulwich.contrib'],
      package_data={'': ['../docs/tutorial/*.txt', 'py.typed']},
      scripts=scripts,
      ext_modules=ext_modules,
      zip_safe=False,
      distclass=DulwichDistribution,  # type: ignore
      install_requires=['urllib3>=1.24.1', 'certifi'],
      include_package_data=True,
      test_suite='dulwich.tests.test_suite',
      tests_require=tests_require,
      entry_points={
          "console_scripts": ["dulwich=dulwich.cli:main"]
      },
      python_requires='>=3.6',
      classifiers=[
          'Development Status :: 4 - Beta',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
          'Programming Language :: Python :: Implementation :: CPython',
          'Programming Language :: Python :: Implementation :: PyPy',
          'Operating System :: POSIX',
          'Operating System :: Microsoft :: Windows',
          'Topic :: Software Development :: Version Control',
      ],
      extras_require={
          'fastimport': ['fastimport'],
          'https': ['urllib3>=1.24.1'],
          'pgp': ['gpg'],
          'paramiko': ['paramiko'],
      })
