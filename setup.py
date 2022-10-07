#!/usr/bin/python3
# encoding: utf-8
# Setup file for dulwich
# Copyright (C) 2008-2022 Jelmer Vernooĳ <jelmer@jelmer.uk>

from setuptools import setup, Extension, Distribution
import os
import sys


if sys.version_info < (3, 6):
    raise Exception(
        'Dulwich only supports Python 3.6 and later. '
        'For 2.7 support, please install a version prior to 0.20')


dulwich_version_string = '0.20.46'


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


setup(keywords="git vcs",
      package_data={'': ['../docs/tutorial/*.txt', 'py.typed']},
      scripts=scripts,
      ext_modules=ext_modules,
      zip_safe=False,
      distclass=DulwichDistribution,  # type: ignore
      include_package_data=True,
      tests_require=tests_require)
