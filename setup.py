#!/usr/bin/python
# Setup file for bzr-git
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>

from distutils.cmd import Command
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError, DistutilsPlatformError
try:
    from setuptools import setup, Extension, Feature
    from setuptools.command.bdist_egg import bdist_egg
except ImportError:
    from distutils.core import setup, Extension
    Feature = None
    bdist_egg = None

dulwich_version_string = '0.4.2'

include_dirs = []
# Windows MSVC support
import sys
if sys.platform == 'win32':
    include_dirs.append('dulwich')

#sys.path.append(os.path.join('doc', 'common'))

_speedup_available = False

class optional_build_ext(build_ext):
    # This class allows C extension building to fail.
    def run(self):
        try:
            print "trying run"
            build_ext.run(self)
        except DistutilsPlatformError, e:
            self._unavailable(e)

    def build_extension(self, ext):
        try:
            print "trying build_extension %s" % (ext,)
            build_ext.build_extension(self, ext)
            global _speedup_available
            _speedup_available = True
        except CCompilerError, e:
            self._unavailable(e)

    def _unavailable(self, exc):
        print('*' * 70)
        print("""WARNING:
An optional C extension could not be compiled, speedups will not be
available.""")
        print('*' * 70)
        print(exc)

if Feature:
    speedups = Feature(
        "optional C speed-enhancements",
        standard = True,
        ext_modules=[
            Extension('dulwich._objects', ['dulwich/_objects.c'],
                      include_dirs=include_dirs),
            Extension('dulwich._pack', ['dulwich/_pack.c'],
                      include_dirs=include_dirs),
            ],
    )
else:
    speedups = None


# Setuptools need some help figuring out if the egg is "zip_safe" or not
if bdist_egg:
    class my_bdist_egg(bdist_egg):
        def zip_safe(self):
            return not _speedup_available and bdist_egg.zip_safe(self)


cmdclass = {'build_ext': optional_build_ext}
if bdist_egg:
    cmdclass['bdist_egg'] = my_bdist_egg



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
      scripts=['bin/dulwich', 'bin/dul-daemon'],
      features = {'speedups': speedups},
      cmdclass = cmdclass,
      )
