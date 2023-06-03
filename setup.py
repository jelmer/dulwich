# encoding: utf-8

import os
from setuptools import setup

long_description = open(os.path.join(os.path.dirname(__file__), "README.rst")).read()

def install_data_files_hack():
    # This is a clever hack to circumvent distutil's data_files
    # policy "install once, find never". Definitely a TODO!
    # -- https://groups.google.com/group/comp.lang.python/msg/2105ee4d9e8042cb
    from distutils.command.install import INSTALL_SCHEMES

    for scheme in INSTALL_SCHEMES.values():
        scheme["data"] = scheme["purelib"]


install_data_files_hack()

requires = [
    "flask",
    "Werkzeug>=0.15.0",
    "pygments",
    "httpauth",
    "humanize",
    'dulwich>=0.19.3;python_version>="3.5"',
    'dulwich>=0.19.3,<0.20;python_version<"3.5"',
]

setup(
    name="klaus",
    version="2.0.3",
    author="Jonas Haag",
    author_email="jonas@lophus.org",
    packages=["klaus", "klaus.contrib"],
    include_package_data=True,
    zip_safe=False,
    url="https://github.com/jonashaag/klaus",
    description="The first Git web viewer that Just Works™.",
    long_description=long_description,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "Topic :: Software Development :: Version Control",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: ISC License (ISCL)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
    ],
    install_requires=requires,
    entry_points={
        "console_scripts": ["klaus=klaus.cli:main"],
    },
)
