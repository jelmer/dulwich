#!/usr/bin/python3
# Setup file for dulwich
# Copyright (C) 2008-2022 Jelmer Vernooĳ <jelmer@jelmer.uk>

import os
import sys

from setuptools import setup
from setuptools_rust import Binding, RustExtension

if sys.platform == "darwin" and os.path.exists("/usr/bin/xcodebuild"):
    # XCode 4.0 dropped support for ppc architecture, which is hardcoded in
    # distutils.sysconfig
    import subprocess

    p = subprocess.Popen(
        ["/usr/bin/xcodebuild", "-version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={},
    )
    out, err = p.communicate()
    for line in out.splitlines():
        line = line.decode("utf8")
        # Also parse only first digit, because 3.2.1 can't be parsed nicely
        if line.startswith("Xcode") and int(line.split()[1].split(".")[0]) >= 4:
            os.environ["ARCHFLAGS"] = ""

tests_require = ["fastimport"]


if "__pypy__" not in sys.modules and sys.platform != "win32":
    tests_require.extend(["gevent", "geventhttpclient", "setuptools>=17.1"])


optional = os.environ.get("CIBUILDWHEEL", "0") != "1"

rust_extensions = [
    RustExtension(
        "dulwich._objects",
        "crates/objects/Cargo.toml",
        binding=Binding.PyO3,
        optional=optional,
    ),
    RustExtension(
        "dulwich._pack",
        "crates/pack/Cargo.toml",
        binding=Binding.PyO3,
        optional=optional,
    ),
    RustExtension(
        "dulwich._diff_tree",
        "crates/diff-tree/Cargo.toml",
        binding=Binding.PyO3,
        optional=optional,
    ),
]

# Ideally, setuptools would just provide a way to do this
if "--pure" in sys.argv:
    sys.argv.remove("--pure")
    rust_extensions = []


setup(
    package_data={"": ["py.typed"]},
    rust_extensions=rust_extensions,
    tests_require=tests_require,
)
