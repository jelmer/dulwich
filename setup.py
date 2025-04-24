#!/usr/bin/python3
# Setup file for dulwich
# Copyright (C) 2008-2022 Jelmer Vernooĳ <jelmer@jelmer.uk>
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

import os
import sys

from setuptools import setup

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

# Ideally, setuptools would just provide a way to do this
if "PURE" in os.environ or "--pure" in sys.argv:
    if "--pure" in sys.argv:
        sys.argv.remove("--pure")
    setup_requires = []
    rust_extensions = []
else:
    setup_requires = ["setuptools_rust"]
    # We check for egg_info since that indicates we are running prepare_metadata_for_build_*
    if "egg_info" in sys.argv:
        rust_extensions = []
    else:
        from setuptools_rust import Binding, RustExtension

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


setup(
    package_data={"": ["py.typed"]},
    rust_extensions=rust_extensions,
    setup_requires=setup_requires,
)
