# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

import sys
from io import BytesIO

import atheris
from test_utils import is_expected_exception

with atheris.instrument_imports():
    from dulwich.config import ConfigFile


def TestOneInput(data) -> int | None:
    try:
        ConfigFile.from_file(BytesIO(data))
    except ValueError as e:
        expected_exceptions = [
            "without section",
            "invalid variable name",
            "expected trailing ]",
            "invalid section name",
            "Invalid subsection",
            "escape character",
            "missing end quote",
        ]
        if is_expected_exception(expected_exceptions, e):
            return -1
        else:
            raise e


def main() -> None:
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
