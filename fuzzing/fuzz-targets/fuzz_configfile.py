import sys
from io import BytesIO
from test_utils import is_expected_exception

import atheris

with atheris.instrument_imports():
    from dulwich.config import ConfigFile


def TestOneInput(data):
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


def main():
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
