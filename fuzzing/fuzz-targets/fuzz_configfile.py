import atheris
import sys
from io import BytesIO

with atheris.instrument_imports():
    from dulwich.config import ConfigFile


def is_expected_error(error_list, error_msg):
    for error in error_list:
        if error in error_msg:
            return True
    return False


def TestOneInput(data):
    try:
        ConfigFile.from_file(BytesIO(data))
    except ValueError as e:
        expected_errors = [
            "without section",
            "invalid variable name",
            "expected trailing ]",
            "invalid section name",
            "Invalid subsection",
            "escape character",
            "missing end quote",
        ]
        if is_expected_error(expected_errors, str(e)):
            return -1
        else:
            raise e


def main():
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
