# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

import sys
from io import BytesIO

import atheris

with atheris.instrument_imports():
    from dulwich.bitmap import EWAHBitmap, read_bitmap_file


def TestOneInput(data) -> int | None:
    # Exercise the standalone EWAH decoder as well as the full bitmap file
    # reader, both of which parse untrusted input.
    try:
        EWAHBitmap(data)
    except ValueError:
        pass

    try:
        read_bitmap_file(BytesIO(data))
    except ValueError:
        pass


def main() -> None:
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
