# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

import sys
from io import BytesIO

import atheris

with atheris.instrument_imports():
    from dulwich.bitmap import EWAHBitmap, read_bitmap_file


# There is no pack to bound bitmap sizes against, so pick a limit that keeps
# memory use well below the fuzzer's RSS limit.
MAX_BIT_COUNT = 1 << 20


def TestOneInput(data) -> int | None:
    # Exercise the standalone EWAH decoder as well as the full bitmap file
    # reader, both of which parse untrusted input.
    try:
        EWAHBitmap(data, max_bit_count=MAX_BIT_COUNT)
    except ValueError:
        pass

    try:
        read_bitmap_file(BytesIO(data), num_objects=MAX_BIT_COUNT)
    except ValueError:
        pass


def main() -> None:
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
