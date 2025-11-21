"""Common type definitions for Dulwich."""

import sys

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    Buffer = bytes | bytearray | memoryview

__all__ = ["Buffer"]
