"""Entry point for running dulwich as a module.

This module allows dulwich to be run as a Python module using the -m flag:
    python -m dulwich

It serves as the main entry point for the dulwich command-line interface.
"""

from . import cli

if __name__ == "__main__":
    cli._main()
