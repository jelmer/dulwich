# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build/Test Commands
- Build: `make build`
- Run all tests: `make check`
- Run single test: `PYTHONPATH=$(pwd) python3 -m unittest tests.test_module_name.TestClassName.test_method_name`
- Type checking: `make typing` or `python3 -m mypy dulwich`
- Lint code: `make style` or `ruff check .`
- Fix lint issues: `make fix` or `ruff check --fix .`
- Format code: `make reformat` or `ruff format .`
- Generate coverage: `make coverage` or `make coverage-html` for HTML report

## Code Style Guidelines
- Follow PEP8 with accommodations listed in `pyproject.toml` (ruff config)
- Use Google-style docstrings for public methods, functions and classes
- Triple quotes should always be """, single quotes are ' (unless " reduces escaping)
- Git paths/filenames are treated as bytestrings (bytes), not unicode strings
- On-disk filenames: use regular strings or pathlib.Path objects
- Ensure all functionality is available in pure Python (Rust implementations optional)
- Add unit tests for new functionality and bug fixes
- All contributions must be under Apache License 2.0+ or GPL 2.0+