# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

import os
import sys
import tempfile
from typing import Optional

import atheris

with atheris.instrument_imports():
    # We instrument `test_utils` as well, so it doesn't block coverage analysis in Fuzz Introspector:
    from test_utils import EnhancedFuzzedDataProvider, is_expected_exception

    from dulwich.repo import (
        InvalidUserIdentity,
        Repo,
    )


def TestOneInput(data) -> Optional[int]:
    fdp = EnhancedFuzzedDataProvider(data)
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Repo.init(temp_dir)
        repo.set_description(fdp.ConsumeRandomBytes())
        repo.get_description()

        try:
            # Generate a minimal set of files based on fuzz data to minimize I/O operations.
            file_names = [
                f"File{i}{fdp.ConsumeRandomString(without_surrogates=True)}"
                for i in range(min(3, fdp.ConsumeIntInRange(1, 3)))
            ]
            for file in file_names:
                with open(os.path.join(temp_dir, file), "wb") as f:
                    f.write(fdp.ConsumeRandomBytes())
        except (ValueError, OSError):
            # Exit early if the fuzzer generates an invalid filename.
            return -1

        try:
            repo.stage(file_names)
            repo.do_commit(
                message=fdp.ConsumeRandomBytes(),
                committer=fdp.ConsumeRandomBytes(),
                author=fdp.ConsumeRandomBytes(),
                commit_timestamp=fdp.ConsumeRandomInt(),
                commit_timezone=fdp.ConsumeRandomInt(),
                author_timestamp=fdp.ConsumeRandomInt(),
                author_timezone=fdp.ConsumeRandomInt(),
            )
        except InvalidUserIdentity:
            return -1
        except ValueError as e:
            if is_expected_exception(["Unable to handle non-minute offset"], e):
                return -1
            else:
                raise e


def main() -> None:
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
