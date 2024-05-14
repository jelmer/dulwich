import os
import sys
import tempfile

import atheris

with atheris.instrument_imports():
    # We instrument `test_utils` as well, so it doesn't block coverage analysis in Fuzz Introspector:
    from test_utils import EnhancedFuzzedDataProvider

    from dulwich.repo import (
        InvalidUserIdentity,
        Repo,
    )


def TestOneInput(data):
    fdp = EnhancedFuzzedDataProvider(data)
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Repo.init(temp_dir)
        repo.set_description(fdp.ConsumeRandomBytes())
        repo.get_description()

        # Generate a minimal set of files based on fuzz data to minimize I/O operations.
        file_paths = [
            os.path.join(temp_dir, f"File{i}")
            for i in range(min(3, fdp.ConsumeIntInRange(1, 3)))
        ]
        for file_path in file_paths:
            with open(file_path, "wb") as f:
                f.write(fdp.ConsumeRandomBytes())

        try:
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

        for file_path in file_paths:
            with open(file_path, "wb") as f:
                f.write(fdp.ConsumeRandomBytes())

        repo.stage(file_paths)
        repo.do_commit(
            message=fdp.ConsumeRandomBytes(),
        )


def main():
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
