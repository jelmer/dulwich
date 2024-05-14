import sys
import stat

import atheris

with atheris.instrument_imports():
    # We instrument `test_utils` as well, so it doesn't block coverage analysis in Fuzz Introspector:
    from test_utils import EnhancedFuzzedDataProvider, is_expected_exception
    from dulwich.objects import Blob, Tree, Commit, S_IFGITLINK
    from dulwich.errors import ObjectFormatException
    from dulwich.repo import (
        MemoryRepo,
        InvalidUserIdentity,
    )


def TestOneInput(data):
    fdp = EnhancedFuzzedDataProvider(data)
    repo = MemoryRepo()
    blob = Blob.from_string(fdp.ConsumeRandomBytes())
    tree = Tree()
    tree.add(
        fdp.ConsumeRandomBytes(),
        fdp.PickValueInList([stat.S_IFREG, stat.S_IFLNK, stat.S_IFDIR, S_IFGITLINK]),
        blob.id,
    )
    commit = Commit()
    commit.tree = tree.id
    commit.author = fdp.ConsumeRandomBytes()
    commit.committer = fdp.ConsumeRandomBytes()
    commit.commit_time = fdp.ConsumeRandomInt()
    commit.commit_timezone = fdp.ConsumeRandomInt()
    commit.author_time = fdp.ConsumeRandomInt()
    commit.author_timezone = fdp.ConsumeRandomInt()
    commit.message = fdp.ConsumeRandomBytes()

    object_store = repo.object_store

    try:
        object_store.add_object(blob)
        object_store.add_object(tree)
        object_store.add_object(commit)
    except (InvalidUserIdentity, ObjectFormatException):
        return -1
    except ValueError as e:
        expected_exceptions = [
            "subsection not found",
            "Unable to handle non-minute offset",
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
