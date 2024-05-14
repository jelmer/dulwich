import stat
import sys
from io import BytesIO

import atheris

with atheris.instrument_imports():
    # We instrument `test_utils` as well, so it doesn't block coverage analysis in Fuzz Introspector:
    from test_utils import EnhancedFuzzedDataProvider, is_expected_exception

    from dulwich.errors import ObjectFormatException
    from dulwich.objects import S_IFGITLINK, Blob, Commit, Tree
    from dulwich.patch import write_tree_diff
    from dulwich.repo import (
        InvalidUserIdentity,
        MemoryRepo,
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

    commit2 = Commit()
    commit2.tree = tree.id
    commit2.parents = [commit.id]
    commit2.author = commit.author
    commit2.committer = commit.committer
    commit2.commit_time = fdp.ConsumeRandomInt()
    commit2.commit_timezone = fdp.ConsumeRandomInt()
    commit2.author_time = fdp.ConsumeRandomInt()
    commit2.author_timezone = fdp.ConsumeRandomInt()
    commit2.message = fdp.ConsumeRandomBytes()

    try:
        blob.data = fdp.ConsumeRandomBytes()
        repo.object_store.add_object(blob)
        repo.object_store.add_object(tree)
        repo.object_store.add_object(commit2)
        out = BytesIO()
        write_tree_diff(out, repo.object_store, commit.tree, tree.id)
    except (InvalidUserIdentity, ObjectFormatException):
        return -1
    except ValueError as e:
        expected_exceptions = [
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
