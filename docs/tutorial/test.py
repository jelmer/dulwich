#!/usr/bin/env python
# -*- encoding: UTF-8 -*-

# Import from the Standard Library
from os import F_OK, access, mkdir
from pprint import pprint
from shutil import rmtree
from subprocess import call
from time import time

# Import from dulwich
from dulwich.repo import Repo
from dulwich.objects import Blob, Tree, Commit, parse_timezone


DIRNAME = "myrepo"
AUTHOR = "Your Name <your.email@example.com>"
TZ = parse_timezone('-200')
ENCODING = "UTF-8"


def make_commit(repo, tree_id, message):
    """Build a commit object on the same pattern. Only changing values are
    required as parameters.
    """
    commit = Commit()
    try:
        commit.parents = [repo.head()]
    except KeyError:
        # The initial commit has no parent
        pass
    commit.tree = tree_id
    commit.message = message
    commit.author = commit.committer = AUTHOR
    commit.commit_time = commit.author_time = int(time())
    commit.commit_timezone = commit.author_timezone = TZ
    commit.encoding = ENCODING
    return commit



def make_tree(repo):
    """Return the last known tree.
    """
    commit_id = repo.head()
    commit = repo.commit(commit_id)
    tree_id = commit.tree
    return repo.tree(tree_id)



def update_master(repo, commit_id):
    repo.refs['refs/heads/master'] = commit_id



def initial_commit(repo):
    # Add file content
    blob = Blob.from_string("My file content\n")
    # Add file
    tree = Tree()
    tree.add(0100644, "spam", blob.id)
    # Set commit
    commit = make_commit(repo, tree.id, "Initial commit")
    # Initial commit
    object_store = repo.object_store
    object_store.add_object(blob)
    object_store.add_object(tree)
    object_store.add_object(commit)
    # Update master
    update_master(repo, commit.id)
    # Set the master branch as the default
    repo.refs['HEAD'] = 'ref: refs/heads/master'



def test_change(repo):
    tree = make_tree(repo)
    # Change a file
    spam = Blob.from_string("My new file content\n")
    tree.add(0100644, "spam", spam.id)
    # Set commit
    commit = make_commit(repo, tree.id, "Change spam")
    # Second commit
    object_store = repo.object_store
    object_store.add_object(spam)
    object_store.add_object(tree)
    object_store.add_object(commit)
    # Update master
    update_master(repo, commit.id)



def test_add(repo):
    tree = make_tree(repo)
    # Add another file
    ham = Blob.from_string("Another\nmultiline\nfile\n")
    tree.add(0100644, "ham", ham.id)
    # Set commit
    commit = make_commit(repo, tree.id, "Add ham")
    # Second commit
    object_store = repo.object_store
    object_store.add_object(ham)
    object_store.add_object(tree)
    object_store.add_object(commit)
    # Update master
    update_master(repo, commit.id)



def test_remove(repo):
    tree = make_tree(repo)
    # Remove a file
    del tree["ham"]
    # Set commit
    commit = make_commit(repo, tree.id, 'Remove "ham"')
    # Third commit
    # No blob change, just tree operation
    object_store = repo.object_store
    object_store.add_object(tree)
    object_store.add_object(commit)
    # Update master
    update_master(repo, commit.id)



def test_rename(repo):
    tree = make_tree(repo)
    # Rename a file
    tree["eggs"] = tree["spam"]
    del tree["spam"]
    # Set commit
    commit = make_commit(repo, tree.id, 'Rename "spam" to "eggs"')
    # Fourth commit
    # No blob change, just tree operation
    object_store = repo.object_store
    object_store.add_object(tree)
    object_store.add_object(commit)
    # Update master
    update_master(repo, commit.id)



def test_history(repo):
    pprint(repo.revision_history(repo.head()))



def test_file(repo):
    tree = make_tree(repo)
    print "entries", tree.entries()
    mode, blob_id = tree["eggs"]
    blob = repo.get_blob(blob_id)
    print "eggs", repr(blob.data)



if __name__ == '__main__':
    # Creating the repository
    if access(DIRNAME, F_OK):
        rmtree(DIRNAME)
    mkdir(DIRNAME)
    repo = Repo.init(DIRNAME)
    initial_commit(repo)
    test_change(repo)
    test_add(repo)
    test_remove(repo)
    test_rename(repo)
    last_commit_id = repo.head()
    call(['git', 'gc'], cwd=DIRNAME)
    # Re-load the repo
    del repo
    repo = Repo(DIRNAME)
    # XXX the ref was removed and dulwich doesn't know where to read it
    update_master(repo, last_commit_id)
    assert last_commit_id == repo.head()
    test_history(repo)
    test_file(repo)
