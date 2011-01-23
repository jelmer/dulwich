.. _tutorial-repo:

The Repository
==============

After this introduction, let's start directly with code::

  >>> from dulwich.repo import Repo

The access to a repository is through the Repo object. You can open an
existing repository or you can create a new one. There are two types of Git
repositories:

  Regular Repositories -- They are the ones you create using ``git init`` and
  you daily use. They contain a ``.git`` folder.

  Bare Repositories -- There is not ".git" folder. The top-level folder
  contains itself the "branches", "hooks"... folders. These are used for
  published repositories (mirrors). They do not have a working tree.

Let's create a folder and turn it into a repository, like ``git init`` would::

  >>> from os import mkdir
  >>> mkdir("myrepo")
  >>> repo = Repo.init("myrepo")
  >>> repo
  <Repo at 'myrepo'>

You can already look a the structure of the "myrepo/.git" folder, though it
is mostly empty for now.
