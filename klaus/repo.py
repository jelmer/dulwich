import functools
import io
import os
import stat
import subprocess
import threading

from dulwich.objects import S_ISGITLINK
from dulwich.object_store import tree_lookup_path
from dulwich.objects import Blob
from dulwich.errors import NotTreeError
import dulwich, dulwich.patch

try:
    from dulwich.refs import SymrefLoop
except ImportError:  # dulwich < 0.20.46
    InaccessibleRef = KeyError
else:
    InaccessibleRef = (SymrefLoop, KeyError)


from klaus.utils import (
    force_unicode,
    parent_directory,
    repo_human_name,
    encode_for_git,
    decode_from_git,
)
from klaus.diff import render_diff


NOT_SET = "__not_set__"


def cached_call(key, validator, producer, _cache={}):
    data, old_validator = _cache.get(key, (None, NOT_SET))
    if old_validator != validator:
        data = producer()
    _cache[key] = (data, validator)
    return data


def synchronized(func, lock=threading.RLock()):
    @functools.wraps(func)
    def synchronized_func(*args, **kwargs):
        with lock:
            return func(*args, **kwargs)
    return synchronized_func


class FancyRepo(object):
    """A wrapper around Dulwich's Repo that adds some helper methods."""

    def __init__(self, path, namespace):
        self.dulwich_repo = dulwich.repo.Repo(path)
        self.namespace = namespace

    @property
    def path(self):
        return self.dulwich_repo.path

    @property
    def name(self):
        return repo_human_name(self.path)

    @property
    def namespaced_name(self):
        if self.namespace:
            return "~{}/{}".format(self.namespace, self.name)
        else:
            return self.name

    @synchronized
    def __getitem__(self, key):
        return self.dulwich_repo[key]

    # TODO: factor out stuff into dulwich
    @synchronized
    def get_last_updated_at(self):
        """Get datetime of last commit to this repository.

        Caches the result to speed up the repo_list page.
        Cache is invalidated if one of the ref targets changes,
        eg. a new commit has been made and 'refs/heads/master' was changed.
        """
        def _get_commit_time_cached(ref_id):
            return cached_call(
                key=(ref_id, "_get_commit_time"),
                validator=None,
                producer=lambda: _get_commit_time(ref_id),
            )

        def _get_commit_time(ref_id):
            try:
                return self[ref_id].commit_time
            except (KeyError, AttributeError):
                # Missing or non-commit object
                return None

        max_refs = 1000
        if len(self.dulwich_repo.refs.keys()) > max_refs:
            # If we have too many refs, look at the branches only. (And HEAD, see below.)
            base = b"refs/heads"
        else:
            base = None
        all_ids = list(self.dulwich_repo.refs.as_dict(base).values())
        # If we still have too many refs, keep only some.
        if len(all_ids) > max_refs:
            all_ids = sorted(all_ids)[:max_refs]
        # Always add HEAD.
        try:
            all_ids.append(self.dulwich_repo.refs[b"HEAD"])
        except KeyError:
            pass

        commit_times = filter(None, map(_get_commit_time_cached, all_ids))
        try:
            return max(commit_times)
        except ValueError:
            # Python 2 does not support max(..., default=)
            return None

    @property
    @synchronized
    def cloneurl(self):
        """Retrieve the gitweb notion of the public clone URL of this repo."""
        f = self.dulwich_repo.get_named_file("cloneurl")
        if f is not None:
            return force_unicode(f.read())
        c = self.dulwich_repo.get_config()
        try:
            return force_unicode(c.get(b"gitweb", b"url"))
        except KeyError:
            return None

    @synchronized
    def get_description(self):
        """Like Dulwich's `get_description`, but returns None if the file
        contains Git's default text "Unnamed repository[...]".
        """
        # Cache result to speed up repo_list.html template.
        # If description file mtime has changed, we should invalidate the cache.
        description_file = os.path.join(self.dulwich_repo._controldir, "description")
        try:
            description_mtime = os.stat(description_file).st_mtime
        except OSError:
            description_mtime = None

        return cached_call(
            key=(id(self), "get_description"),
            validator=description_mtime,
            producer=self._get_description,
        )

    def _get_description(self):
        description = self.dulwich_repo.get_description()
        if description:
            description = force_unicode(description)
            if not description.startswith("Unnamed repository;"):
                return force_unicode(description)

    @synchronized
    def get_commit(self, rev):
        """Get commit object identified by `rev` (SHA or branch or tag name)."""
        for prefix in ["refs/heads/", "refs/tags/", ""]:
            key = prefix + rev
            try:
                obj = self[encode_for_git(key)]
                if isinstance(obj, dulwich.objects.Tag):
                    obj = self[obj.object[1]]
                return obj
            except KeyError:
                pass
        raise KeyError(rev)

    @synchronized
    def get_default_branch(self):
        """Tries to guess the default repo branch name."""
        for candidate in ["master", "main", "trunk", "default", "gh-pages"]:
            try:
                self.get_commit(candidate)
                return candidate
            except InaccessibleRef:
                pass
        return None

    @synchronized
    def get_ref_names_ordered_by_last_commit(self, prefix, exclude=None):
        """Return a list of ref names that begin with `prefix`, ordered by the
        time they have been committed to last.
        """

        def get_commit_time(refname):
            try:
                obj = self[refs[refname]]
            except InaccessibleRef:
                # Default to 0, i.e. sorting refs that point at non-existant
                # objects last.
                return 0
            if isinstance(obj, dulwich.objects.Tag):
                return obj.tag_time
            return obj.commit_time

        refs = self.dulwich_repo.refs.as_dict(encode_for_git(prefix))
        if exclude:
            refs.pop(prefix + exclude, None)
        sorted_names = sorted(refs.keys(), key=get_commit_time, reverse=True)
        return [decode_from_git(ref) for ref in sorted_names]

    @synchronized
    def get_branch_names(self, exclude=None):
        """Return a list of branch names of this repo, ordered by the time they
        have been committed to last.
        """
        return self.get_ref_names_ordered_by_last_commit("refs/heads", exclude)

    @synchronized
    def get_tag_names(self):
        """Return a list of tag names of this repo, ordered by creation time."""
        return self.get_ref_names_ordered_by_last_commit("refs/tags")

    @synchronized
    def get_tag_and_branch_shas(self):
        """Return a list of SHAs of all tags and branches."""
        tag_shas = self.dulwich_repo.refs.as_dict(b"refs/tags/").values()
        branch_shas = self.dulwich_repo.refs.as_dict(b"refs/heads/").values()
        return set(tag_shas) | set(branch_shas)

    @synchronized
    def history(self, commit, path=None, max_commits=None, skip=0):
        """Return a list of all commits that affected `path`, starting at branch
        or commit `commit`. `skip` can be used for pagination, `max_commits`
        to limit the number of commits returned.

        Similar to `git log [branch/commit] [--skip skip] [-n max_commits]`.
        """
        # XXX The pure-Python/dulwich code is very slow compared to `git log`
        #     at the time of this writing (mid-2012).
        #     For instance, `git log .tx` in the Django root directory takes
        #     about 0.15s on my machine whereas the history() method needs 5s.
        #     Therefore we use `git log` here until dulwich gets faster.
        #     For the pure-Python implementation, see the 'purepy-hist' branch.

        cmd = ["git", "log", "--format=%H"]
        if skip:
            cmd.append("--skip=%d" % skip)
        if max_commits:
            cmd.append("--max-count=%d" % max_commits)
        cmd.append(decode_from_git(commit.id))
        if path:
            cmd.extend(["--", path])

        output = subprocess.check_output(cmd, cwd=os.path.abspath(self.path))
        sha1_sums = output.strip().split(b"\n")
        return [self[sha1] for sha1 in sha1_sums]

    @synchronized
    def blame(self, commit, path):
        """Return a 'git blame' list for the file at `path`: For each line in
        the file, the list contains the commit that last changed that line.
        """
        # XXX see comment in `.history()`
        cmd = ["git", "blame", "-ls", "--root", decode_from_git(commit.id), "--", path]
        output = subprocess.check_output(cmd, cwd=os.path.abspath(self.path))
        sha1_sums = [line[:40] for line in output.strip().split(b"\n") if line]
        return [
            None if self[sha1] is None else decode_from_git(self[sha1].id)
            for sha1 in sha1_sums
        ]

    @synchronized
    def get_blob_or_tree(self, commit, path):
        """Return the Git tree or blob object for `path` at `commit`."""
        try:
            (mode, oid) = tree_lookup_path(
                self.dulwich_repo.__getitem__, commit.tree, encode_for_git(path)
            )
        except NotTreeError:
            # Some part of the path was a file where a folder was expected.
            # Example: path="/path/to/foo.txt" but "to" is a file in "/path".
            raise KeyError
        return self[oid]

    @synchronized
    def listdir(self, commit, path):
        """Return a list of submodules, directories and files in given
        directory: Lists of (link name, target path) tuples.
        """
        submodules, dirs, files = [], [], []
        for entry_rel in self.get_blob_or_tree(commit, path).items():
            # entry_rel: Entry('foo.txt', ...)
            # entry_abs: Entry('spam/eggs/foo.txt', ...)
            entry_abs = entry_rel.in_path(encode_for_git(path))
            path_str = decode_from_git(entry_abs.path)
            item = (os.path.basename(path_str), path_str)
            if S_ISGITLINK(entry_abs.mode):
                submodules.append(item)
            elif stat.S_ISDIR(entry_abs.mode):
                dirs.append(item)
            else:
                files.append(item)

        def keyfunc(tpl):
            return tpl[0].lower()
        submodules.sort(key=keyfunc)
        files.sort(key=keyfunc)
        dirs.sort(key=keyfunc)

        if path:
            dirs.insert(0, ("..", parent_directory(path)))

        return {"submodules": submodules, "dirs": dirs, "files": files}

    @synchronized
    def commit_diff(self, commit):
        """Return the list of changes introduced by `commit`."""
        from klaus.utils import guess_is_binary

        if commit.parents:
            parent_tree = self[commit.parents[0]].tree
        else:
            parent_tree = None

        summary = {"nfiles": 0, "nadditions": 0, "ndeletions": 0}
        file_changes = []  # the changes in detail

        dulwich_changes = self.dulwich_repo.object_store.tree_changes(parent_tree, commit.tree)
        for (oldpath, newpath), (oldmode, newmode), (oldsha, newsha) in dulwich_changes:
            summary["nfiles"] += 1
            try:
                oldblob = self.dulwich_repo.object_store[oldsha] if oldsha else Blob.from_string(b"")
            except KeyError:
                # probably related to submodules; Dulwich will handle that.
                oldblob = Blob.from_string(b"")
            try:
                newblob = self.dulwich_repo.object_store[newsha] if newsha else Blob.from_string(b"")
            except KeyError:
                # probably related to submodules; Dulwich will handle that.
                newblob = Blob.from_string(b"")

            # Check for binary files -- can't show diffs for these
            if guess_is_binary(newblob) or guess_is_binary(oldblob):
                file_changes.append(
                    {
                        "is_binary": True,
                        "old_filename": oldpath or "/dev/null",
                        "new_filename": newpath or "/dev/null",
                        "chunks": None,
                    }
                )
                continue

            additions, deletions, chunks = render_diff(
                oldblob.splitlines(), newblob.splitlines()
            )
            change = {
                "is_binary": False,
                "old_filename": oldpath or "/dev/null",
                "new_filename": newpath or "/dev/null",
                "chunks": chunks,
                "additions": additions,
                "deletions": deletions,
            }
            summary["nadditions"] += additions
            summary["ndeletions"] += deletions
            file_changes.append(change)

        return summary, file_changes

    @synchronized
    def raw_commit_diff(self, commit):
        if commit.parents:
            parent_tree = self[commit.parents[0]].tree
        else:
            parent_tree = None
        bytesio = io.BytesIO()
        dulwich.patch.write_tree_diff(
            bytesio, self.dulwich_repo.object_store, parent_tree, commit.tree
        )
        return bytesio.getvalue()

    def freeze(self):
        return FrozenFancyRepo(self)


class FrozenFancyRepo(object):
    """A special version of FancyRepo that assumes the underlying Git
    repository does not change.  Used for performance optimizations.
    """

    def __init__(self, repo):
        self.__repo = repo
        self.__last_updated_at = NOT_SET

    def __setattr__(self, name, value):
        if not name.startswith("_FrozenFancyRepo__"):
            raise TypeError("Can't set %s attribute on FrozenFancyRepo" % name)
        super(FrozenFancyRepo, self).__setattr__(name, value)

    def __getattr__(self, name):
        return getattr(self.__repo, name)

    def fast_get_last_updated_at(self):
        if self.__last_updated_at is NOT_SET:
            self.__last_updated_at = self.__repo.get_last_updated_at()
        return self.__last_updated_at


class InvalidRepo:
    """Represent an invalid repository and store pertinent data."""

    def __init__(self, path, namespace):
        self.path = path
        self.namespace = namespace

    @property
    def name(self):
        return repo_human_name(self.path)

    @property
    def namespaced_name(self):
        if self.namespace:
            return "~{}/{}".format(self.namespace, self.name)
        else:
            return self.name
