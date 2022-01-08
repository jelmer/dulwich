# clone.py
# Copyright (C) 2021 Jelmer Vernooij <jelmer@samba.org>
#
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# You should have received a copy of the licenses; if not, see
# <http://www.gnu.org/licenses/> for a copy of the GNU General Public License
# and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache
# License, Version 2.0.
#

"""Repository clone handling."""

import os
import shutil
from typing import TYPE_CHECKING, Callable, Tuple

from dulwich.objects import (
    Tag,
)
from dulwich.refs import (
    LOCAL_BRANCH_PREFIX,
    LOCAL_TAG_PREFIX,
)

if TYPE_CHECKING:
    from dulwich.repo import Repo


def do_clone(
    source_path,
    target_path,
    clone_refs: Callable[["Repo", bytes], Tuple[bytes, bytes]] = None,
    mkdir=True,
    bare=False,
    origin=b"origin",
    checkout=None,
    errstream=None,
    branch=None,
):
    """Clone a repository.

    Args:
      source_path: Source repository path
      target_path: Target repository path
      clone_refs: Callback to handle setting up cloned remote refs in
        the target repo
      mkdir: Create the target directory
      bare: Whether to create a bare repository
      checkout: Whether or not to check-out HEAD after cloning
      origin: Base name for refs in target repository
        cloned from this repository
      branch: Optional branch or tag to be used as HEAD in the new repository
        instead of the source repository's HEAD.
    Returns: Created repository as `Repo`
    """
    from dulwich.repo import Repo

    if not clone_refs:
        raise ValueError("clone_refs callback is required")

    if mkdir:
        os.mkdir(target_path)

    try:
        target = None
        if not bare:
            target = Repo.init(target_path)
            if checkout is None:
                checkout = True
        else:
            if checkout:
                raise ValueError("checkout and bare are incompatible")
            target = Repo.init_bare(target_path)

        target_config = target.get_config()
        target_config.set((b"remote", origin), b"url", source_path)
        target_config.set(
            (b"remote", origin),
            b"fetch",
            b"+refs/heads/*:refs/remotes/" + origin + b"/*",
        )
        target_config.write_to_path()

        ref_message = b"clone: from " + source_path
        origin_head, origin_sha = clone_refs(target, ref_message)
        if origin_sha and not origin_head:
            # set detached HEAD
            target.refs[b"HEAD"] = origin_sha

        _set_origin_head(target, origin, origin_head)
        head_ref = _set_default_branch(
            target, origin, origin_head, branch, ref_message
        )

        # Update target head
        if head_ref:
            head = _set_head(target, head_ref, ref_message)
        else:
            head = None

        if checkout and head is not None:
            if errstream:
                errstream.write(b"Checking out " + head + b"\n")
            target.reset_index()
    except BaseException:
        if target is not None:
            target.close()
        if mkdir:
            shutil.rmtree(target_path)
        raise

    return target


def _set_origin_head(r, origin, origin_head):
    # set refs/remotes/origin/HEAD
    origin_base = b"refs/remotes/" + origin + b"/"
    if origin_head and origin_head.startswith(LOCAL_BRANCH_PREFIX):
        origin_ref = origin_base + b"HEAD"
        target_ref = origin_base + origin_head[len(LOCAL_BRANCH_PREFIX) :]
        if target_ref in r.refs:
            r.refs.set_symbolic_ref(origin_ref, target_ref)


def _set_default_branch(r, origin, origin_head, branch, ref_message):
    origin_base = b"refs/remotes/" + origin + b"/"
    if branch:
        origin_ref = origin_base + branch
        if origin_ref in r.refs:
            local_ref = LOCAL_BRANCH_PREFIX + branch
            r.refs.add_if_new(
                local_ref, r.refs[origin_ref], ref_message
            )
            head_ref = local_ref
        elif LOCAL_TAG_PREFIX + branch in r.refs:
            head_ref = LOCAL_TAG_PREFIX + branch
        else:
            raise ValueError(
                "%s is not a valid branch or tag" % os.fsencode(branch)
            )
    elif origin_head:
        head_ref = origin_head
        if origin_head.startswith(LOCAL_BRANCH_PREFIX):
            origin_ref = origin_base + origin_head[len(LOCAL_BRANCH_PREFIX) :]
        else:
            origin_ref = origin_head
        try:
            r.refs.add_if_new(
                head_ref, r.refs[origin_ref], ref_message
            )
        except KeyError:
            pass
    return head_ref


def _set_head(r, head_ref, ref_message):
    if head_ref.startswith(LOCAL_TAG_PREFIX):
        # detach HEAD at specified tag
        head = r.refs[head_ref]
        if isinstance(head, Tag):
            _cls, obj = head.object
            head = obj.get_object(obj).id
        del r.refs[b"HEAD"]
        r.refs.set_if_equals(
            b"HEAD", None, head, message=ref_message
        )
    else:
        # set HEAD to specific branch
        try:
            head = r.refs[head_ref]
            r.refs.set_symbolic_ref(b"HEAD", head_ref)
            r.refs.set_if_equals(
                b"HEAD", None, head, message=ref_message
            )
        except KeyError:
            head = None
    return head
