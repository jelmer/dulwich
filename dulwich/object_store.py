# object_store.py -- Object store for git objects 
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

from objects import (
        ShaFile,
        )
import os
from pack import (
        iter_sha1, 
        load_packs, 
        write_pack_index_v2,
        PackData, 
        )
PACKDIR = 'pack'

class ObjectStore(object):

    def __init__(self, path):
        self.path = path
        self._packs = None

    def pack_dir(self):
        return os.path.join(self.path, PACKDIR)

    def __contains__(self, sha):
        # TODO: This can be more efficient
        try:
            self[sha]
            return True
        except KeyError:
            return False

    @property
    def packs(self):
        """List with pack objects."""
        if self._packs is None:
            self._packs = list(load_packs(self.pack_dir()))
        return self._packs

    def _get_shafile(self, sha):
        dir = sha[:2]
        file = sha[2:]
        # Check from object dir
        path = os.path.join(self.path, dir, file)
        if os.path.exists(path):
          return ShaFile.from_file(path)
        return None

    def get_raw(self, sha):
        """Obtain the raw text for an object.
        
        :param sha: Sha for the object.
        :return: tuple with object type and object contents.
        """
        for pack in self.packs:
            if sha in pack:
                return pack.get_raw(sha, self.get_raw)
        # FIXME: Are pack deltas ever against on-disk shafiles ?
        ret = self._get_shafile(sha)
        if ret is not None:
            return ret.as_raw_string()
        raise KeyError(sha)

    def __getitem__(self, sha):
        assert len(sha) == 40, "Incorrect length sha: %s" % str(sha)
        ret = self._get_shafile(sha)
        if ret is not None:
            return ret
        # Check from packs
        type, uncomp = self.get_raw(sha)
        return ShaFile.from_raw_string(type, uncomp)

    def move_in_pack(self, path):
        """Move a specific file containing a pack into the pack directory.

        :note: The file should be on the same file system as the 
            packs directory.

        :param path: Path to the pack file.
        """
        p = PackData(path)
        entries = p.sorted_entries(self.get_raw)
        basename = os.path.join(self.pack_dir(), 
            "pack-%s" % iter_sha1(entry[0] for entry in entries))
        write_pack_index_v2(basename+".idx", entries, p.calculate_checksum())
        os.rename(path, basename + ".pack")

    def add_pack(self):
        """Add a new pack to this object store. 

        :return: Fileobject to write to and a commit function to 
            call when the pack is finished.
        """
        fd, path = tempfile.mkstemp(dir=self.pack_dir(), suffix=".pack")
        f = os.fdopen(fd, 'w')
        def commit():
            if os.path.getsize(path) > 0:
                self.move_in_pack(path)
        return f, commit
