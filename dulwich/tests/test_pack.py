# test_pack.py -- Tests for the handling of git packs.
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License, or (at your option) any later version of the license.
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

"""Tests for Dulwich packs."""


from cStringIO import StringIO
import os
import shutil
import tempfile
import zlib

from dulwich.errors import (
    ChecksumMismatch,
    )
from dulwich.file import (
    GitFile,
    )
from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Blob,
    hex_to_sha,
    sha_to_hex,
    Commit,
    Tree,
    Blob,
    )
from dulwich.pack import (
    OFS_DELTA,
    REF_DELTA,
    DELTA_TYPES,
    MemoryPackIndex,
    Pack,
    obj_sha,
    PackData,
    ThinPackData,
    apply_delta,
    create_delta,
    deltify_pack_objects,
    load_pack_index,
    read_zlib_chunks,
    write_pack_header,
    write_pack_index_v1,
    write_pack_index_v2,
    SHA1Writer,
    write_pack_object,
    write_pack,
    DeltaChainIterator,
    )
from dulwich.tests import (
    TestCase,
    )
from utils import (
    make_object,
    )

pack1_sha = 'bc63ddad95e7321ee734ea11a7a62d314e0d7481'

a_sha = '6f670c0fb53f9463760b7295fbb814e965fb20c8'
tree_sha = 'b2a2766a2879c209ab1176e7e778b81ae422eeaa'
commit_sha = 'f18faa16531ac570a3fdc8c7ca16682548dafd12'


class PackTests(TestCase):
    """Base class for testing packs"""

    def setUp(self):
        super(PackTests, self).setUp()
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)
        super(PackTests, self).tearDown()

    datadir = os.path.abspath(os.path.join(os.path.dirname(__file__),
        'data/packs'))

    def get_pack_index(self, sha):
        """Returns a PackIndex from the datadir with the given sha"""
        return load_pack_index(os.path.join(self.datadir, 'pack-%s.idx' % sha))

    def get_pack_data(self, sha):
        """Returns a PackData object from the datadir with the given sha"""
        return PackData(os.path.join(self.datadir, 'pack-%s.pack' % sha))

    def get_pack(self, sha):
        return Pack(os.path.join(self.datadir, 'pack-%s' % sha))

    def assertSucceeds(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except ChecksumMismatch, e:
            self.fail(e)


class PackIndexTests(PackTests):
    """Class that tests the index of packfiles"""

    def test_object_index(self):
        """Tests that the correct object offset is returned from the index."""
        p = self.get_pack_index(pack1_sha)
        self.assertRaises(KeyError, p.object_index, pack1_sha)
        self.assertEqual(p.object_index(a_sha), 178)
        self.assertEqual(p.object_index(tree_sha), 138)
        self.assertEqual(p.object_index(commit_sha), 12)

    def test_index_len(self):
        p = self.get_pack_index(pack1_sha)
        self.assertEquals(3, len(p))

    def test_get_stored_checksum(self):
        p = self.get_pack_index(pack1_sha)
        self.assertEquals('f2848e2ad16f329ae1c92e3b95e91888daa5bd01',
                          sha_to_hex(p.get_stored_checksum()))
        self.assertEquals('721980e866af9a5f93ad674144e1459b8ba3e7b7',
                          sha_to_hex(p.get_pack_checksum()))

    def test_index_check(self):
        p = self.get_pack_index(pack1_sha)
        self.assertSucceeds(p.check)

    def test_iterentries(self):
        p = self.get_pack_index(pack1_sha)
        entries = [(sha_to_hex(s), o, c) for s, o, c in p.iterentries()]
        self.assertEquals([
          ('6f670c0fb53f9463760b7295fbb814e965fb20c8', 178, None),
          ('b2a2766a2879c209ab1176e7e778b81ae422eeaa', 138, None),
          ('f18faa16531ac570a3fdc8c7ca16682548dafd12', 12, None)
          ], entries)

    def test_iter(self):
        p = self.get_pack_index(pack1_sha)
        self.assertEquals(set([tree_sha, commit_sha, a_sha]), set(p))


class TestPackDeltas(TestCase):

    test_string1 = 'The answer was flailing in the wind'
    test_string2 = 'The answer was falling down the pipe'
    test_string3 = 'zzzzz'

    test_string_empty = ''
    test_string_big = 'Z' * 8192

    def _test_roundtrip(self, base, target):
        self.assertEquals(target,
                          ''.join(apply_delta(base, create_delta(base, target))))

    def test_nochange(self):
        self._test_roundtrip(self.test_string1, self.test_string1)

    def test_change(self):
        self._test_roundtrip(self.test_string1, self.test_string2)

    def test_rewrite(self):
        self._test_roundtrip(self.test_string1, self.test_string3)

    def test_overflow(self):
        self._test_roundtrip(self.test_string_empty, self.test_string_big)


class TestPackData(PackTests):
    """Tests getting the data from the packfile."""

    def test_create_pack(self):
        p = self.get_pack_data(pack1_sha)

    def test_from_file(self):
        path = os.path.join(self.datadir, 'pack-%s.pack' % pack1_sha)
        PackData.from_file(open(path), os.path.getsize(path))

    # TODO: more ThinPackData tests.
    def test_thin_from_file(self):
        test_sha = '1' * 40

        def resolve(sha):
            self.assertEqual(test_sha, sha)
            return 3, 'data'

        path = os.path.join(self.datadir, 'pack-%s.pack' % pack1_sha)
        data = ThinPackData.from_file(resolve, open(path),
                                      os.path.getsize(path))
        idx = self.get_pack_index(pack1_sha)
        Pack.from_objects(data, idx)
        self.assertEqual((None, 3, 'data'), data.get_ref(test_sha))

    def test_pack_len(self):
        p = self.get_pack_data(pack1_sha)
        self.assertEquals(3, len(p))

    def test_index_check(self):
        p = self.get_pack_data(pack1_sha)
        self.assertSucceeds(p.check)

    def test_iterobjects(self):
        p = self.get_pack_data(pack1_sha)
        commit_data = ('tree b2a2766a2879c209ab1176e7e778b81ae422eeaa\n'
                       'author James Westby <jw+debian@jameswestby.net> '
                       '1174945067 +0100\n'
                       'committer James Westby <jw+debian@jameswestby.net> '
                       '1174945067 +0100\n'
                       '\n'
                       'Test commit\n')
        blob_sha = '6f670c0fb53f9463760b7295fbb814e965fb20c8'
        tree_data = '100644 a\0%s' % hex_to_sha(blob_sha)
        actual = []
        for offset, type_num, chunks, crc32 in p.iterobjects():
            actual.append((offset, type_num, ''.join(chunks), crc32))
        self.assertEquals([
          (12, 1, commit_data, 3775879613L),
          (138, 2, tree_data, 912998690L),
          (178, 3, 'test 1\n', 1373561701L)
          ], actual)

    def test_iterentries(self):
        p = self.get_pack_data(pack1_sha)
        entries = set((sha_to_hex(s), o, c) for s, o, c in p.iterentries())
        self.assertEquals(set([
          ('6f670c0fb53f9463760b7295fbb814e965fb20c8', 178, 1373561701L),
          ('b2a2766a2879c209ab1176e7e778b81ae422eeaa', 138, 912998690L),
          ('f18faa16531ac570a3fdc8c7ca16682548dafd12', 12, 3775879613L),
          ]), entries)

    def test_create_index_v1(self):
        p = self.get_pack_data(pack1_sha)
        filename = os.path.join(self.tempdir, 'v1test.idx')
        p.create_index_v1(filename)
        idx1 = load_pack_index(filename)
        idx2 = self.get_pack_index(pack1_sha)
        self.assertEquals(idx1, idx2)

    def test_create_index_v2(self):
        p = self.get_pack_data(pack1_sha)
        filename = os.path.join(self.tempdir, 'v2test.idx')
        p.create_index_v2(filename)
        idx1 = load_pack_index(filename)
        idx2 = self.get_pack_index(pack1_sha)
        self.assertEquals(idx1, idx2)


class TestPack(PackTests):

    def test_len(self):
        p = self.get_pack(pack1_sha)
        self.assertEquals(3, len(p))

    def test_contains(self):
        p = self.get_pack(pack1_sha)
        self.assertTrue(tree_sha in p)

    def test_get(self):
        p = self.get_pack(pack1_sha)
        self.assertEquals(type(p[tree_sha]), Tree)

    def test_iter(self):
        p = self.get_pack(pack1_sha)
        self.assertEquals(set([tree_sha, commit_sha, a_sha]), set(p))

    def test_iterobjects(self):
        p = self.get_pack(pack1_sha)
        expected = set([p[s] for s in [commit_sha, tree_sha, a_sha]])
        self.assertEquals(expected, set(list(p.iterobjects())))

    def test_pack_tuples(self):
        p = self.get_pack(pack1_sha)
        tuples = p.pack_tuples()
        expected = set([(p[s], None) for s in [commit_sha, tree_sha, a_sha]])
        self.assertEquals(expected, set(list(tuples)))
        self.assertEquals(expected, set(list(tuples)))
        self.assertEquals(3, len(tuples))

    def test_get_object_at(self):
        """Tests random access for non-delta objects"""
        p = self.get_pack(pack1_sha)
        obj = p[a_sha]
        self.assertEqual(obj.type_name, 'blob')
        self.assertEqual(obj.sha().hexdigest(), a_sha)
        obj = p[tree_sha]
        self.assertEqual(obj.type_name, 'tree')
        self.assertEqual(obj.sha().hexdigest(), tree_sha)
        obj = p[commit_sha]
        self.assertEqual(obj.type_name, 'commit')
        self.assertEqual(obj.sha().hexdigest(), commit_sha)

    def test_copy(self):
        origpack = self.get_pack(pack1_sha)

        try:
            self.assertSucceeds(origpack.index.check)
            basename = os.path.join(self.tempdir, 'Elch')
            write_pack(basename, origpack.pack_tuples())
            newpack = Pack(basename)

            try:
                self.assertEquals(origpack, newpack)
                self.assertSucceeds(newpack.index.check)
                self.assertEquals(origpack.name(), newpack.name())
                self.assertEquals(origpack.index.get_pack_checksum(),
                                  newpack.index.get_pack_checksum())

                wrong_version = origpack.index.version != newpack.index.version
                orig_checksum = origpack.index.get_stored_checksum()
                new_checksum = newpack.index.get_stored_checksum()
                self.assertTrue(wrong_version or orig_checksum == new_checksum)
            finally:
                newpack.close()
        finally:
            origpack.close()


    def test_commit_obj(self):
        p = self.get_pack(pack1_sha)
        commit = p[commit_sha]
        self.assertEquals('James Westby <jw+debian@jameswestby.net>',
                          commit.author)
        self.assertEquals([], commit.parents)

    def _copy_pack(self, origpack):
        basename = os.path.join(self.tempdir, 'somepack')
        write_pack(basename, origpack.pack_tuples())
        return Pack(basename)

    def test_keep_no_message(self):
        p = self.get_pack(pack1_sha)
        p = self._copy_pack(p)

        keepfile_name = p.keep()
        # file should exist
        self.assertTrue(os.path.exists(keepfile_name))

        f = open(keepfile_name, 'r')
        try:
            buf = f.read()
            self.assertEqual('', buf)
        finally:
            f.close()

    def test_keep_message(self):
        p = self.get_pack(pack1_sha)
        p = self._copy_pack(p)

        msg = 'some message'
        keepfile_name = p.keep(msg)

        # file should exist
        self.assertTrue(os.path.exists(keepfile_name))

        # and contain the right message, with a linefeed
        f = open(keepfile_name, 'r')
        try:
            buf = f.read()
            self.assertEqual(msg + '\n', buf)
        finally:
            f.close()

    def test_name(self):
        p = self.get_pack(pack1_sha)
        self.assertEquals(pack1_sha, p.name())


class WritePackHeaderTests(TestCase):

    def test_simple(self):
        f = StringIO()
        write_pack_header(f, 42)
        self.assertEquals('PACK\x00\x00\x00\x02\x00\x00\x00*',
                f.getvalue())


pack_checksum = hex_to_sha('721980e866af9a5f93ad674144e1459b8ba3e7b7')


class BaseTestPackIndexWriting(object):

    def assertSucceeds(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except ChecksumMismatch, e:
            self.fail(e)

    def index(self, filename, entries, pack_checksum):
        raise NotImplementedError(self.index)

    def test_empty(self):
        idx = self.index('empty.idx', [], pack_checksum)
        self.assertEquals(idx.get_pack_checksum(), pack_checksum)
        self.assertEquals(0, len(idx))

    def test_single(self):
        entry_sha = hex_to_sha('6f670c0fb53f9463760b7295fbb814e965fb20c8')
        my_entries = [(entry_sha, 178, 42)]
        idx = self.index('single.idx', my_entries, pack_checksum)
        self.assertEquals(idx.get_pack_checksum(), pack_checksum)
        self.assertEquals(1, len(idx))
        actual_entries = list(idx.iterentries())
        self.assertEquals(len(my_entries), len(actual_entries))
        for mine, actual in zip(my_entries, actual_entries):
            my_sha, my_offset, my_crc = mine
            actual_sha, actual_offset, actual_crc = actual
            self.assertEquals(my_sha, actual_sha)
            self.assertEquals(my_offset, actual_offset)
            if self._has_crc32_checksum:
                self.assertEquals(my_crc, actual_crc)
            else:
                self.assertTrue(actual_crc is None)


class BaseTestFilePackIndexWriting(BaseTestPackIndexWriting):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def index(self, filename, entries, pack_checksum):
        path = os.path.join(self.tempdir, filename)
        self.writeIndex(path, entries, pack_checksum)
        idx = load_pack_index(path)
        self.assertSucceeds(idx.check)
        self.assertEquals(idx.version, self._expected_version)
        return idx

    def writeIndex(self, filename, entries, pack_checksum):
        # FIXME: Write to StringIO instead rather than hitting disk ?
        f = GitFile(filename, "wb")
        try:
            self._write_fn(f, entries, pack_checksum)
        finally:
            f.close()


class TestMemoryIndexWriting(TestCase, BaseTestPackIndexWriting):

    def setUp(self):
        TestCase.setUp(self)
        self._has_crc32_checksum = True

    def index(self, filename, entries, pack_checksum):
        return MemoryPackIndex(entries, pack_checksum)

    def tearDown(self):
        TestCase.tearDown(self)


class TestPackIndexWritingv1(TestCase, BaseTestFilePackIndexWriting):

    def setUp(self):
        TestCase.setUp(self)
        BaseTestFilePackIndexWriting.setUp(self)
        self._has_crc32_checksum = False
        self._expected_version = 1
        self._write_fn = write_pack_index_v1

    def tearDown(self):
        TestCase.tearDown(self)
        BaseTestFilePackIndexWriting.tearDown(self)


class TestPackIndexWritingv2(TestCase, BaseTestFilePackIndexWriting):

    def setUp(self):
        TestCase.setUp(self)
        BaseTestFilePackIndexWriting.setUp(self)
        self._has_crc32_checksum = True
        self._expected_version = 2
        self._write_fn = write_pack_index_v2

    def tearDown(self):
        TestCase.tearDown(self)
        BaseTestFilePackIndexWriting.tearDown(self)


class ReadZlibTests(TestCase):

    decomp = (
      'tree 4ada885c9196b6b6fa08744b5862bf92896fc002\n'
      'parent None\n'
      'author Jelmer Vernooij <jelmer@samba.org> 1228980214 +0000\n'
      'committer Jelmer Vernooij <jelmer@samba.org> 1228980214 +0000\n'
      '\n'
      "Provide replacement for mmap()'s offset argument.")
    comp = zlib.compress(decomp)
    extra = 'nextobject'

    def setUp(self):
        super(ReadZlibTests, self).setUp()
        self.read = StringIO(self.comp + self.extra).read

    def test_decompress_size(self):
        good_decomp_len = len(self.decomp)
        self.assertRaises(ValueError, read_zlib_chunks, self.read, -1)
        self.assertRaises(zlib.error, read_zlib_chunks, self.read,
                          good_decomp_len - 1)
        self.assertRaises(zlib.error, read_zlib_chunks, self.read,
                          good_decomp_len + 1)

    def test_decompress_truncated(self):
        read = StringIO(self.comp[:10]).read
        self.assertRaises(zlib.error, read_zlib_chunks, read, len(self.decomp))

        read = StringIO(self.comp).read
        self.assertRaises(zlib.error, read_zlib_chunks, read, len(self.decomp))

    def test_decompress_empty(self):
        comp = zlib.compress('')
        read = StringIO(comp + self.extra).read
        decomp, comp_len, crc32, unused_data = read_zlib_chunks(read, 0,
                                                                crc32=0)
        self.assertEqual('', ''.join(decomp))
        self.assertEqual(len(comp), comp_len)
        self.assertNotEquals('', unused_data)
        self.assertEquals(self.extra, unused_data + read())

    def test_decompress_no_crc32(self):
        _, _, crc32, _ = read_zlib_chunks(
          self.read, len(self.decomp), buffer_size=4096)
        self.assertEquals(None, crc32)

    def _do_decompress_test(self, buffer_size):
        decomp, comp_len, crc32, unused_data = read_zlib_chunks(
          self.read, len(self.decomp), buffer_size=buffer_size, crc32=0)
        self.assertEquals(self.decomp, ''.join(decomp))
        self.assertEquals(len(self.comp), comp_len)
        self.assertEquals(crc32, zlib.crc32(self.comp))
        self.assertNotEquals('', unused_data)
        self.assertEquals(self.extra, unused_data + self.read())

    def test_simple_decompress(self):
        self._do_decompress_test(4096)

    # These buffer sizes are not intended to be realistic, but rather simulate
    # larger buffer sizes that may end at various places.
    def test_decompress_buffer_size_1(self):
        self._do_decompress_test(1)

    def test_decompress_buffer_size_2(self):
        self._do_decompress_test(2)

    def test_decompress_buffer_size_3(self):
        self._do_decompress_test(3)

    def test_decompress_buffer_size_4(self):
        self._do_decompress_test(4)


class DeltifyTests(TestCase):

    def test_empty(self):
        self.assertEquals([], list(deltify_pack_objects([])))

    def test_single(self):
        b = Blob.from_string("foo")
        self.assertEquals(
            [(b.type_num, b.sha().digest(), None, b.as_raw_string())],
            list(deltify_pack_objects([(b, "")])))

    def test_simple_delta(self):
        b1 = Blob.from_string("a" * 101)
        b2 = Blob.from_string("a" * 100)
        delta = create_delta(b1.as_raw_string(), b2.as_raw_string())
        self.assertEquals([
            (b1.type_num, b1.sha().digest(), None, b1.as_raw_string()),
            (b2.type_num, b2.sha().digest(), b1.sha().digest(), delta)
            ],
            list(deltify_pack_objects([(b1, ""), (b2, "")])))


class TestPackIterator(DeltaChainIterator):

    _compute_crc32 = True

    def __init__(self, pack_data):
        super(TestPackIterator, self).__init__(pack_data)
        self._unpacked = set()

    def _result(self, offset, type_num, chunks, sha, crc32):
        return offset, type_num, ''.join(chunks), sha, crc32

    def _resolve_object(self, offset, base_type_num, base_chunks):
        assert offset not in self._unpacked, ('Attempted to re-inflate offset '
                                              '%i' % offset)
        self._unpacked.add(offset)
        return super(TestPackIterator, self)._resolve_object(
          offset, base_type_num, base_chunks)



class DeltaChainIteratorTests(TestCase):

    def setUp(self):
        self.store = MemoryObjectStore()
        self.fetched = set()

    def store_blobs(self, blobs_data):
        blobs = []
        for data in blobs_data:
            blob = make_object(Blob, data=data)
            blobs.append(blob)
            self.store.add_object(blob)
        return blobs

    def write_pack_data(self, objects_spec):
        """Write test pack data from a concise spec.

        :param objects_spec: A list of (type_num, obj). For non-delta types,
            obj is the string of that object's data.
            For delta types, obj is a tuple of (base_index, data), where
            base_index is the index in objects_spec of the base for that delta,
            and data is the full, non-deltified data for that object.
            (Offsets/refs and deltas are computed within this function.)
        :return: A tuple of (f, entries), where f is a file-like object
            pointed at the beginning of a pack with the requested data, and
            entries is a list of tuples of (offset, type num, data, sha, CRC32)
            These tuples match the result format from TestPackIterator, and are
            returned in the order specified by objects_spec.
        """
        f = StringIO()
        sf = SHA1Writer(f)
        num_objects = len(objects_spec)
        write_pack_header(sf, num_objects)

        full_objects = {}
        offsets = {}
        crc32s = {}

        while len(full_objects) < num_objects:
            for i, (type_num, data) in enumerate(objects_spec):
                if type_num not in DELTA_TYPES:
                    full_objects[i] = (type_num, data,
                                       obj_sha(type_num, [data]))
                    continue
                base, data = data
                if isinstance(base, int):
                    if base not in full_objects:
                        continue
                    base_type_num, _, _ = full_objects[base]
                else:
                    base_type_num, _ = self.store.get_raw(base)
                full_objects[i] = (base_type_num, data,
                                   obj_sha(base_type_num, [data]))

        for i, (type_num, obj) in enumerate(objects_spec):
            offset = f.tell()
            if type_num == OFS_DELTA:
                base_index, data = obj
                base = offset - offsets[base_index]
                _, base_data, _ = full_objects[base_index]
                obj = (base, create_delta(base_data, data))
            elif type_num == REF_DELTA:
                base_ref, data = obj
                if isinstance(base_ref, int):
                    _, base_data, base = full_objects[base_ref]
                else:
                    base_type_num, base_data = self.store.get_raw(base_ref)
                    base = obj_sha(base_type_num, base_data)
                obj = (base, create_delta(base_data, data))

            crc32 = write_pack_object(sf, type_num, obj)
            offsets[i] = offset
            crc32s[i] = crc32

        expected = []
        for i in xrange(num_objects):
            type_num, data, sha = full_objects[i]
            expected.append((offsets[i], type_num, data, sha, crc32s[i]))

        sf.write_sha()
        f.seek(0)
        return f, expected

    def get_raw_no_repeat(self, bin_sha):
        """Wrapper around store.get_raw that doesn't allow repeat lookups."""
        hex_sha = sha_to_hex(bin_sha)
        self.assertFalse(hex_sha in self.fetched,
                         'Attempted to re-fetch object %s' % hex_sha)
        self.fetched.add(hex_sha)
        return self.store.get_raw(hex_sha)

    def make_pack_iter(self, f, thin=None):
        if thin is None:
            thin = bool(list(self.store))
        if thin:
            data = ThinPackData(self.get_raw_no_repeat, 'test.pack', file=f)
        else:
            data = PackData('test.pack', file=f)
        return TestPackIterator.for_pack_data(data)

    def assertEntriesMatch(self, expected_indexes, entries, pack_iter):
        expected = [entries[i] for i in expected_indexes]
        self.assertEqual(expected, list(pack_iter._walk_all_chains()))

    def test_no_deltas(self):
        f, entries = self.write_pack_data([
          (Commit.type_num, 'commit'),
          (Blob.type_num, 'blob'),
          (Tree.type_num, 'tree'),
          ])
        self.assertEntriesMatch([0, 1, 2], entries, self.make_pack_iter(f))

    def test_ofs_deltas(self):
        f, entries = self.write_pack_data([
          (Blob.type_num, 'blob'),
          (OFS_DELTA, (0, 'blob1')),
          (OFS_DELTA, (0, 'blob2')),
          ])
        self.assertEntriesMatch([0, 1, 2], entries, self.make_pack_iter(f))

    def test_ofs_deltas_chain(self):
        f, entries = self.write_pack_data([
          (Blob.type_num, 'blob'),
          (OFS_DELTA, (0, 'blob1')),
          (OFS_DELTA, (1, 'blob2')),
          ])
        self.assertEntriesMatch([0, 1, 2], entries, self.make_pack_iter(f))

    def test_ref_deltas(self):
        f, entries = self.write_pack_data([
          (REF_DELTA, (1, 'blob1')),
          (Blob.type_num, ('blob')),
          (REF_DELTA, (1, 'blob2')),
          ])
        self.assertEntriesMatch([1, 0, 2], entries, self.make_pack_iter(f))

    def test_ref_deltas_chain(self):
        f, entries = self.write_pack_data([
          (REF_DELTA, (2, 'blob1')),
          (Blob.type_num, ('blob')),
          (REF_DELTA, (1, 'blob2')),
          ])
        self.assertEntriesMatch([1, 2, 0], entries, self.make_pack_iter(f))

    def test_ofs_and_ref_deltas(self):
        # Deltas pending on this offset are popped before deltas depending on
        # this ref.
        f, entries = self.write_pack_data([
          (REF_DELTA, (1, 'blob1')),
          (Blob.type_num, ('blob')),
          (OFS_DELTA, (1, 'blob2')),
          ])
        self.assertEntriesMatch([1, 2, 0], entries, self.make_pack_iter(f))

    def test_mixed_chain(self):
        f, entries = self.write_pack_data([
          (Blob.type_num, 'blob'),
          (REF_DELTA, (2, 'blob2')),
          (OFS_DELTA, (0, 'blob1')),
          (OFS_DELTA, (1, 'blob3')),
          (OFS_DELTA, (0, 'bob')),
          ])
        self.assertEntriesMatch([0, 2, 1, 3, 4], entries,
                                self.make_pack_iter(f))

    def test_long_chain(self):
        n = 100
        objects_spec = [(Blob.type_num, 'blob')]
        for i in xrange(n):
            objects_spec.append((OFS_DELTA, (i, 'blob%i' % i)))
        f, entries = self.write_pack_data(objects_spec)
        self.assertEntriesMatch(xrange(n + 1), entries, self.make_pack_iter(f))

    def test_branchy_chain(self):
        n = 100
        objects_spec = [(Blob.type_num, 'blob')]
        for i in xrange(n):
            objects_spec.append((OFS_DELTA, (0, 'blob%i' % i)))
        f, entries = self.write_pack_data(objects_spec)
        self.assertEntriesMatch(xrange(n + 1), entries, self.make_pack_iter(f))

    def test_ext_ref(self):
        blob, = self.store_blobs(['blob'])
        f, entries = self.write_pack_data([(REF_DELTA, (blob.id, 'blob1'))])
        pack_iter = self.make_pack_iter(f)
        self.assertEntriesMatch([0], entries, pack_iter)
        self.assertEqual([hex_to_sha(blob.id)], pack_iter.ext_refs())

    def test_ext_ref_chain(self):
        blob, = self.store_blobs(['blob'])
        f, entries = self.write_pack_data([
          (REF_DELTA, (1, 'blob2')),
          (REF_DELTA, (blob.id, 'blob1')),
          ])
        pack_iter = self.make_pack_iter(f)
        self.assertEntriesMatch([1, 0], entries, pack_iter)
        self.assertEqual([hex_to_sha(blob.id)], pack_iter.ext_refs())

    def test_ext_ref_multiple_times(self):
        blob, = self.store_blobs(['blob'])
        f, entries = self.write_pack_data([
          (REF_DELTA, (blob.id, 'blob1')),
          (REF_DELTA, (blob.id, 'blob2')),
          ])
        pack_iter = self.make_pack_iter(f)
        self.assertEntriesMatch([0, 1], entries, pack_iter)
        self.assertEqual([hex_to_sha(blob.id)], pack_iter.ext_refs())

    def test_multiple_ext_refs(self):
        b1, b2 = self.store_blobs(['foo', 'bar'])
        f, entries = self.write_pack_data([
          (REF_DELTA, (b1.id, 'foo1')),
          (REF_DELTA, (b2.id, 'bar2')),
          ])
        pack_iter = self.make_pack_iter(f)
        self.assertEntriesMatch([0, 1], entries, pack_iter)
        self.assertEqual([hex_to_sha(b1.id), hex_to_sha(b2.id)],
                         pack_iter.ext_refs())

    def test_bad_ext_ref_non_thin_pack(self):
        blob, = self.store_blobs(['blob'])
        f, entries = self.write_pack_data([(REF_DELTA, (blob.id, 'blob1'))])
        pack_iter = self.make_pack_iter(f, thin=False)
        try:
            list(pack_iter._walk_all_chains())
            self.fail()
        except KeyError, e:
            self.assertEqual(([blob.id],), e.args)

    def test_bad_ext_ref_thin_pack(self):
        b1, b2, b3 = self.store_blobs(['foo', 'bar', 'baz'])
        f, entries = self.write_pack_data([
          (REF_DELTA, (1, 'foo99')),
          (REF_DELTA, (b1.id, 'foo1')),
          (REF_DELTA, (b2.id, 'bar2')),
          (REF_DELTA, (b3.id, 'baz3')),
          ])
        del self.store[b2.id]
        del self.store[b3.id]
        pack_iter = self.make_pack_iter(f)
        try:
            list(pack_iter._walk_all_chains())
            self.fail()
        except KeyError, e:
            self.assertEqual((sorted([b2.id, b3.id]),), e.args)
