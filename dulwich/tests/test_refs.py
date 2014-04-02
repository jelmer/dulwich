# test_refs.py -- tests for refs.py
# Copyright (C) 2013 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of
# the License.
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

"""Tests for dulwich.refs."""

from io import BytesIO
import os
import tempfile

from dulwich import errors
from dulwich.file import (
    GitFile,
    )
from dulwich.refs import (
    DictRefsContainer,
    InfoRefsContainer,
    check_ref_format,
    _split_ref_line,
    read_packed_refs_with_peeled,
    read_packed_refs,
    write_packed_refs,
    )
from dulwich.repo import Repo

from dulwich.tests import (
    TestCase,
    )

from dulwich.tests.utils import (
    open_repo,
    tear_down_repo,
    )


class CheckRefFormatTests(TestCase):
    """Tests for the check_ref_format function.

    These are the same tests as in the git test suite.
    """

    def test_valid(self):
        self.assertTrue(check_ref_format('heads/foo'))
        self.assertTrue(check_ref_format('foo/bar/baz'))
        self.assertTrue(check_ref_format('refs///heads/foo'))
        self.assertTrue(check_ref_format('foo./bar'))
        self.assertTrue(check_ref_format('heads/foo@bar'))
        self.assertTrue(check_ref_format('heads/fix.lock.error'))

    def test_invalid(self):
        self.assertFalse(check_ref_format('foo'))
        self.assertFalse(check_ref_format('heads/foo/'))
        self.assertFalse(check_ref_format('./foo'))
        self.assertFalse(check_ref_format('.refs/foo'))
        self.assertFalse(check_ref_format('heads/foo..bar'))
        self.assertFalse(check_ref_format('heads/foo?bar'))
        self.assertFalse(check_ref_format('heads/foo.lock'))
        self.assertFalse(check_ref_format('heads/v@{ation'))
        self.assertFalse(check_ref_format('heads/foo\bar'))


ONES = "1" * 40
TWOS = "2" * 40
THREES = "3" * 40
FOURS = "4" * 40

class PackedRefsFileTests(TestCase):

    def test_split_ref_line_errors(self):
        self.assertRaises(errors.PackedRefsException, _split_ref_line,
                          'singlefield')
        self.assertRaises(errors.PackedRefsException, _split_ref_line,
                          'badsha name')
        self.assertRaises(errors.PackedRefsException, _split_ref_line,
                          '%s bad/../refname' % ONES)

    def test_read_without_peeled(self):
        f = BytesIO('# comment\n%s ref/1\n%s ref/2' % (ONES, TWOS))
        self.assertEqual([(ONES, 'ref/1'), (TWOS, 'ref/2')],
                         list(read_packed_refs(f)))

    def test_read_without_peeled_errors(self):
        f = BytesIO('%s ref/1\n^%s' % (ONES, TWOS))
        self.assertRaises(errors.PackedRefsException, list, read_packed_refs(f))

    def test_read_with_peeled(self):
        f = BytesIO('%s ref/1\n%s ref/2\n^%s\n%s ref/4' % (
          ONES, TWOS, THREES, FOURS))
        self.assertEqual([
          (ONES, 'ref/1', None),
          (TWOS, 'ref/2', THREES),
          (FOURS, 'ref/4', None),
          ], list(read_packed_refs_with_peeled(f)))

    def test_read_with_peeled_errors(self):
        f = BytesIO('^%s\n%s ref/1' % (TWOS, ONES))
        self.assertRaises(errors.PackedRefsException, list, read_packed_refs(f))

        f = BytesIO('%s ref/1\n^%s\n^%s' % (ONES, TWOS, THREES))
        self.assertRaises(errors.PackedRefsException, list, read_packed_refs(f))

    def test_write_with_peeled(self):
        f = BytesIO()
        write_packed_refs(f, {'ref/1': ONES, 'ref/2': TWOS},
                          {'ref/1': THREES})
        self.assertEqual(
          "# pack-refs with: peeled\n%s ref/1\n^%s\n%s ref/2\n" % (
          ONES, THREES, TWOS), f.getvalue())

    def test_write_without_peeled(self):
        f = BytesIO()
        write_packed_refs(f, {'ref/1': ONES, 'ref/2': TWOS})
        self.assertEqual("%s ref/1\n%s ref/2\n" % (ONES, TWOS), f.getvalue())


# Dict of refs that we expect all RefsContainerTests subclasses to define.
_TEST_REFS = {
  'HEAD': '42d06bd4b77fed026b154d16493e5deab78f02ec',
  'refs/heads/40-char-ref-aaaaaaaaaaaaaaaaaa': '42d06bd4b77fed026b154d16493e5deab78f02ec',
  'refs/heads/master': '42d06bd4b77fed026b154d16493e5deab78f02ec',
  'refs/heads/packed': '42d06bd4b77fed026b154d16493e5deab78f02ec',
  'refs/tags/refs-0.1': 'df6800012397fb85c56e7418dd4eb9405dee075c',
  'refs/tags/refs-0.2': '3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8',
  }


class RefsContainerTests(object):

    def test_keys(self):
        actual_keys = set(self._refs.keys())
        self.assertEqual(set(self._refs.allkeys()), actual_keys)
        # ignore the symref loop if it exists
        actual_keys.discard('refs/heads/loop')
        self.assertEqual(set(_TEST_REFS.iterkeys()), actual_keys)

        actual_keys = self._refs.keys('refs/heads')
        actual_keys.discard('loop')
        self.assertEqual(
            ['40-char-ref-aaaaaaaaaaaaaaaaaa', 'master', 'packed'],
            sorted(actual_keys))
        self.assertEqual(['refs-0.1', 'refs-0.2'],
                         sorted(self._refs.keys('refs/tags')))

    def test_as_dict(self):
        # refs/heads/loop does not show up even if it exists
        self.assertEqual(_TEST_REFS, self._refs.as_dict())

    def test_setitem(self):
        self._refs['refs/some/ref'] = '42d06bd4b77fed026b154d16493e5deab78f02ec'
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/some/ref'])
        self.assertRaises(errors.RefFormatError, self._refs.__setitem__,
                          'notrefs/foo', '42d06bd4b77fed026b154d16493e5deab78f02ec')

    def test_set_if_equals(self):
        nines = '9' * 40
        self.assertFalse(self._refs.set_if_equals('HEAD', 'c0ffee', nines))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['HEAD'])

        self.assertTrue(self._refs.set_if_equals(
          'HEAD', '42d06bd4b77fed026b154d16493e5deab78f02ec', nines))
        self.assertEqual(nines, self._refs['HEAD'])

        self.assertTrue(self._refs.set_if_equals('refs/heads/master', None,
                                                 nines))
        self.assertEqual(nines, self._refs['refs/heads/master'])

    def test_add_if_new(self):
        nines = '9' * 40
        self.assertFalse(self._refs.add_if_new('refs/heads/master', nines))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/heads/master'])

        self.assertTrue(self._refs.add_if_new('refs/some/ref', nines))
        self.assertEqual(nines, self._refs['refs/some/ref'])

    def test_set_symbolic_ref(self):
        self._refs.set_symbolic_ref('refs/heads/symbolic', 'refs/heads/master')
        self.assertEqual('ref: refs/heads/master',
                         self._refs.read_loose_ref('refs/heads/symbolic'))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/heads/symbolic'])

    def test_set_symbolic_ref_overwrite(self):
        nines = '9' * 40
        self.assertFalse('refs/heads/symbolic' in self._refs)
        self._refs['refs/heads/symbolic'] = nines
        self.assertEqual(nines, self._refs.read_loose_ref('refs/heads/symbolic'))
        self._refs.set_symbolic_ref('refs/heads/symbolic', 'refs/heads/master')
        self.assertEqual('ref: refs/heads/master',
                         self._refs.read_loose_ref('refs/heads/symbolic'))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/heads/symbolic'])

    def test_check_refname(self):
        self._refs._check_refname('HEAD')
        self._refs._check_refname('refs/stash')
        self._refs._check_refname('refs/heads/foo')

        self.assertRaises(errors.RefFormatError, self._refs._check_refname,
                          'refs')
        self.assertRaises(errors.RefFormatError, self._refs._check_refname,
                          'notrefs/foo')

    def test_contains(self):
        self.assertTrue('refs/heads/master' in self._refs)
        self.assertFalse('refs/heads/bar' in self._refs)

    def test_delitem(self):
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                          self._refs['refs/heads/master'])
        del self._refs['refs/heads/master']
        self.assertRaises(KeyError, lambda: self._refs['refs/heads/master'])

    def test_remove_if_equals(self):
        self.assertFalse(self._refs.remove_if_equals('HEAD', 'c0ffee'))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['HEAD'])
        self.assertTrue(self._refs.remove_if_equals(
          'refs/tags/refs-0.2', '3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8'))
        self.assertFalse('refs/tags/refs-0.2' in self._refs)




class DictRefsContainerTests(RefsContainerTests, TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self._refs = DictRefsContainer(dict(_TEST_REFS))

    def test_invalid_refname(self):
        # FIXME: Move this test into RefsContainerTests, but requires
        # some way of injecting invalid refs.
        self._refs._refs["refs/stash"] = "00" * 20
        expected_refs = dict(_TEST_REFS)
        expected_refs["refs/stash"] = "00" * 20
        self.assertEqual(expected_refs, self._refs.as_dict())


class DiskRefsContainerTests(RefsContainerTests, TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self._repo = open_repo('refs.git')
        self._refs = self._repo.refs

    def tearDown(self):
        tear_down_repo(self._repo)
        TestCase.tearDown(self)

    def test_get_packed_refs(self):
        self.assertEqual({
          'refs/heads/packed': '42d06bd4b77fed026b154d16493e5deab78f02ec',
          'refs/tags/refs-0.1': 'df6800012397fb85c56e7418dd4eb9405dee075c',
          }, self._refs.get_packed_refs())

    def test_get_peeled_not_packed(self):
        # not packed
        self.assertEqual(None, self._refs.get_peeled('refs/tags/refs-0.2'))
        self.assertEqual('3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8',
                         self._refs['refs/tags/refs-0.2'])

        # packed, known not peelable
        self.assertEqual(self._refs['refs/heads/packed'],
                         self._refs.get_peeled('refs/heads/packed'))

        # packed, peeled
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs.get_peeled('refs/tags/refs-0.1'))

    def test_setitem(self):
        RefsContainerTests.test_setitem(self)
        f = open(os.path.join(self._refs.path, 'refs', 'some', 'ref'), 'rb')
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                          f.read()[:40])
        f.close()

    def test_setitem_symbolic(self):
        ones = '1' * 40
        self._refs['HEAD'] = ones
        self.assertEqual(ones, self._refs['HEAD'])

        # ensure HEAD was not modified
        f = open(os.path.join(self._refs.path, 'HEAD'), 'rb')
        self.assertEqual('ref: refs/heads/master', next(iter(f)).rstrip('\n'))
        f.close()

        # ensure the symbolic link was written through
        f = open(os.path.join(self._refs.path, 'refs', 'heads', 'master'), 'rb')
        self.assertEqual(ones, f.read()[:40])
        f.close()

    def test_set_if_equals(self):
        RefsContainerTests.test_set_if_equals(self)

        # ensure symref was followed
        self.assertEqual('9' * 40, self._refs['refs/heads/master'])

        # ensure lockfile was deleted
        self.assertFalse(os.path.exists(
          os.path.join(self._refs.path, 'refs', 'heads', 'master.lock')))
        self.assertFalse(os.path.exists(
          os.path.join(self._refs.path, 'HEAD.lock')))

    def test_add_if_new_packed(self):
        # don't overwrite packed ref
        self.assertFalse(self._refs.add_if_new('refs/tags/refs-0.1', '9' * 40))
        self.assertEqual('df6800012397fb85c56e7418dd4eb9405dee075c',
                         self._refs['refs/tags/refs-0.1'])

    def test_add_if_new_symbolic(self):
        # Use an empty repo instead of the default.
        tear_down_repo(self._repo)
        repo_dir = os.path.join(tempfile.mkdtemp(), 'test')
        os.makedirs(repo_dir)
        self._repo = Repo.init(repo_dir)
        refs = self._repo.refs

        nines = '9' * 40
        self.assertEqual('ref: refs/heads/master', refs.read_ref('HEAD'))
        self.assertFalse('refs/heads/master' in refs)
        self.assertTrue(refs.add_if_new('HEAD', nines))
        self.assertEqual('ref: refs/heads/master', refs.read_ref('HEAD'))
        self.assertEqual(nines, refs['HEAD'])
        self.assertEqual(nines, refs['refs/heads/master'])
        self.assertFalse(refs.add_if_new('HEAD', '1' * 40))
        self.assertEqual(nines, refs['HEAD'])
        self.assertEqual(nines, refs['refs/heads/master'])

    def test_follow(self):
        self.assertEqual(
          ('refs/heads/master', '42d06bd4b77fed026b154d16493e5deab78f02ec'),
          self._refs._follow('HEAD'))
        self.assertEqual(
          ('refs/heads/master', '42d06bd4b77fed026b154d16493e5deab78f02ec'),
          self._refs._follow('refs/heads/master'))
        self.assertRaises(KeyError, self._refs._follow, 'refs/heads/loop')

    def test_delitem(self):
        RefsContainerTests.test_delitem(self)
        ref_file = os.path.join(self._refs.path, 'refs', 'heads', 'master')
        self.assertFalse(os.path.exists(ref_file))
        self.assertFalse('refs/heads/master' in self._refs.get_packed_refs())

    def test_delitem_symbolic(self):
        self.assertEqual('ref: refs/heads/master',
                          self._refs.read_loose_ref('HEAD'))
        del self._refs['HEAD']
        self.assertRaises(KeyError, lambda: self._refs['HEAD'])
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/heads/master'])
        self.assertFalse(os.path.exists(os.path.join(self._refs.path, 'HEAD')))

    def test_remove_if_equals_symref(self):
        # HEAD is a symref, so shouldn't equal its dereferenced value
        self.assertFalse(self._refs.remove_if_equals(
          'HEAD', '42d06bd4b77fed026b154d16493e5deab78f02ec'))
        self.assertTrue(self._refs.remove_if_equals(
          'refs/heads/master', '42d06bd4b77fed026b154d16493e5deab78f02ec'))
        self.assertRaises(KeyError, lambda: self._refs['refs/heads/master'])

        # HEAD is now a broken symref
        self.assertRaises(KeyError, lambda: self._refs['HEAD'])
        self.assertEqual('ref: refs/heads/master',
                          self._refs.read_loose_ref('HEAD'))

        self.assertFalse(os.path.exists(
            os.path.join(self._refs.path, 'refs', 'heads', 'master.lock')))
        self.assertFalse(os.path.exists(
            os.path.join(self._refs.path, 'HEAD.lock')))

    def test_remove_packed_without_peeled(self):
        refs_file = os.path.join(self._repo.path, 'packed-refs')
        f = GitFile(refs_file)
        refs_data = f.read()
        f.close()
        f = GitFile(refs_file, 'wb')
        f.write('\n'.join(l for l in refs_data.split('\n')
                          if not l or l[0] not in '#^'))
        f.close()
        self._repo = Repo(self._repo.path)
        refs = self._repo.refs
        self.assertTrue(refs.remove_if_equals(
          'refs/heads/packed', '42d06bd4b77fed026b154d16493e5deab78f02ec'))

    def test_remove_if_equals_packed(self):
        # test removing ref that is only packed
        self.assertEqual('df6800012397fb85c56e7418dd4eb9405dee075c',
                         self._refs['refs/tags/refs-0.1'])
        self.assertTrue(
          self._refs.remove_if_equals('refs/tags/refs-0.1',
          'df6800012397fb85c56e7418dd4eb9405dee075c'))
        self.assertRaises(KeyError, lambda: self._refs['refs/tags/refs-0.1'])

    def test_read_ref(self):
        self.assertEqual('ref: refs/heads/master', self._refs.read_ref("HEAD"))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
            self._refs.read_ref("refs/heads/packed"))
        self.assertEqual(None,
            self._refs.read_ref("nonexistant"))


_TEST_REFS_SERIALIZED = (
'42d06bd4b77fed026b154d16493e5deab78f02ec\trefs/heads/40-char-ref-aaaaaaaaaaaaaaaaaa\n'
'42d06bd4b77fed026b154d16493e5deab78f02ec\trefs/heads/master\n'
'42d06bd4b77fed026b154d16493e5deab78f02ec\trefs/heads/packed\n'
'df6800012397fb85c56e7418dd4eb9405dee075c\trefs/tags/refs-0.1\n'
'3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8\trefs/tags/refs-0.2\n')


class InfoRefsContainerTests(TestCase):

    def test_invalid_refname(self):
        text = _TEST_REFS_SERIALIZED + '00' * 20 + '\trefs/stash\n'
        refs = InfoRefsContainer(BytesIO(text))
        expected_refs = dict(_TEST_REFS)
        del expected_refs['HEAD']
        expected_refs["refs/stash"] = "00" * 20
        self.assertEqual(expected_refs, refs.as_dict())

    def test_keys(self):
        refs = InfoRefsContainer(BytesIO(_TEST_REFS_SERIALIZED))
        actual_keys = set(refs.keys())
        self.assertEqual(set(refs.allkeys()), actual_keys)
        # ignore the symref loop if it exists
        actual_keys.discard('refs/heads/loop')
        expected_refs = dict(_TEST_REFS)
        del expected_refs['HEAD']
        self.assertEqual(set(expected_refs.iterkeys()), actual_keys)

        actual_keys = refs.keys('refs/heads')
        actual_keys.discard('loop')
        self.assertEqual(
            ['40-char-ref-aaaaaaaaaaaaaaaaaa', 'master', 'packed'],
            sorted(actual_keys))
        self.assertEqual(['refs-0.1', 'refs-0.2'],
                         sorted(refs.keys('refs/tags')))

    def test_as_dict(self):
        refs = InfoRefsContainer(BytesIO(_TEST_REFS_SERIALIZED))
        # refs/heads/loop does not show up even if it exists
        expected_refs = dict(_TEST_REFS)
        del expected_refs['HEAD']
        self.assertEqual(expected_refs, refs.as_dict())

    def test_contains(self):
        refs = InfoRefsContainer(BytesIO(_TEST_REFS_SERIALIZED))
        self.assertTrue('refs/heads/master' in refs)
        self.assertFalse('refs/heads/bar' in refs)

    def test_get_peeled(self):
        refs = InfoRefsContainer(BytesIO(_TEST_REFS_SERIALIZED))
        # refs/heads/loop does not show up even if it exists
        self.assertEqual(
            _TEST_REFS['refs/heads/master'],
            refs.get_peeled('refs/heads/master'))
