#-*- coding:utf-8 -*-
# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Tests for swift.obj.diskfile """

from __future__ import with_statement

import cPickle as pickle
import os
import mock
import unittest
import email
import tempfile
from shutil import rmtree
from time import time
from tempfile import mkdtemp
from hashlib import md5
from contextlib import closing
from gzip import GzipFile

from eventlet import tpool
from test.unit import FakeLogger, mock as unit_mock
from test.unit import _setxattr as setxattr
from swift.obj import diskfile
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, normalize_timestamp
from swift.common import ring
from swift.common.exceptions import DiskFileError, DiskFileNotExist, \
        DiskFileDeleted, DiskFileDeviceUnavailable, DiskFileSizeInvalid


def _create_test_ring(path):
    testgz = os.path.join(path, 'object.ring.gz')
    intended_replica2part2dev_id = [
        [0, 1, 2, 3, 4, 5, 6],
        [1, 2, 3, 0, 5, 6, 4],
        [2, 3, 0, 1, 6, 4, 5]]
    intended_devs = [
        {'id': 0, 'device': 'sda', 'zone': 0, 'ip': '127.0.0.0', 'port': 6000},
        {'id': 1, 'device': 'sda', 'zone': 1, 'ip': '127.0.0.1', 'port': 6000},
        {'id': 2, 'device': 'sda', 'zone': 2, 'ip': '127.0.0.2', 'port': 6000},
        {'id': 3, 'device': 'sda', 'zone': 4, 'ip': '127.0.0.3', 'port': 6000},
        {'id': 4, 'device': 'sda', 'zone': 5, 'ip': '127.0.0.4', 'port': 6000},
        {'id': 5, 'device': 'sda', 'zone': 6,
         'ip': 'fe80::202:b3ff:fe1e:8329', 'port': 6000},
        {'id': 6, 'device': 'sda', 'zone': 7,
         'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334', 'port': 6000}]
    intended_part_shift = 30
    intended_reload_time = 15
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id, intended_devs,
                          intended_part_shift),
            f)
    return ring.Ring(path, ring_name='object',
                     reload_time=intended_reload_time)


class TestDiskFileModuleMethods(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''
        # Setup a test ring (stolen from common/test_ring.py)
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)
        os.mkdir(os.path.join(self.devices, 'sda'))
        self.objects = os.path.join(self.devices, 'sda', 'objects')
        os.mkdir(self.objects)
        self.parts = {}
        for part in ['0', '1', '2', '3']:
            self.parts[part] = os.path.join(self.objects, part)
            os.mkdir(os.path.join(self.objects, part))
        self.ring = _create_test_ring(self.testdir)
        self.conf = dict(
            swift_dir=self.testdir, devices=self.devices, mount_check='false',
            timeout='300', stats_interval='1')

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_hash_suffix_hash_dir_is_file_quarantine(self):
        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(os.path.dirname(df.datadir))
        open(df.datadir, 'wb').close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        orig_quarantine_renamer = diskfile.quarantine_renamer
        called = [False]

        def wrapped(*args, **kwargs):
            called[0] = True
            return orig_quarantine_renamer(*args, **kwargs)

        try:
            diskfile.quarantine_renamer = wrapped
            diskfile.hash_suffix(whole_path_from, 101)
        finally:
            diskfile.quarantine_renamer = orig_quarantine_renamer
        self.assertTrue(called[0])

    def test_hash_suffix_one_file(self):
        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        f = open(
            os.path.join(df.datadir,
                         normalize_timestamp(time() - 100) + '.ts'),
            'wb')
        f.write('1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        diskfile.hash_suffix(whole_path_from, 101)
        self.assertEquals(len(os.listdir(self.parts['0'])), 1)

        diskfile.hash_suffix(whole_path_from, 99)
        self.assertEquals(len(os.listdir(self.parts['0'])), 0)

    def test_hash_suffix_multi_file_one(self):
        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        for tdiff in [1, 50, 100, 500]:
            for suff in ['.meta', '.data', '.ts']:
                f = open(
                    os.path.join(
                        df.datadir,
                        normalize_timestamp(int(time()) - tdiff) + suff),
                    'wb')
                f.write('1234567890')
                f.close()

        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hsh_path = os.listdir(whole_path_from)[0]
        whole_hsh_path = os.path.join(whole_path_from, hsh_path)

        diskfile.hash_suffix(whole_path_from, 99)
        # only the tombstone should be left
        self.assertEquals(len(os.listdir(whole_hsh_path)), 1)

    def test_hash_suffix_multi_file_two(self):
        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        for tdiff in [1, 50, 100, 500]:
            suffs = ['.meta', '.data']
            if tdiff > 50:
                suffs.append('.ts')
            for suff in suffs:
                f = open(
                    os.path.join(
                        df.datadir,
                        normalize_timestamp(int(time()) - tdiff) + suff),
                    'wb')
                f.write('1234567890')
                f.close()

        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hsh_path = os.listdir(whole_path_from)[0]
        whole_hsh_path = os.path.join(whole_path_from, hsh_path)

        diskfile.hash_suffix(whole_path_from, 99)
        # only the meta and data should be left
        self.assertEquals(len(os.listdir(whole_hsh_path)), 2)

    def test_invalidate_hash(self):

        def assertFileData(file_path, data):
            with open(file_path, 'r') as fp:
                fdata = fp.read()
                self.assertEquals(pickle.loads(fdata), pickle.loads(data))

        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hashes_file = os.path.join(self.objects, '0',
                                   diskfile.HASH_FILE)
        # test that non existent file except caught
        self.assertEquals(diskfile.invalidate_hash(whole_path_from),
                          None)
        # test that hashes get cleared
        check_pickle_data = pickle.dumps({data_dir: None},
                                         diskfile.PICKLE_PROTOCOL)
        for data_hash in [{data_dir: None}, {data_dir: 'abcdefg'}]:
            with open(hashes_file, 'wb') as fp:
                pickle.dump(data_hash, fp, diskfile.PICKLE_PROTOCOL)
            diskfile.invalidate_hash(whole_path_from)
            assertFileData(hashes_file, check_pickle_data)

    def test_get_hashes(self):
        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        with open(
                os.path.join(df.datadir,
                             normalize_timestamp(time()) + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = diskfile.get_hashes(part)
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)
        hashed, hashes = diskfile.get_hashes(part, do_listdir=True)
        self.assertEquals(hashed, 0)
        self.assert_('a83' in hashes)
        hashed, hashes = diskfile.get_hashes(part, recalculate=['a83'])
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)

    def test_get_hashes_bad_dir(self):
        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        with open(os.path.join(self.objects, '0', 'bad'), 'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = diskfile.get_hashes(part)
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)
        self.assert_('bad' not in hashes)

    def test_get_hashes_unmodified(self):
        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        with open(
                os.path.join(df.datadir,
                             normalize_timestamp(time()) + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = diskfile.get_hashes(part)
        i = [0]

        def _getmtime(filename):
            i[0] += 1
            return 1
        with unit_mock({'swift.obj.diskfile.getmtime': _getmtime}):
            hashed, hashes = diskfile.get_hashes(
                part, recalculate=['a83'])
        self.assertEquals(i[0], 2)

    def test_get_hashes_unmodified_and_zero_bytes(self):
        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        part = os.path.join(self.objects, '0')
        open(os.path.join(part, diskfile.HASH_FILE), 'w')
        # Now the hash file is zero bytes.
        i = [0]

        def _getmtime(filename):
            i[0] += 1
            return 1
        with unit_mock({'swift.obj.diskfile.getmtime': _getmtime}):
            diskfile.getmtime("/tmp")
            hashed, hashes = diskfile.get_hashes(
                part, recalculate=[])
        # getmtime will actually not get called.  Initially, the pickle.load
        # will raise an exception first and later, force_rewrite will
        # short-circuit the if clause to determine whether to write out a
        # fresh hashes_file.
        self.assertEquals(i[0], 1)
        self.assertTrue('a83' in hashes)

    def test_get_hashes_modified(self):
        df = diskfile.DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        with open(
                os.path.join(df.datadir,
                             normalize_timestamp(time()) + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = diskfile.get_hashes(part)
        i = [0]

        def _getmtime(filename):
            if i[0] < 3:
                i[0] += 1
            return i[0]
        with unit_mock({'swift.obj.diskfile.getmtime': _getmtime}):
            hashed, hashes = diskfile.get_hashes(
                part, recalculate=['a83'])
        self.assertEquals(i[0], 3)


class TestDiskFile(unittest.TestCase):
    """Test swift.obj.diskfile.DiskFile"""

    def setUp(self):
        """ Set up for testing swift.obj.diskfile"""
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_obj_server_DiskFile')
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)

    def tearDown(self):
        """ Tear down for testing swift.obj.diskfile"""
        rmtree(os.path.dirname(self.testdir))
        tpool.execute = self._orig_tpool_exc

    def _create_test_reader(self, data):
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        f.write(data)
        setxattr(f.fileno(), diskfile.METADATA_KEY,
                 pickle.dumps({}, diskfile.PICKLE_PROTOCOL))
        f.close()
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        return df.open()

    def test_disk_file_default_disallowed_metadata(self):
        # build an object with some meta (ts 41)
        orig_metadata = {'X-Object-Meta-Key1': 'Value1',
                         'Content-Type': 'text/garbage'}
        df = self._get_disk_file(ts=41, extra_metadata=orig_metadata)
        with df.open():
            metadata = df.read_metadata()
        self.assertEquals('1024', metadata['Content-Length'])
        # write some new metadata (fast POST, don't send orig meta, ts 42)
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        df.update_metadata({'X-Timestamp': '42', 'X-Object-Meta-Key2': 'Value2'})
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        with df.open():
            metadata = df.read_metadata()
        # non-fast-post updateable keys are preserved
        self.assertEquals('text/garbage', metadata['Content-Type'])
        # original fast-post updateable keys are removed
        self.assert_('X-Object-Meta-Key1' not in metadata)
        # new fast-post updateable keys are added
        self.assertEquals('Value2', metadata['X-Object-Meta-Key2'])

    def test_disk_file_app_iter_corners(self):
        reader = self._create_test_reader('1234567890')
        self.assertEquals(''.join(reader.app_iter_range(0, None)), '1234567890')

        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        with df.open() as reader:
            self.assertEqual(''.join(reader.app_iter_range(5, None)), '67890')

    def test_disk_file_app_iter_partial_closes(self):
        reader = self._create_test_reader('1234567890')
        it = reader.app_iter_range(0, 5)
        self.assertEqual(''.join(it), '12345')
        self.assertEqual(reader.fp, None)

    def test_disk_file_app_iter_ranges(self):
        reader = self._create_test_reader('012345678911234567892123456789')
        it = reader.app_iter_ranges([(0, 10), (10, 20), (20, 30)], 'plain/text',
                                '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assert_('0123456789' in value)
        self.assert_('1123456789' in value)
        self.assert_('2123456789' in value)

    def test_disk_file_app_iter_ranges_edges(self):
        reader = self._create_test_reader('012345678911234567892123456789')
        it = reader.app_iter_ranges([(3, 10), (0, 2)], 'application/whatever',
                                '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assert_('3456789' in value)
        self.assert_('01' in value)

    def test_disk_file_large_app_iter_ranges(self):
        """
        This test case is to make sure that the disk file app_iter_ranges
        method all the paths being tested.
        """
        long_str = '01234567890' * 65536
        target_strs = ['3456789', long_str[0:65590]]
        reader = self._create_test_reader(long_str)

        it = reader.app_iter_ranges([(3, 10), (0, 65590)], 'plain/text',
                                '5e816ff8b8b8e9a5d355497e5d9e0301', 655360)

        """
        the produced string actually missing the MIME headers
        need to add these headers to make it as real MIME message.
        The body of the message is produced by method app_iter_ranges
        off of DiskFile object.
        """
        header = ''.join(['Content-Type: multipart/byteranges;',
                          'boundary=',
                          '5e816ff8b8b8e9a5d355497e5d9e0301\r\n'])

        value = header + ''.join(it)

        parts = map(lambda p: p.get_payload(decode=True),
                    email.message_from_string(value).walk())[1:3]
        self.assertEqual(parts, target_strs)

    def test_disk_file_app_iter_ranges_empty(self):
        """
        This test case tests when empty value passed into app_iter_ranges
        When ranges passed into the method is either empty array or None,
        this method will yield empty string
        """
        reader = self._create_test_reader('012345678911234567892123456789')
        it = reader.app_iter_ranges([], 'application/whatever',
                                '\r\n--someheader\r\n', 100)
        self.assertEqual(''.join(it), '')

        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        with df.open() as reader:
            it = reader.app_iter_ranges(None, 'app/something',
                                        '\r\n--someheader\r\n', 150)
            self.assertEqual(''.join(it), '')

    def test_disk_file_mkstemp_creates_dir(self):
        tmpdir = os.path.join(self.testdir, 'sda1', 'tmp')
        os.rmdir(tmpdir)
        with diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                                    'o', FakeLogger()).create():
            self.assert_(os.path.exists(tmpdir))

    def test_iter_hook(self):
        hook_call_count = [0]

        def hook():
            hook_call_count[0] += 1

        df = self._get_disk_file(fsize=65, csize=8, iter_hook=hook)
        with df.open() as reader:
            for _ in reader:
                pass

        self.assertEquals(hook_call_count[0], 9)

    def test_quarantine(self):
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                               FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        setxattr(f.fileno(), diskfile.METADATA_KEY,
                 pickle.dumps({}, diskfile.PICKLE_PROTOCOL))
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                               FakeLogger())
        with df.open() as reader:
            pass
        reader.quarantine()
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined',
                                'objects', os.path.basename(os.path.dirname(
                                                            reader._data_file)))
        self.assert_(os.path.isdir(quar_dir))

    def test_quarantine_same_file(self):
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        setxattr(f.fileno(), diskfile.METADATA_KEY,
                 pickle.dumps({}, diskfile.PICKLE_PROTOCOL))
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        with df.open() as reader:
            pass
        new_dir = reader.quarantine()
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined',
                                'objects', os.path.basename(os.path.dirname(
                                    reader._data_file)))
        self.assert_(os.path.isdir(quar_dir))
        self.assertEquals(quar_dir, new_dir)
        # have to remake the datadir and file
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        setxattr(f.fileno(), diskfile.METADATA_KEY,
                 pickle.dumps({}, diskfile.PICKLE_PROTOCOL))

        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        with df.open() as reader:
            pass
        double_uuid_path = reader.quarantine()
        self.assert_(os.path.isdir(double_uuid_path))
        self.assert_('-' in os.path.basename(double_uuid_path))

    def _get_disk_file(self, invalid_type=None, obj_name='o',
                       fsize=1024, csize=8, mark_deleted=False, ts=None,
                       iter_hook=None, mount_check=False,
                       extra_metadata=None):
        '''returns a DiskFile'''
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                                    obj_name, FakeLogger())
        data = '0' * fsize
        etag = md5()
        if ts:
            timestamp = ts
        else:
            timestamp = str(normalize_timestamp(time()))
        with df.create() as writer:
            writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(writer.fd).st_size),
            }
            metadata.update(extra_metadata or {})
            writer.put(metadata)
            if invalid_type == 'ETag':
                etag = md5()
                etag.update('1' + '0' * (fsize - 1))
                etag = etag.hexdigest()
                metadata['ETag'] = etag
                diskfile.write_metadata(writer.fd, metadata)
            if invalid_type == 'Content-Length':
                metadata['Content-Length'] = fsize - 1
                diskfile.write_metadata(writer.fd, metadata)
        if mark_deleted:
            df.delete(timestamp)
        if invalid_type == 'Zero-Byte':
            os.remove(df._data_file)
            fp = open(df._data_file, 'w')
            fp.close()
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                               obj_name, FakeLogger(),
                               disk_chunk_size=csize,
                               iter_hook=iter_hook, mount_check=mount_check)
        df.unit_test_len = fsize
        return df

    def test_quarantine_valids(self):
        df = self._get_disk_file(obj_name='1')
        with df.open() as reader:
            for chunk in reader:
                pass
        self.assertFalse(reader.quarantined_dir)

        df = self._get_disk_file(obj_name='2', csize=1)
        with df.open() as reader:
            for chunk in reader:
                pass
        self.assertFalse(reader.quarantined_dir)

        df = self._get_disk_file(obj_name='3', csize=100000)
        with df.open() as reader:
            for chunk in reader:
                pass
        self.assertFalse(reader.quarantined_dir)

    def _safe_disk_iter(self, disk_iter):
        try:
            for chunk in disk_iter:
                pass
        except DiskFileError:
            pass

    def run_quarantine_invalids(self, invalid_type):
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='1')
        with df.open() as reader:
            self._safe_disk_iter(reader)
        self.assertTrue(reader.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type,
                                 obj_name='2', csize=1)
        with df.open() as reader:
            self._safe_disk_iter(reader)
        self.assertTrue(reader.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type,
                                 obj_name='3', csize=100000)
        with df.open() as reader:
            self._safe_disk_iter(reader)
        self.assertTrue(reader.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='4')
        with df.open() as reader:
            self.assertFalse(reader.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='5')
        with df.open() as reader:
            self._safe_disk_iter(reader.app_iter_range(0, df.unit_test_len))
        self.assertTrue(reader.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='6')
        with df.open() as reader:
            self._safe_disk_iter(reader.app_iter_range(0, df.unit_test_len + 100))
        self.assertTrue(reader.quarantined_dir)
        expected_quar = False
        # for the following, Content-Length/Zero-Byte errors will always result
        # in a quarantine, even if the whole file isn't check-summed
        if invalid_type in ('Zero-Byte', 'Content-Length'):
            expected_quar = True
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='7')
        with df.open() as reader:
            self._safe_disk_iter(reader.app_iter_range(1, df.unit_test_len))
        self.assertEquals(bool(reader.quarantined_dir), expected_quar)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='8')
        with df.open() as reader:
            self._safe_disk_iter(reader.app_iter_range(0, df.unit_test_len - 1))
        self.assertEquals(bool(reader.quarantined_dir), expected_quar)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='8')
        with df.open() as reader:
            self._safe_disk_iter(reader.app_iter_range(1, df.unit_test_len + 1))
        self.assertEquals(bool(reader.quarantined_dir), expected_quar)

    def test_quarantine_invalids(self):
        self.run_quarantine_invalids('ETag')
        self.run_quarantine_invalids('Content-Length')
        self.run_quarantine_invalids('Zero-Byte')

    def test_quarantine_on_get_size(self):
        df = self._get_disk_file(invalid_type='Content-Length')
        try:
            with df.open() as reader:
                reader.get_obj_size()
        except DiskFileError:
            pass
        self.assertTrue(reader.quarantined_dir)
        self.assertRaises(DiskFileNotExist, df.open)

    def test_quarantine_on_read(self):
        df = self._get_disk_file(invalid_type='Content-Length')
        try:
            with df.open() as reader:
                iter(reader).next()
        except DiskFileError:
            pass
        self.assertTrue(reader.quarantined_dir)
        self.assertRaises(DiskFileNotExist, df.open)

    def test_quarantine_on_read_metadata(self):
        df = self._get_disk_file(invalid_type='Content-Length')
        self.assertRaises(DiskFileSizeInvalid, df.read_metadata)
        self.assertRaises(DiskFileNotExist, df.open)

    def test_skip_quarantine_check_on_read_metadata(self):
        df = self._get_disk_file(invalid_type='Content-Length')
        df.read_metadata(verify_data_file_size=False)
        with df.open() as reader:
            self.assertRaises(DiskFileSizeInvalid, reader.get_obj_size)

    def test_update_metadata(self):
        df = self._get_disk_file()
        ts = time()
        metadata = {'X-Timestamp': ts, 'X-Object-Meta-test': 'data'}
        df.update_metadata(metadata)
        exp_name = '%s.meta' % str(normalize_timestamp(ts))
        dl = os.listdir(df.datadir)
        self.assertEquals(len(dl), 2)
        self.assertTrue(exp_name in set(dl))

    def test_delete(self):
        df = self._get_disk_file()
        ts = time()
        df.delete(ts)
        dl = os.listdir(df.datadir)
        self.assertEquals(len(dl), 1)
        exp_name = '%s.ts' % str(normalize_timestamp(ts))
        self.assertEquals(exp_name, dl[0])

    def test_unlinkold_after_create(self):
        current_time = time()
        df1 = self._get_disk_file(ts=current_time)
        dl = os.listdir(df1.datadir)
        self.assertEquals(len(dl), 1)
        self.assertEquals(dl[0], "%s.data" % normalize_timestamp(current_time))
        future_time = current_time + 100
        df2 = self._get_disk_file(ts=future_time)
        dl = os.listdir(df2.datadir)
        self.assertEquals(len(dl), 1)
        self.assertEquals(dl[0], "%s.data" % normalize_timestamp(future_time))

    def test_close_error(self):

        def err():
            raise Exception("bad")

        df = self._get_disk_file(fsize=1024 * 2)
        with df.open() as reader:
            reader._handle_close_quarantine = err
            for chunk in reader:
                pass
        # close is called at the end of the iterator
        self.assertEquals(reader.fp, None)
        self.assertEquals(len(reader.logger.log_dict['error']), 1)

    def test_quarantine_twice(self):
        df = self._get_disk_file(invalid_type='Content-Length')
        with df.open() as reader:
            self.assert_(os.path.isfile(reader._data_file))
            self._safe_disk_iter(reader)
        self.assertFalse(os.path.isfile(reader._data_file))
        self.assert_(os.path.isdir(reader.quarantined_dir))
        self.assertEquals(reader.quarantine(), None)

    def test_mount_checking(self):
        def _mock_ismount(*args, **kwargs):
            return False
        with mock.patch("os.path.ismount", _mock_ismount):
            self.assertRaises(DiskFileDeviceUnavailable, self._get_disk_file,
                              mount_check=True)

    def test_drop_cache_with_no_keep_cache_size_drops_cache(self):
        df = self._get_disk_file()
        called = []
        def _mock_drop_buffer_cache(*args, **kwargs):
            called.append(True)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache",
                        _mock_drop_buffer_cache):
            with df.open() as reader:
                for chunk in reader:
                    pass
        self.assertEquals(called, [True])

    def test_drop_cache_with_small_size_drops_cache(self):
        df = self._get_disk_file()
        called = []
        def _mock_drop_buffer_cache(*args, **kwargs):
            called.append(True)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache",
                        _mock_drop_buffer_cache):
            with df.open(keep_cache_size=1) as reader:
                for chunk in reader:
                    pass
        self.assertEquals(called, [True])

    def test_drop_cache_with_large_size_keeps_cache(self):
        df = self._get_disk_file()
        called = []
        def _mock_drop_buffer_cache(*args, **kwargs):
            called.append(True)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache",
                        _mock_drop_buffer_cache):
            with df.open(keep_cache_size=2 ** 20) as reader:
                for chunk in reader:
                    pass
        self.assertEquals(called, [])
