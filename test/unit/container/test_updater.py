# Copyright (c) 2010-2012 OpenStack Foundation
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

import cPickle as pickle
import mock
import os
import time
import unittest
from collections import defaultdict
from contextlib import closing
from gzip import GzipFile
from shutil import rmtree
from tempfile import mkdtemp

from eventlet import spawn, Timeout, listen

from swift.common import internal_client, swob, utils
from swift.container import updater as container_updater
from swift.container.backend import ContainerBroker, DATADIR
from swift.common.exceptions import ClientException
from swift.common.ring import RingData
from swift.common.utils import normalize_timestamp, split_path
from test.unit import FakeLogger, FakeRing
from test.unit.common.middleware.helpers import FakeSwift


class TestContainerUpdater(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = 'startcap'
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_container_updater')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        ring_file = os.path.join(self.testdir, 'account.ring.gz')
        with closing(GzipFile(ring_file, 'wb')) as f:
            pickle.dump(
                RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
                         [{'id': 0, 'ip': '127.0.0.1', 'port': 12345,
                           'device': 'sda1', 'zone': 0},
                          {'id': 1, 'ip': '127.0.0.1', 'port': 12345,
                           'device': 'sda1', 'zone': 2}], 30),
                f)
        self.devices_dir = os.path.join(self.testdir, 'devices')
        os.mkdir(self.devices_dir)
        self.sda1 = os.path.join(self.devices_dir, 'sda1')
        os.mkdir(self.sda1)

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    def test_creation(self):
        cu = container_updater.ContainerUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '2',
            'node_timeout': '5',
        })
        self.assert_(hasattr(cu, 'logger'))
        self.assert_(cu.logger is not None)
        self.assertEquals(cu.devices, self.devices_dir)
        self.assertEquals(cu.interval, 1)
        self.assertEquals(cu.concurrency, 2)
        self.assertEquals(cu.node_timeout, 5)
        self.assert_(cu.get_account_ring() is not None)

    def test_run_once(self):
        cu = container_updater.ContainerUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15',
            'account_suppression_time': 0
        })
        cu.run_once()
        containers_dir = os.path.join(self.sda1, DATADIR)
        os.mkdir(containers_dir)
        cu.run_once()
        self.assert_(os.path.exists(containers_dir))
        subdir = os.path.join(containers_dir, 'subdir')
        os.mkdir(subdir)
        cb = ContainerBroker(os.path.join(subdir, 'hash.db'), account='a',
                             container='c')
        cb.initialize(normalize_timestamp(1), 0)
        cu.run_once()
        info = cb.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        cb.put_object('o', normalize_timestamp(2), 3, 'text/plain',
                      '68b329da9893e34099c7d8ad5cb9c940', 0)
        cu.run_once()
        info = cb.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 3)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        def accept(sock, addr, return_code):
            try:
                with Timeout(3):
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write('HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEquals(inc.readline(),
                                      'PUT /sda1/0/a/c HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != '\r\n':
                        headers[line.split(':')[0].lower()] = \
                            line.split(':')[1].strip()
                        line = inc.readline()
                    self.assert_('x-put-timestamp' in headers)
                    self.assert_('x-delete-timestamp' in headers)
                    self.assert_('x-object-count' in headers)
                    self.assert_('x-bytes-used' in headers)
            except BaseException as err:
                import traceback
                traceback.print_exc()
                return err
            return None
        bindsock = listen(('127.0.0.1', 0))

        def spawn_accepts():
            events = []
            for _junk in xrange(2):
                sock, addr = bindsock.accept()
                events.append(spawn(accept, sock, addr, 201))
            return events

        spawned = spawn(spawn_accepts)
        for dev in cu.get_account_ring().devs:
            if dev is not None:
                dev['port'] = bindsock.getsockname()[1]
        cu.run_once()
        for event in spawned.wait():
            err = event.wait()
            if err:
                raise err
        info = cb.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 3)
        self.assertEquals(info['reported_object_count'], 1)
        self.assertEquals(info['reported_bytes_used'], 3)

    def test_unicode(self):
        cu = container_updater.ContainerUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15',
        })
        containers_dir = os.path.join(self.sda1, DATADIR)
        os.mkdir(containers_dir)
        subdir = os.path.join(containers_dir, 'subdir')
        os.mkdir(subdir)
        cb = ContainerBroker(os.path.join(subdir, 'hash.db'), account='a',
                             container='\xce\xa9')
        cb.initialize(normalize_timestamp(1), 0)
        cb.put_object('\xce\xa9', normalize_timestamp(2), 3, 'text/plain',
                      '68b329da9893e34099c7d8ad5cb9c940', 0)

        def accept(sock, addr):
            try:
                with Timeout(3):
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write('HTTP/1.1 201 OK\r\nContent-Length: 0\r\n\r\n')
                    out.flush()
                    inc.read()
            except BaseException as err:
                import traceback
                traceback.print_exc()
                return err
            return None

        bindsock = listen(('127.0.0.1', 0))

        def spawn_accepts():
            events = []
            for _junk in xrange(2):
                with Timeout(3):
                    sock, addr = bindsock.accept()
                    events.append(spawn(accept, sock, addr))
            return events

        spawned = spawn(spawn_accepts)
        for dev in cu.get_account_ring().devs:
            if dev is not None:
                dev['port'] = bindsock.getsockname()[1]
        cu.run_once()
        for event in spawned.wait():
            err = event.wait()
            if err:
                raise err
        info = cb.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 3)
        self.assertEquals(info['reported_object_count'], 1)
        self.assertEquals(info['reported_bytes_used'], 3)


class FakeStoragePolicySwift(object):
    """
    Acts like a FakeSwift, but internally has one FakeSwift per storage policy
    and routes things accordingly.
    """
    def __init__(self):
        self.storage_policy = defaultdict(FakeSwift)
        self._mock_oldest_spi_map = {}

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            return getattr(self.storage_policy[None], name)

    def __call__(self, env, start_response):
        method = env['REQUEST_METHOD']
        path = env['PATH_INFO']
        _, acc, cont, obj = split_path(env['PATH_INFO'], 0, 4,
                                       rest_with_last=True)
        if not obj:
            spidx = None
        else:
            spidx = int(env.get('HTTP_X_BACKEND_STORAGE_POLICY_INDEX',
                                self._mock_oldest_spi_map.get(cont, 0)))

        try:
            return self.storage_policy[spidx].__call__(
                env, start_response)
        except KeyError:
            pass

        if method == 'PUT':
            resp_class = swob.HTTPCreated
        else:
            resp_class = swob.HTTPNotFound
        self.storage_policy[spidx].register(
            method, path, resp_class, {}, '')

        return self.storage_policy[spidx].__call__(
            env, start_response)


class FakeInternalClient(internal_client.InternalClient):
    def __init__(self):
        self.app = FakeStoragePolicySwift()
        self.user_agent = 'fake-internal-client'
        self.request_tries = 1


class TestContainerReconciler(unittest.TestCase):
    def setUp(self):
        self.testdir = mkdtemp()

        self.container_policy = 633
        self.broker = ContainerBroker(
            os.path.join(self.testdir, 'test_reconciler'),
            account='a', container='c')
        self.broker.initialize(normalize_timestamp(time.time()),
                               self.container_policy)

        self.reconciler = container_updater.ContainerReconciler({
            'request_tries': 1}, FakeLogger())
        self.reconciler.container_ring = FakeRing()
        self.reconciler.swift = FakeInternalClient()
        self.fake_swift = self.reconciler.swift.app

    def tearDown(self):
        rmtree(self.testdir)

    def reconcile(self, oldest_policy_index=False, check_calls=True):
        if oldest_policy_index is False:
            # Match by default, but allow someone to pass in None because None
            # indicates failure
            oldest_policy_index = self.container_policy

        with mock.patch.object(
                container_updater,
                'direct_get_oldest_storage_policy_index') as mock_get_oldest:
            mock_get_oldest.return_value = oldest_policy_index
            self.reconciler.reconcile_container(
                self.broker, self.broker.get_info())

        # we actually called mock_get_oldest correctly
        if check_calls:
            calls = mock_get_oldest.mock_calls
            self.assertEqual(len(calls), 1)
            self.assertEqual(
                calls[0],
                mock.call(self.reconciler.container_ring,
                          self.broker.account, self.broker.container))

    def test_happy_path(self):
        now = 1395725943.92440

        # this one's in the right place, so it'll be skipped
        self.broker.put_object(
            'abc', normalize_timestamp(now), 0, 'text/plain',
            'etag-abc', self.container_policy)
        self.broker.put_object(
            'def', normalize_timestamp(now), 0, 'text/plain',
            'etag-def', self.container_policy + 1)

        self.fake_swift.storage_policy[self.container_policy + 1].register(
            'GET', '/v1/a/c/def', swob.HTTPOk,
            {'X-Timestamp': normalize_timestamp(now)},
            "vowelish-superworldly")
        self.fake_swift.storage_policy[self.container_policy + 1].register(
            'DELETE', '/v1/a/c/def', swob.HTTPNoContent, {}, None)
        self.fake_swift.storage_policy[self.container_policy].register(
            'PUT', '/v1/a/c/def', swob.HTTPCreated, {}, None)
        self.reconcile()

        # we didn't forget the policy override anywhere
        self.assertEqual(self.fake_swift.storage_policy[None].calls, [])

        source = self.fake_swift.storage_policy[self.container_policy + 1]
        destination = self.fake_swift.storage_policy[self.container_policy]
        self.assertEqual(
            source.calls,
            [('GET', '/v1/a/c/def'), ('DELETE', '/v1/a/c/def')])
        self.assertEqual(
            destination.calls,
            [('PUT', '/v1/a/c/def')])

        self.assertEqual(source.headers[1]['X-Timestamp'], "1395725943.92441")
        self.assertEqual(destination.headers[0]['X-Timestamp'],
                         "1395725943.92442")

        # make sure we got the body contents right
        _, _, resp_body = self.reconciler.swift.get_object(
            'a', 'c', 'def',
            {'X-Backend-Storage-Policy-Index': self.container_policy})
        self.assertEqual(''.join(resp_body), "vowelish-superworldly")

    def test_early_deletion_of_cleanups(self):
        pass

    def test_source_object_404(self):
        pass

    def test_storage_polices_dont_actually_differ(self):
        # This is just a bit of defensive programming to protect against
        # future changes; there's currently no way (without mocking) to
        # trigger this condition, so we have to introduce an error the hard
        # way.
        now = 1395729638.46292

        def fake_list_misplaced_objects(marker=''):
            if not marker:
                return [('obj', normalize_timestamp(now),
                         'junk', 'junk', 'junk', self.container_policy)]
            else:
                return []

        with mock.patch.object(self.broker, 'list_misplaced_objects',
                               fake_list_misplaced_objects):
            self.reconcile(check_calls=False)

        source = self.fake_swift.storage_policy[self.container_policy + 1]
        destination = self.fake_swift.storage_policy[self.container_policy]
        self.assertEqual(source.calls, [])
        self.assertEqual(destination.calls, [])

    def test_source_timestamp_older(self):
        now = 1395726350.73870
        self.broker.put_object(
            'obj', normalize_timestamp(now - 0.001), 374202518, 'text/plain',
            'd0ed3614637ae52456607488b796db9f', self.container_policy + 1)
        self.fake_swift.storage_policy[self.container_policy + 1].register(
            'GET', '/v1/a/c/obj', swob.HTTPOk,
            {'X-Timestamp': normalize_timestamp(now)},
            "stuffstuff")
        self.reconcile()

        # we didn't forget the policy override anywhere
        self.assertEqual(self.fake_swift.storage_policy[None].calls, [])

        source = self.fake_swift.storage_policy[self.container_policy + 1]
        destination = self.fake_swift.storage_policy[self.container_policy]
        self.assertEqual(source.calls, [('GET', '/v1/a/c/obj')])
        self.assertEqual(destination.calls, [])

    def test_source_timestamp_newer(self):
        now = 1395728503.06582
        self.broker.put_object(
            'obj', normalize_timestamp(now + 0.001), 374202518, 'text/plain',
            'd0ed3614637ae52456607488b796db9f', self.container_policy + 1)
        self.fake_swift.storage_policy[self.container_policy + 1].register(
            'GET', '/v1/a/c/obj', swob.HTTPOk,
            {'X-Timestamp': normalize_timestamp(now)},
            "stuffstuff")
        self.reconcile()

        # we didn't forget the policy override anywhere
        self.assertEqual(self.fake_swift.storage_policy[None].calls, [])

        source = self.fake_swift.storage_policy[self.container_policy + 1]
        destination = self.fake_swift.storage_policy[self.container_policy]
        self.assertEqual(source.calls, [('GET', '/v1/a/c/obj')])
        self.assertEqual(destination.calls, [])

    def test_real_policy_index_differs(self):
        now = 1395726350.73870
        self.broker.put_object(
            'obj', normalize_timestamp(now - 0.001), 374202518, 'text/plain',
            'd0ed3614637ae52456607488b796db9f', self.container_policy + 1)
        self.fake_swift.storage_policy[self.container_policy + 1].register(
            'GET', '/v1/a/c/obj', swob.HTTPOk,
            {'X-Timestamp': normalize_timestamp(now)},
            "stuffstuff")
        self.reconcile(oldest_policy_index=self.container_policy + 99)

        source = self.fake_swift.storage_policy[self.container_policy + 1]
        destination = self.fake_swift.storage_policy[self.container_policy]
        self.assertEqual(source.calls, [])
        self.assertEqual(destination.calls, [])

    def test_real_policy_index_unknown(self):
        now = 1395726350.73870
        self.broker.put_object(
            'obj', normalize_timestamp(now - 0.001), 374202518, 'text/plain',
            'd0ed3614637ae52456607488b796db9f', self.container_policy + 1)
        self.fake_swift.storage_policy[self.container_policy + 1].register(
            'GET', '/v1/a/c/obj', swob.HTTPOk,
            {'X-Timestamp': normalize_timestamp(now)},
            "stuffstuff")
        # None means "couldn't get quorum"
        self.reconcile(oldest_policy_index=None)

        source = self.fake_swift.storage_policy[self.container_policy + 1]
        destination = self.fake_swift.storage_policy[self.container_policy]
        self.assertEqual(source.calls, [])
        self.assertEqual(destination.calls, [])

    def test_turn_cleanups_to_tombstones(self):
        now = 1395760860.59528

        # when an object update is received that changes an object's storage
        # policy index, a cleanup record is created
        self.broker.put_object(
            'o1', normalize_timestamp(now - 0.001), 374202518, 'text/plain',
            'd0ed3614637ae52456607488b796db9f', self.container_policy + 1)
        self.broker.put_object(
            'o1', normalize_timestamp(now), 374202518, 'text/plain',
            'd0ed3614637ae52456607488b796db9f', self.container_policy)

        self.broker.put_object(
            'o2', normalize_timestamp(now - 0.001), 9577670, 'text/plain',
            'ba29745e6dd00b9ab7755b52aff6d611', self.container_policy + 2)
        self.broker.put_object(
            'o2', normalize_timestamp(now), 9577670, 'text/plain',
            'ba29745e6dd00b9ab7755b52aff6d611', self.container_policy)

        self.broker._commit_puts()
        # sanity check
        self.assertEqual(len(self.broker.list_cleanups(999)), 2)

        plusone = self.fake_swift.storage_policy[self.container_policy + 1]
        plustwo = self.fake_swift.storage_policy[self.container_policy + 2]
        plusone.register(
            'DELETE', '/v1/a/c/o1', swob.HTTPNoContent, {}, None)
        plustwo.register(
            'DELETE', '/v1/a/c/o2', swob.HTTPNoContent, {}, None)

        self.reconcile()

        self.assertEqual(plusone.calls, [('DELETE', '/v1/a/c/o1')])
        self.assertEqual(plustwo.calls, [('DELETE', '/v1/a/c/o2')])


class TestReconcilerHelpers(unittest.TestCase):

    def setUp(self):
        self.fake_ring = FakeRing()

    def test_direct_get_oldest_storage_policy_index(self):
        mock_path = 'swift.container.updater.direct_head_container'
        stub_resp_headers = [
            {
                'x-timestamp': '1393542492.31822',
                'x-storage-policy-index': '0',
            },
            {
                'x-timestamp': '1393542493.75106',
                'x-storage-policy-index': '1',
            },
            {
                'x-timestamp': '1393542492.31822',
                'x-storage-policy-index': '0',
            },
        ]
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = \
                container_updater.direct_get_oldest_storage_policy_index(
                    self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 0)

    def test_get_oldest_storage_policy_index_with_error(self):
        mock_path = 'swift.container.updater.direct_head_container'
        stub_resp_headers = [
            {
                'x-timestamp': '1393542492.31822',
                'x-storage-policy-index': '1',
            },
            {
                'x-timestamp': '1393542499.31822',
                'x-storage-policy-index': '0',
            },
            ClientException(
                'Container Server blew up',
                '10.0.0.1', 6001, 'sdb', 404, 'Not Found'
            ),
        ]
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = \
                container_updater.direct_get_oldest_storage_policy_index(
                    self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 1)

    def test_get_oldest_storage_policy_index_with_too_many_errors(self):
        mock_path = 'swift.container.updater.direct_head_container'
        stub_resp_headers = [
            {
                'x-timestamp': '1393542492.31822',
                'x-storage-policy-index': '0',
            },
            ClientException(
                'Container Server blew up',
                '10.0.0.1', 6001, 'sdb', 404, 'Not Found'
            ),
            ClientException(
                'Container Server blew up',
                '10.0.0.12', 6001, 'sdj', 404, 'Not Found'
            ),
        ]
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = \
                container_updater.direct_get_oldest_storage_policy_index(
                    self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, None)


if __name__ == '__main__':
    unittest.main()
