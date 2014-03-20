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

import json
import mock
import os
import time
import unittest
from ConfigParser import ConfigParser
from tempfile import mkdtemp
from test.unit import patch_policies, FakeLogger, FakeRing
from shutil import rmtree
from swift.container import backend, replicator, server
from swift.common.utils import normalize_timestamp
from swift.common.storage_policy import StoragePolicy, parse_storage_policies


class FakeContainerReplicator(replicator.ContainerReplicator):
    def __init__(self, conf):
        super(FakeContainerReplicator, self).__init__(conf)
        self.logger = FakeLogger()
        self.ring = FakeRing()

    def _repl_to_node(self, node, broker, partition, info):
        return True


class FakeReplConnection(object):
    replicated = False
    host = 'localhost'

    def __init__(self, set_status=200, response_body=''):
        self.set_status = set_status
        self.response_body = response_body

    def replicate(self, *args):
        self.replicated = True

        class Response(object):
            status = self.set_status
            data = self.response_body

            def read(innerself):
                return self.response_body
        return Response()


class TestReplicator(unittest.TestCase):

    def setUp(self):
        self.orig_ring = replicator.db_replicator.ring.Ring
        replicator.db_replicator.ring.Ring = lambda *args, **kwargs: None
        self.testdir = mkdtemp()

    def tearDown(self):
        replicator.db_replicator.ring.Ring = self.orig_ring
        rmtree(self.testdir)

    def test_report_up_to_date(self):
        repl = replicator.ContainerReplicator({})
        info = {'put_timestamp': normalize_timestamp(1),
                'delete_timestamp': normalize_timestamp(0),
                'object_count': 0,
                'bytes_used': 0,
                'reported_put_timestamp': normalize_timestamp(1),
                'reported_delete_timestamp': normalize_timestamp(0),
                'reported_object_count': 0,
                'reported_bytes_used': 0}
        self.assertTrue(repl.report_up_to_date(info))
        info['delete_timestamp'] = normalize_timestamp(2)
        self.assertFalse(repl.report_up_to_date(info))
        info['reported_delete_timestamp'] = normalize_timestamp(2)
        self.assertTrue(repl.report_up_to_date(info))
        info['object_count'] = 1
        self.assertFalse(repl.report_up_to_date(info))
        info['reported_object_count'] = 1
        self.assertTrue(repl.report_up_to_date(info))
        info['bytes_used'] = 1
        self.assertFalse(repl.report_up_to_date(info))
        info['reported_bytes_used'] = 1
        self.assertTrue(repl.report_up_to_date(info))
        info['put_timestamp'] = normalize_timestamp(3)
        self.assertFalse(repl.report_up_to_date(info))
        info['reported_put_timestamp'] = normalize_timestamp(3)
        self.assertTrue(repl.report_up_to_date(info))

    def test_replicate_container_out_of_place_with_cleanups(self):
        db_path = os.path.join(self.testdir, 'replicate-container-test')
        broker = backend.ContainerBroker(db_path, account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        replicator = FakeContainerReplicator({})

        # Correct node_id, wrong part, no cleanups --> gets removed
        part = replicator.ring.get_part('acc', 'con') + 1
        node_id = replicator.ring.get_part_nodes(part)[0]['id']

        with mock.patch.object(replicator, 'delete_db') as delete_db_mock:
            replicator._replicate_object(str(part), db_path, node_id)
        self.assertEqual(delete_db_mock.mock_calls, [mock.call(db_path)])

        # Now add a cleanup record by changing an object's storage policy
        broker.put_object('obj', normalize_timestamp(100), 77,
                          'text/plain', 'etag', 123)
        broker.put_object('obj', normalize_timestamp(101), 77,
                          'text/plain', 'etag', 0)
        broker._commit_puts()
        self.assertTrue(len(broker.list_cleanups(999)) > 0)  # sanity check
        with mock.patch.object(replicator, 'delete_db') as delete_db_mock:
            replicator._replicate_object(str(part), db_path, node_id)
        self.assertEqual(delete_db_mock.mock_calls, [])

    def test_sync_args(self):
        repl = replicator.ContainerReplicator({})
        replication_info = {
            'max_row': '_max_row', 'hash': '_hash', 'id': '_id',
            'created_at': '_created_at', 'put_timestamp': '_put_timestamp',
            'delete_timestamp': '_delete_timestamp', 'metadata': '_metadata',
            'storage_policy_index': '_storage_policy_index',
            'otherstuff': '_otherstuff'}

        # simulate cluster without storage policies
        pols = parse_storage_policies(ConfigParser())
        with patch_policies(pols):
            sync_args = repl.sync_args_from_replication_info(replication_info)
            self.assertEqual(sync_args, [
                '_max_row', '_hash', '_id', '_created_at', '_put_timestamp',
                '_delete_timestamp', '_metadata'])

        # with some storage policies defined, we send the policy index
        pols = [StoragePolicy(0, 'zero'),
                StoragePolicy(1, 'one', is_default=True)]
        with patch_policies(pols):
            sync_args = repl.sync_args_from_replication_info(replication_info)
            self.assertEqual(sync_args, [
                '_max_row', '_hash', '_id', '_created_at', '_put_timestamp',
                '_delete_timestamp', '_metadata', '_storage_policy_index'])

    def test_sync_different_storage_policy_when_local_is_newer(self):
        # If the local one is newer, we need update its storage policy
        broker = backend.ContainerBroker(':memory:', logger=FakeLogger(),
                                         account='a', container='c')
        broker.initialize(normalize_timestamp(time.time()), 1212)
        broker.put_object('o1', normalize_timestamp(1100), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1212)
        broker.put_object('o2', normalize_timestamp(1200), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1212)

        repl = replicator.ContainerReplicator({})
        repl.ring = 'fake container ring'
        remote_response = {'hash': '_hash', 'id': '_id',
                           'point': 999999999999999,
                           'put_timestamp': '_put_timestamp',
                           'delete_timestamp': '_delete_timestamp',
                           'object_count': '_object_count',
                           'created_at': normalize_timestamp(time.time() - 99),
                           'storage_policy_index': 2323}

        fake_repl_conn = FakeReplConnection(200, json.dumps(remote_response))

        with mock.patch('swift.common.db_replicator.ReplConnection',
                        lambda *a, **kw: fake_repl_conn):
            retval = repl._repl_to_node(
                {'a': 'ring node'}, broker,
                'partition', broker.get_replication_info())
        self.assertTrue(retval)
        self.assertEqual(2323, broker.get_info()['storage_policy_index'])

    def test_sync_different_storage_policy_when_local_is_older(self):
        # If the local one is older, don't change anything.
        broker = backend.ContainerBroker(':memory:', logger=FakeLogger(),
                                         account='a', container='c')
        broker.initialize(normalize_timestamp(time.time()), 1212)
        broker.put_object('o1', normalize_timestamp(1100), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1212)
        broker.put_object('o2', normalize_timestamp(1200), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1212)

        repl = replicator.ContainerReplicator({})
        repl.ring = 'fake container ring'
        remote_response = {'hash': '_hash', 'id': '_id',
                           'point': 999999999999999,
                           'put_timestamp': '_put_timestamp',
                           'delete_timestamp': '_delete_timestamp',
                           'object_count': '_object_count',
                           'created_at': normalize_timestamp(time.time() + 99),
                           'storage_policy_index': 2323}

        fake_repl_conn = FakeReplConnection(200, json.dumps(remote_response))

        with mock.patch('swift.common.db_replicator.ReplConnection',
                        lambda *a, **kw: fake_repl_conn):
            retval = repl._repl_to_node(
                {'a': 'ring node'}, broker,
                'partition', broker.get_replication_info())
        self.assertTrue(retval)
        self.assertEqual(1212, broker.get_info()['storage_policy_index'])

    def test_sync_different_storage_policy_same_timestamp(self):
        # If the timestamps match exactly, we've got to have some way of
        # breaking ties, and this is one of them.
        now = time.time()
        broker = backend.ContainerBroker(':memory:', logger=FakeLogger(),
                                         account='a', container='c')
        broker.initialize(normalize_timestamp(now), 1212)
        broker.put_object('o1', normalize_timestamp(now), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1212)
        broker.put_object('o2', normalize_timestamp(now), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1212)

        repl = replicator.ContainerReplicator({})
        repl.ring = 'fake container ring'
        remote_response = {'hash': '_hash', 'id': '_id',
                           'point': 999999999999999,
                           'put_timestamp': '_put_timestamp',
                           'delete_timestamp': '_delete_timestamp',
                           'object_count': '_object_count',
                           'created_at': normalize_timestamp(now),
                           'storage_policy_index': 444}

        fake_repl_conn = FakeReplConnection(200, json.dumps(remote_response))

        with mock.patch('swift.common.db_replicator.ReplConnection',
                        lambda *a, **kw: fake_repl_conn):
            retval = repl._repl_to_node(
                {'a': 'ring node'}, broker,
                'partition', broker.get_replication_info())
        self.assertTrue(retval)
        self.assertEqual(444, broker.get_info()['storage_policy_index'])


class TestReplicatorRpc(unittest.TestCase):
    def setUp(self):
        self.testdir = mkdtemp()
        self.replicator_rpc = server.ContainerReplicatorRpc(
            self.testdir, 'containers', backend.ContainerBroker,
            mount_check=False, logger=FakeLogger())

    def tearDown(self):
        rmtree(self.testdir)

    def test_sync_older_different_storage_policy(self):
        broker = backend.ContainerBroker(':memory:', account='a',
                                         container='c')
        broker.initialize(normalize_timestamp('9000'), 16)

        sync_args = ['_max_row', '_hash', '_id', normalize_timestamp('8999'),
                     '_put_timestamp', '_delete_timestamp', '{}', 12]

        response = self.replicator_rpc.sync(broker, sync_args)
        self.assertEqual(response.status_int, 200)  # sanity check
        self.assertEqual(12, broker.get_info()['storage_policy_index'])

    def test_sync_newer_different_storage_policy(self):
        broker = backend.ContainerBroker(':memory:', account='a',
                                         container='c')
        broker.initialize(normalize_timestamp('9000'), 16)

        sync_args = ['_max_row', '_hash', '_id', normalize_timestamp('9001'),
                     '_put_timestamp', '_delete_timestamp', '{}', 12]

        response = self.replicator_rpc.sync(broker, sync_args)
        self.assertEqual(response.status_int, 200)  # sanity check
        self.assertEqual(12, broker.get_info()['storage_policy_index'])

    def test_sync_different_storage_policy_same_timestamp(self):
        # As a tiebreaker, the lesser policy index wins.
        broker = backend.ContainerBroker(':memory:', account='a',
                                         container='c')
        broker.initialize(normalize_timestamp('9000'), 16)

        sync_args = ['_max_row', '_hash', '_id', normalize_timestamp('9000'),
                     '_put_timestamp', '_delete_timestamp', '{}', 12]

        response = self.replicator_rpc.sync(broker, sync_args)
        self.assertEqual(response.status_int, 200)  # sanity check
        self.assertEqual(12, broker.get_info()['storage_policy_index'])


if __name__ == '__main__':
    unittest.main()
