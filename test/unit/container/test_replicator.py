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

import os
import time
import shutil
import itertools
import unittest
import mock
import random

from swift.common import db_replicator
from swift.container import replicator, backend, server
from swift.common.utils import normalize_timestamp
from swift.common.exceptions import StoragePolicyConflict
from swift.common.storage_policy import POLICIES

from test.unit.common import test_db_replicator
from test.unit import patch_policies, mocked_http_conn


class TestReplicator(unittest.TestCase):

    def setUp(self):
        self.orig_ring = replicator.db_replicator.ring.Ring
        replicator.db_replicator.ring.Ring = lambda *args, **kwargs: None

    def tearDown(self):
        replicator.db_replicator.ring.Ring = self.orig_ring

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


class TestReplicatorSync(test_db_replicator.TestReplicatorSync):

    backend = backend.ContainerBroker
    datadir = server.DATADIR
    replicator_daemon = replicator.ContainerReplicator
    replicator_rpc = replicator.ContainerReplicatorRpc

    def test_sync_remote_in_sync(self):
        # setup a local container
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # "replicate" to same database
        node = {'device': 'sdb', 'replication_ip': '127.0.0.1'}
        daemon = replicator.ContainerReplicator({})
        # replicate
        part, node = self._get_broker_part_node(broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        # nothing to do
        self.assertTrue(success)
        self.assertEqual(1, daemon.stats['no_change'])

    def test_sync_remote_with_timings(self):
        # setup a local container
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)
        broker.update_metadata(
            {'x-container-meta-test': ('foo', put_timestamp)})
        # setup remote container
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(time.time(), POLICIES.default.idx)
        timestamp = time.time()
        for db in (broker, remote_broker):
            db.put_object('/a/c/o', timestamp, 0, 'content-type', 'etag',
                          policy_index=db.storage_policy_index)
        # replicate
        daemon = replicator.ContainerReplicator({})
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        with mock.patch.object(db_replicator, 'DEBUG_TIMINGS_THRESHOLD', 0):
            success = daemon._repl_to_node(node, broker, part, info)
        # nothing to do
        self.assertTrue(success)
        self.assertEqual(1, daemon.stats['no_change'])
        expected_timings = ('info', 'update_metadata', 'merge_timestamps',
                            'get_sync', 'merge_syncs')
        debug_lines = self.rpc.logger.logger.get_lines_for_level('debug')
        self.assertEqual(len(expected_timings), len(debug_lines))
        for metric in expected_timings:
            expected = 'replicator-rpc-sync time for %s:' % metric
            self.assert_(any(expected in line for line in debug_lines),
                         'debug timing %r was not in %r' % (
                             expected, debug_lines))

    def test_sync_remote_missing(self):
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)

        # "replicate"
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)

        # complete rsync to all other nodes
        self.assertEqual(2, daemon.stats['rsync'])
        for i in range(1, 3):
            remote_broker = self._get_broker('a', 'c', node_index=i)
            self.assertTrue(os.path.exists(remote_broker.db_file))
            remote_info = remote_broker.get_info()
            local_info = self._get_broker(
                'a', 'c', node_index=0).get_info()
            for k, v in local_info.items():
                if k == 'id':
                    continue
                self.assertEqual(remote_info[k], v,
                                 "mismatch remote %s %r != %r" % (
                                     k, remote_info[k], v))

    def test_rsync_failure(self):
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # "replicate" to different device
        daemon = replicator.ContainerReplicator({})

        def _rsync_file(*args, **kwargs):
            return False
        daemon._rsync_file = _rsync_file

        # replicate
        part, local_node = self._get_broker_part_node(broker)
        node = random.choice([n for n in self._ring.devs if n != local_node])
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertFalse(success)

    def test_sync_remote_missing_most_rows(self):
        put_timestamp = time.time()
        # create "local" broker
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # create "remote" broker
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)
        # add a row to "local" db
        broker.put_object('/a/c/o', time.time(), 0, 'content-type', 'etag',
                          policy_index=broker.storage_policy_index)
        #replicate
        node = {'device': 'sdc', 'replication_ip': '127.0.0.1'}
        daemon = replicator.ContainerReplicator({})

        def _rsync_file(db_file, remote_file, **kwargs):
            remote_server, remote_path = remote_file.split('/', 1)
            dest_path = os.path.join(self.root, remote_path)
            shutil.copy(db_file, dest_path)
            return True
        daemon._rsync_file = _rsync_file
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        # row merge
        self.assertEqual(1, daemon.stats['remote_merge'])
        local_info = self._get_broker(
            'a', 'c', node_index=0).get_info()
        remote_info = self._get_broker(
            'a', 'c', node_index=1).get_info()
        for k, v in local_info.items():
            if k == 'id':
                continue
            self.assertEqual(remote_info[k], v,
                             "mismatch remote %s %r != %r" % (
                                 k, remote_info[k], v))

    def test_sync_remote_missing_one_rows(self):
        put_timestamp = time.time()
        # create "local" broker
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # create "remote" broker
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)
        # add some rows to both db
        for i in range(10):
            put_timestamp = time.time()
            for db in (broker, remote_broker):
                path = '/a/c/o_%s' % i
                db.put_object(path, put_timestamp, 0, 'content-type', 'etag',
                              policy_index=db.storage_policy_index)
        # now a row to the "local" broker only
        broker.put_object('/a/c/o_missing', time.time(), 0,
                          'content-type', 'etag',
                          policy_index=broker.storage_policy_index)
        # replicate
        daemon = replicator.ContainerReplicator({})
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        # row merge
        self.assertEqual(1, daemon.stats['diff'])
        local_info = self._get_broker(
            'a', 'c', node_index=0).get_info()
        remote_info = self._get_broker(
            'a', 'c', node_index=1).get_info()
        for k, v in local_info.items():
            if k == 'id':
                continue
            self.assertEqual(remote_info[k], v,
                             "mismatch remote %s %r != %r" % (
                                 k, remote_info[k], v))

    def test_sync_remote_can_not_keep_up(self):
        put_timestamp = time.time()
        # create "local" broker
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # create "remote" broker
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)
        # add some rows to both db's
        for i in range(10):
            put_timestamp = time.time()
            for db in (broker, remote_broker):
                obj_name = 'o_%s' % i
                db.put_object(obj_name, put_timestamp, 0,
                              'content-type', 'etag',
                              policy_index=db.storage_policy_index)
        # setup REPLICATE callback to simulate adding rows during merge_items
        missing_counter = itertools.count()

        def put_more_objects(op, *args):
            if op != 'merge_items':
                return
            path = '/a/c/o_missing_%s' % missing_counter.next()
            broker.put_object(path, time.time(), 0, 'content-type',
                              'etag', policy_index=db.storage_policy_index)
        test_db_replicator.FakeReplConnection = \
            test_db_replicator.attach_fake_replication_rpc(
                self.rpc, replicate_hook=put_more_objects)
        db_replicator.ReplConnection = test_db_replicator.FakeReplConnection
        # and add one extra to local db to trigger merge_items
        put_more_objects('merge_items')
        # limit number of times we'll call merge_items
        daemon = replicator.ContainerReplicator({'max_diffs': 10})
        # replicate
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertFalse(success)
        # back off on the PUTs during replication...
        FakeReplConnection = test_db_replicator.attach_fake_replication_rpc(
            self.rpc, replicate_hook=None)
        db_replicator.ReplConnection = FakeReplConnection
        # retry replication
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        # row merge
        self.assertEqual(2, daemon.stats['diff'])
        self.assertEqual(1, daemon.stats['diff_capped'])
        local_info = self._get_broker(
            'a', 'c', node_index=0).get_info()
        remote_info = self._get_broker(
            'a', 'c', node_index=1).get_info()
        for k, v in local_info.items():
            if k == 'id':
                continue
            self.assertEqual(remote_info[k], v,
                             "mismatch remote %s %r != %r" % (
                                 k, remote_info[k], v))

    def test_sync_status_change(self):
        # setup a local container
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # setup remote container
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)
        # delete local container
        broker.delete_db(time.time())
        # replicate
        daemon = replicator.ContainerReplicator({})
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        # nothing to do
        self.assertTrue(success)
        self.assertEqual(1, daemon.stats['no_change'])
        # status in sync
        self.assertTrue(remote_broker.is_deleted())
        info = broker.get_info()
        remote_info = remote_broker.get_info()
        self.assert_(float(remote_info['status_changed_at']) >
                     float(remote_info['put_timestamp']),
                     'remote status_changed_at (%s) is not '
                     'greater than put_timestamp (%s)' % (
                         remote_info['status_changed_at'],
                         remote_info['put_timestamp']))
        self.assert_(float(remote_info['status_changed_at']) >
                     float(info['status_changed_at']),
                     'remote status_changed_at (%s) is not '
                     'greater than local status_changed_at (%s)' % (
                         remote_info['status_changed_at'],
                         info['status_changed_at']))

    def _replication_scenarios(self, *scenarios, **kwargs):
        remote_wins = kwargs.get('remote_wins', False)
        # these tests are duplicated because of the differences in replication
        # when row counts cause full rsync vs. merge
        scenarios = scenarios or (
            'no_row', 'local_row', 'remote_row', 'both_rows')
        for scenario_name in scenarios:
            ts = itertools.count(int(time.time()))
            policy = random.choice(POLICIES)
            remote_policy = random.choice(
                [p for p in POLICIES if p is not policy])
            broker = self._get_broker('a', 'c', node_index=0)
            remote_broker = self._get_broker('a', 'c', node_index=1)
            yield ts, policy, remote_policy, broker, remote_broker
            # variations on different replication scenarios
            variations = {
                'no_row': (),
                'local_row': (broker,),
                'remote_row': (remote_broker,),
                'both_rows': (broker, remote_broker),
            }
            dbs = variations[scenario_name]
            obj_ts = ts.next()
            for db in dbs:
                db.put_object('/a/c/o', obj_ts, 0, 'content-type', 'etag',
                              policy_index=db.storage_policy_index)
            # replicate
            part, node = self._get_broker_part_node(broker)
            daemon = self._run_once(node)
            self.assertEqual(0, daemon.stats['failure'])

            # in sync
            local_info = self._get_broker(
                'a', 'c', node_index=0).get_info()
            remote_info = self._get_broker(
                'a', 'c', node_index=1).get_info()
            if remote_wins:
                expected = remote_policy.idx
                err = 'local policy did not change to match remote ' \
                    'for replication row scenario %s' % scenario_name
            else:
                expected = policy.idx
                err = 'local policy changed to match remote ' \
                    'for replication row scenario %s' % scenario_name
            self.assertEqual(local_info['storage_policy_index'], expected, err)
            self.assertEqual(remote_info['storage_policy_index'],
                             local_info['storage_policy_index'])
            self.tearDown()
            self.setUp()

    @patch_policies
    def test_sync_local_create_policy_over_newer_remote_create(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)

    @patch_policies
    def test_sync_local_create_policy_over_newer_remote_delete(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # delete "remote" broker
            remote_broker.delete_db(ts.next())

    @patch_policies
    def test_sync_local_create_policy_over_older_remote_delete(self):
        # remote_row & both_rows cases are covered by
        # "test_sync_remote_half_delete_policy_over_newer_local_create"
        for setup in self._replication_scenarios(
                'no_row', 'local_row'):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # delete older "remote" broker
            remote_broker.delete_db(ts.next())
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)

    @patch_policies
    def test_sync_local_half_delete_policy_over_newer_remote_create(self):
        # no_row & remote_row cases are covered by
        # "test_sync_remote_create_policy_over_older_local_delete"
        for setup in self._replication_scenarios('local_row', 'both_rows'):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # half delete older "local" broker
            broker.delete_db(ts.next())
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)

    @patch_policies
    def test_sync_local_recreate_policy_over_newer_remote_create(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # older recreate "local" broker
            broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)

    @patch_policies
    def test_sync_local_recreate_policy_over_older_remote_create(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # recreate "local" broker
            broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)

    @patch_policies
    def test_sync_local_recreate_policy_over_newer_remote_delete(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # recreate "local" broker
            broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)
            # older delete "remote" broker
            remote_broker.delete_db(ts.next())

    @patch_policies
    def test_sync_local_recreate_policy_over_older_remote_delete(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # older delete "remote" broker
            remote_broker.delete_db(ts.next())
            # recreate "local" broker
            broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)

    @patch_policies
    def test_sync_local_recreate_policy_over_older_remote_recreate(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # older recreate "remote" broker
            remote_broker.delete_db(ts.next())
            remote_recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)
            # recreate "local" broker
            broker.delete_db(ts.next())
            local_recreate_timestamp = ts.next()
            broker.update_put_timestamp(local_recreate_timestamp)
            broker.update_status_changed_at(local_recreate_timestamp)

    @patch_policies
    def test_sync_remote_create_policy_over_newer_local_create(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)

    @patch_policies
    def test_sync_remote_create_policy_over_newer_local_delete(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # delete "local" broker
            broker.delete_db(ts.next())

    @patch_policies
    def test_sync_remote_create_policy_over_older_local_delete(self):
        # local_row & both_rows cases are covered by
        # "test_sync_local_half_delete_policy_over_newer_remote_create"
        for setup in self._replication_scenarios(
                'no_row', 'remote_row', remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # delete older "local" broker
            broker.delete_db(ts.next())
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)

    @patch_policies
    def test_sync_remote_half_delete_policy_over_newer_local_create(self):
        # no_row & both_rows cases are covered by
        # "test_sync_local_create_policy_over_older_remote_delete"
        for setup in self._replication_scenarios('remote_row', 'both_rows',
                                                 remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # half delete older "remote" broker
            remote_broker.delete_db(ts.next())
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)

    @patch_policies
    def test_sync_remote_recreate_policy_over_newer_local_create(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # older recreate "remote" broker
            remote_broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(recreate_timestamp)
            remote_broker.update_status_changed_at(recreate_timestamp)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)

    @patch_policies
    def test_sync_remote_recreate_policy_over_older_local_create(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # recreate "remote" broker
            remote_broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(recreate_timestamp)
            remote_broker.update_status_changed_at(recreate_timestamp)

    @patch_policies
    def test_sync_remote_recreate_policy_over_newer_local_delete(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # recreate "remote" broker
            remote_broker.delete_db(ts.next())
            remote_recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)
            # older delete "local" broker
            broker.delete_db(ts.next())

    @patch_policies
    def test_sync_remote_recreate_policy_over_older_local_delete(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # older delete "local" broker
            broker.delete_db(ts.next())
            # recreate "remote" broker
            remote_broker.delete_db(ts.next())
            remote_recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)

    @patch_policies
    def test_sync_remote_recreate_policy_over_older_local_recreate(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # older recreate "local" broker
            broker.delete_db(ts.next())
            local_recreate_timestamp = ts.next()
            broker.update_put_timestamp(local_recreate_timestamp)
            broker.update_status_changed_at(local_recreate_timestamp)
            # recreate "remote" broker
            remote_broker.delete_db(ts.next())
            remote_recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)

    @patch_policies
    def test_out_of_sync_storage_policy_merge_cleans_up_pending(self):
        ts = itertools.count(int(time.time()))
        policy = random.choice(POLICIES)
        remote_policy = random.choice(
            [p for p in POLICIES if p is not policy])
        broker = self._get_broker('a', 'c', node_index=0)
        remote_broker = self._get_broker('a', 'c', node_index=1)
        # create "local" broker
        broker.initialize(ts.next(), policy.idx)
        broker.put_object('/a/c/o', ts.next(), 0, 'content-type', 'etag',
                          policy_index=broker.storage_policy_index)
        orig_pending_file = broker.pending_file
        self.assertTrue(os.path.exists(orig_pending_file))
        # create "remote" broker
        remote_broker.initialize(ts.next(), remote_policy.idx)
        remote_broker.put_object(
            '/a/c/o', ts.next(), 0, 'content-type', 'etag',
            policy_index=remote_broker.storage_policy_index)
        orig_remote_pending_file = remote_broker.pending_file
        self.assertTrue(os.path.exists(orig_remote_pending_file))

        # replicate
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)

        def _rsync_file(db_file, remote_file, **kwargs):
            remote_server, remote_path = remote_file.split('/', 1)
            dest_path = os.path.join(self.root, remote_path)
            shutil.copy(db_file, dest_path)
            return True
        daemon._rsync_file = _rsync_file
        info = broker.get_replication_info()
        with mocked_http_conn(500):
            success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)

        # in sync
        broker = self._get_broker(
            'a', 'c', node_index=0)
        local_info = broker.get_info()
        remote_broker = self._get_broker(
            'a', 'c', node_index=1)
        remote_info = remote_broker.get_info()
        expected = policy.idx
        self.assertEqual(local_info['storage_policy_index'], expected)
        self.assertEqual(remote_info['storage_policy_index'],
                         local_info['storage_policy_index'])

        self.assertTrue(os.path.exists(orig_pending_file))
        self.assertFalse(os.path.exists(orig_remote_pending_file))

        self.assertEqual(remote_broker.storage_policy_index, policy.idx)
        self.assertRaises(StoragePolicyConflict, remote_broker.put_object,
                          '/a/c/o', ts.next(), 0, 'content-type', 'etag',
                          policy_index=remote_policy.idx)
        remote_broker.put_object(
            '/a/c/o', ts.next(), 0, 'content-type', 'etag',
            policy_index=policy.idx)

    @patch_policies
    def test_out_of_sync_remote_storage_policy_merge_cleans_up_pending(self):
        ts = itertools.count(int(time.time()))
        policy = random.choice(POLICIES)
        remote_policy = random.choice(
            [p for p in POLICIES if p is not policy])
        broker = self._get_broker('a', 'c', node_index=0)
        remote_broker = self._get_broker('a', 'c', node_index=1)
        # create "remote" broker
        remote_broker.initialize(ts.next(), remote_policy.idx)
        remote_broker.put_object(
            'o', ts.next(), 0, 'content-type', 'etag',
            policy_index=remote_broker.storage_policy_index)
        orig_remote_pending_file = remote_broker.pending_file
        self.assertTrue(os.path.exists(orig_remote_pending_file))
        # create "local" broker
        broker.initialize(ts.next(), policy.idx)
        broker.put_object('o1', ts.next(), 0, 'content-type', 'etag',
                          policy_index=broker.storage_policy_index)
        broker.put_object('o2', ts.next(), 0, 'content-type', 'etag',
                          policy_index=broker.storage_policy_index)
        orig_pending_file = broker.pending_file
        self.assertTrue(os.path.exists(orig_pending_file))

        # replicate
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)

        self.assertEqual(daemon.stats['success'], 4)

        # in sync
        broker = self._get_broker('a', 'c', node_index=0)
        local_info = broker.get_info()
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_info = remote_broker.get_info()
        expected = remote_policy.idx
        self.assertEqual(local_info['storage_policy_index'], expected)
        self.assertEqual(remote_info['storage_policy_index'],
                         local_info['storage_policy_index'])

        self.assertTrue(os.path.exists(orig_remote_pending_file))
        self.assertEqual(os.path.getsize(orig_remote_pending_file), 0)
        self.assertFalse(os.path.exists(orig_pending_file))

        self.assertRaises(StoragePolicyConflict, broker.put_object,
                          '/a/c/o', ts.next(), 0, 'content-type', 'etag',
                          policy_index=policy.idx)
        broker.put_object(
            '/a/c/o', ts.next(), 0, 'content-type', 'etag',
            policy_index=remote_policy.idx)


if __name__ == '__main__':
    unittest.main()
