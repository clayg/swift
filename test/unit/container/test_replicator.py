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

import mock
import os
import unittest
from shutil import rmtree
from swift.container import backend, replicator
from swift.common.utils import normalize_timestamp
from tempfile import mkdtemp
from test.unit import FakeLogger, FakeRing


class FakeContainerReplicator(replicator.ContainerReplicator):
    def __init__(self, conf):
        super(FakeContainerReplicator, self).__init__(conf)
        self.logger = FakeLogger()
        self.ring = FakeRing()

    def _repl_to_node(self, node, broker, partition, info):
        return True


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


if __name__ == '__main__':
    unittest.main()
