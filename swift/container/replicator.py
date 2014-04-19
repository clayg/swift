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
import itertools
import time

from swift.container.reconciler import (
    MISPLACED_OBJECTS_ACCOUNT, incorrect_policy_index,
    get_reconciler_container_name, get_row_to_q_entry_translater)
from swift.container.backend import ContainerBroker, DATADIR
from swift.common import db_replicator
from swift.common.http import is_success
from swift.common.storage_policy import POLICIES
from swift.common.db import DatabaseAlreadyExists
from swift.common.utils import (json, normalize_timestamp, hash_path,
                                storage_directory)


class ContainerReplicator(db_replicator.Replicator):
    server_type = 'container'
    brokerclass = ContainerBroker
    datadir = DATADIR
    default_port = 6001

    def report_up_to_date(self, full_info):
        for key in ('put_timestamp', 'delete_timestamp', 'object_count',
                    'bytes_used'):
            if full_info['reported_' + key] != full_info[key]:
                return False
        return True

    def _gather_sync_args(self, replication_info):
        parent = super(ContainerReplicator, self)
        sync_args = parent._gather_sync_args(replication_info)
        if len(POLICIES) > 1:
            sync_args += tuple(replication_info[k] for k in
                               ('status_changed_at', 'count',
                                'storage_policy_index'))
        return sync_args

    def _handle_sync_response(self, node, response, info, broker, http):
        parent = super(ContainerReplicator, self)
        if is_success(response.status):
            remote_info = json.loads(response.data)
            if incorrect_policy_index(info, remote_info):
                status_changed_at = normalize_timestamp(time.time())
                broker.set_storage_policy_index(
                    remote_info['storage_policy_index'],
                    timestamp=status_changed_at)
            broker.merge_timestamps(*(remote_info[key] for key in (
                'created_at', 'put_timestamp', 'delete_timestamp')))
        rv = parent._handle_sync_response(
            node, response, info, broker, http)
        return rv

    def get_reconciler_broker(self, timestamp):
        account = MISPLACED_OBJECTS_ACCOUNT
        container = get_reconciler_container_name(timestamp)
        hsh = hash_path(account, container)
        part = self.ring.get_part(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)

        nodes = self.ring.get_part_nodes(part)
        more_nodes = self.ring.get_more_nodes(part)

        for node in itertools.chain(nodes, more_nodes):
            if node['id'] in self._local_device_ids:
                break
        else:
            raise Exception("we're screwed")

        db_path = os.path.join(self.root, node['device'], db_dir, hsh + '.db')
        broker = ContainerBroker(db_path, account=account, container=container)
        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(timestamp, 0)
            except DatabaseAlreadyExists:
                pass
        return part, broker, node['id']

    def dump_rows_to_reconciler(self, broker, info, remote_info):
        misplaced = self.list_misplaced_objects(self.per_diff)
        # need an earlier return with triggers or some shit, I wanna do a
        # policy_stat table like the accounts for the container
        if not misplaced:
            return
        translater = get_row_to_q_entry_translater(broker)
        while misplaced:
            item_list = map(translater, misplaced)
            timestamp = item_list[0]['created_at']
            part, reconciler, node_id, = self.get_reconciler_broker(timestamp)
            self.logger.info('Adding %d objects to the reconciler at %s' %
                             (len(item_list), reconciler.db_file))
            reconciler.merge_items(item_list)
            misplaced = self.list_misplaced_objects(
                self.per_diff, marker=item_list['name'])
        self._replicate_object(part, reconciler.db_file, node_id)

    def _post_replicate_hook(self, broker):
        translater = get_row_to_q_entry_translater(broker)
        item_list = map(translater, broker.iter_misplaced_objects())
        if not item_list:
            return
        timestamp = item_list[0]['created_at']
        part, reconciler, node_id, = self.get_reconciler_broker(timestamp)
        self.logger.info('Adding %d objects to the reconciler at %s' %
                         (len(item_list), reconciler.db_file))
        reconciler.merge_items(item_list)
        self._replicate_object(part, reconciler.db_file, node_id)


class ContainerReplicatorRpc(db_replicator.ReplicatorRpc):

    def _parse_sync_args(self, args):
        parent = super(ContainerReplicatorRpc, self)
        remote_info = parent._parse_sync_args(args)
        if len(args) > 9:
            remote_info['status_changed_at'] = args[7]
            remote_info['count'] = args[8]
            remote_info['storage_policy_index'] = args[9]
        return remote_info

    def _handle_sync_request(self, broker, remote_info):
        if incorrect_policy_index(broker.get_info(), remote_info):
            status_changed_at = normalize_timestamp(time.time())
            broker.set_storage_policy_index(
                remote_info['storage_policy_index'],
                timestamp=status_changed_at)
        parent = super(ContainerReplicatorRpc, self)
        rv = parent._handle_sync_request(broker, remote_info)
        return rv
