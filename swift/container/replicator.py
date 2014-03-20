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
from swift.common import db_replicator
from swift.common.http import is_success
from swift.common.storage_policy import POLICIES
from swift.container.backend import ContainerBroker, DATADIR


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

    def should_delete_db(self, broker, responses, shouldbehere):
        """
        Returns whether or not to keep this DB on this system after replicating
        it to all (other) primary nodes.

        If a DB has cleanup records in it, we keep it.
        """
        drop = super(ContainerReplicator, self).should_delete_db(
            broker, responses, shouldbehere)
        if not drop:
            # skip the expensive check if possible
            return drop
        else:
            cleanups = broker.list_cleanups(limit=1)
            return not any(cleanups)

    def sync_args_from_replication_info(self, replication_info):
        sync_args = super(ContainerReplicator, self).\
            sync_args_from_replication_info(replication_info)
        # If we only have one storage policy, we're in a cluster that isn't
        # using storage policies yet, so we don't send our policy index. The
        # other end is either running new code that will infer a 0 from the
        # policy index's absence *or* is running old code that will blow up if
        # fed a policy index.
        #
        # The point is to keep container replication working during an upgrade
        # from pre-storage-policy code to post-storage-policy code.
        if len(POLICIES) > 1:
            sync_args.append(replication_info['storage_policy_index'])
        return sync_args

    def _handle_sync_response(self, node, response, info, broker, http):
        """
        Handle a sync response from a remote node. If this container needs to
        change its storage policy index, do so. Otherwise, send the necessary
        rows to the remote node.
        """
        if is_success(response.status):
            rinfo = json.loads(response.data)
            local_policy = info['storage_policy_index']
            local_created_at = info['created_at']
            remote_policy = rinfo['storage_policy_index']
            remote_created_at = rinfo['created_at']
            # The oldest policy index wins. In the event of a timestamp tie,
            # the numerically-lesser storage policy wins.
            if ((local_policy != remote_policy) and
                ((local_created_at > remote_created_at) or
                    (local_created_at == remote_created_at and
                     local_policy > remote_policy))):
                broker.set_storage_policy_index(remote_policy)
        return super(ContainerReplicator, self)._handle_sync_response(
            node, response, info, broker, http)
