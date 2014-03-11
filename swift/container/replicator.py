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

from swift.container.backend import ContainerBroker, DATADIR
from swift.common import db_replicator


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
