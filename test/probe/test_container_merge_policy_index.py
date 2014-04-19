#!/usr/bin/python -u
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

import sys
import unittest
import uuid
import itertools
import time

from swift.common.manager import Manager
from swift.common.storage_policy import POLICIES, POLICY_INDEX
from swift.common import direct_client
from swift.common.http import HTTP_NOT_FOUND
from test.probe.common import reset_environment, get_to_final_state


from swiftclient import client, get_auth, ClientException

TIMEOUT = 60


class BrainSpliter(object):

    def __init__(self, url, token, container_name='test', object_name='test'):
        self.url = url
        self.token = token
        self.container_name = container_name
        self.object_name = object_name
        self.servers = Manager(['container-server'])
        self.policy = itertools.cycle(POLICIES)

    def start_first_half(self):
        tuple(self.servers.start(number=n) for n in (1, 2))

    def stop_first_half(self):
        tuple(self.servers.stop(number=n) for n in (1, 2))

    def start_second_half(self):
        tuple(self.servers.start(number=n) for n in (3, 4))

    def stop_second_half(self):
        tuple(self.servers.stop(number=n) for n in (3, 4))

    def put_container(self):
        next_policy = self.policy.next()
        headers = {'X-Storage-Policy': next_policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

    def delete_container(self):
        client.delete_container(self.url, self.token, self.container_name)

    def put_object(self):
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name)

    def delete_object(self):
        client.delete_object(self.url, self.token, self.container_name,
                             self.object_name)


class TestContainerMergePolicyIndex(unittest.TestCase):

    def setUp(self):
        if len(POLICIES) < 2:
            raise unittest.SkipTest()
        (self.pids, self.port2server, self.account_ring, self.container_ring,
         self.object_ring, self.url, self.token,
         self.account, self.configs) = reset_environment()
        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()
        self.brain = BrainSpliter(self.url, self.token,
                                  self.container_name, self.object_name)

    def test_merge_storage_policy_index(self):
        # generic split brain
        self.brain.stop_first_half()
        self.brain.put_container()
        self.brain.start_first_half()
        self.brain.stop_second_half()
        self.brain.put_container()
        self.brain.put_object()
        self.brain.start_second_half()
        # make sure we have some mannor of split brain
        container_part, container_nodes = self.container_ring.get_nodes(
            self.account, self.container_name)
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        found_policy_indexes = set(metadata['x-storage-policy-index'] for
                                   node, metadata in head_responses)
        self.assert_(len(found_policy_indexes) > 1,
                     'primary nodes did not disagree about policy index %r' %
                     head_responses)
        # find our object
        orig_policy_index = None
        for policy_index in found_policy_indexes:
            object_ring = POLICIES.get_object_ring(policy_index, '/etc/swift')
            part, nodes = object_ring.get_nodes(
                self.account, self.container_name, self.object_name)
            for node in nodes:
                try:
                    direct_client.direct_head_object(
                        node, part, self.account, self.container_name,
                        self.object_name, headers={POLICY_INDEX: policy_index})
                except direct_client.ClientException as err:
                    continue
                orig_policy_index = policy_index
                break
            if orig_policy_index is not None:
                break
        else:
            self.fail('Unable to find /%s/%s/%s in %r' % (
                self.account, self.container_name, self.object_name,
                found_policy_indexes))
        get_to_final_state()
        Manager(['container-updater', 'container-reconciler']).once()
        # validate containers
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        found_policy_indexes = set(metadata['x-storage-policy-index'] for
                                   node, metadata in head_responses)
        self.assert_(len(found_policy_indexes) == 1,
                     'primary nodes disagree about policy index %r' %
                     head_responses)
        expected_policy_index = found_policy_indexes.pop()
        self.assertNotEqual(orig_policy_index, expected_policy_index)
        # validate object placement
        orig_policy_ring = POLICIES.get_object_ring(orig_policy_index,
                                                    '/etc/swift')
        for node in orig_policy_ring.devs:
            try:
                direct_client.direct_head_object(
                    node, part, self.account, self.container_name,
                    self.object_name, headers={
                        POLICY_INDEX: orig_policy_index})
            except direct_client.ClientException as err:
                if err.http_status == HTTP_NOT_FOUND:
                    continue
                raise
            else:
                self.fail('Found /%s/%s/%s in %s' % (
                    self.account, self.container_name, self.object_name,
                    orig_policy_index))
        # use proxy to access object (bad container info might be cached...)
        timeout = time.time() + TIMEOUT
        while time.time() < timeout:
            try:
                metadata = client.head_object(self.url, self.token,
                                              self.container_name,
                                              self.object_name)
            except ClientException as err:
                if err.http_status != HTTP_NOT_FOUND:
                    raise
                time.sleep(1)
            else:
                break
        else:
            self.fail('could not HEAD /%s/%s/%s/ from policy %s '
                      'after %s seconds.' % (
                          self.account, self.container_name, self.object_name,
                          expected_policy_index, TIMEOUT))


if __name__ == "__main__":
    if '--setup-manual-split-brain' in sys.argv:
        url, token = get_auth('http://127.0.0.1:8080/auth/v1.0',
                              'test:tester', 'testing')
        brain = BrainSpliter(url, token, 'test')
        for command in sys.argv[2:]:
            method = getattr(brain, command)
            try:
                method()
            except client.ClientException as e:
                print e
        print 'STATUS'.join(['*' * 25] * 2)
        brain.servers.status()
        sys.exit()
    unittest.main()
