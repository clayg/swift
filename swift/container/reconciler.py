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

import time
from collections import defaultdict
import socket
import itertools
import logging

from eventlet import GreenPile, GreenPool, Timeout

from swift.common import constraints
from swift.common.daemon import Daemon
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.utils import get_logger, split_path, quorum_size, \
    FileLikeIter, normalize_timestamp, last_modified_date_to_timestamp
from swift.common.direct_client import (
    direct_head_container, direct_delete_container_object,
    direct_put_container_object, direct_delete_object, ClientException)


MISPLACED_OBJECTS_ACCOUNT = '.misplaced_objects'
MISPLACED_OBJECTS_CONTAINER_DIVISOR = 3600  # 1 hour


def incorrect_policy_index(info, remote_info):
    """
    Compare remote_info to info and decide if the remote storage policy index
    should be used instead of ours.
    """
    if 'storage_policy_index' not in remote_info:
        return False
    if remote_info['storage_policy_index'] == \
            info['storage_policy_index']:
        return False

    def is_deleted(info):
        return (info['delete_timestamp'] > info['put_timestamp'] and
                info.get('count', info.get('object_count', 0)) == 0)

    if is_deleted(remote_info):
        if is_deleted(info):
            return (
                info['status_changed_at'] < remote_info['status_changed_at'])
        return False
    if is_deleted(info):
        return True

    def has_been_recreated(info):
        return (info['put_timestamp'] > info['delete_timestamp'] >
                normalize_timestamp(0))

    remote_recreated = has_been_recreated(remote_info)
    recreated = has_been_recreated(info)
    if remote_recreated and (
            remote_info['status_changed_at'] > info['status_changed_at']
            or not recreated):
        return True
    elif not recreated and (
            remote_info['status_changed_at'] < info['status_changed_at']):
        return True
    return False


def get_reconciler_container_name(obj_timestamp):
    return str(int(float(obj_timestamp)) //
               MISPLACED_OBJECTS_CONTAINER_DIVISOR *
               MISPLACED_OBJECTS_CONTAINER_DIVISOR)


def get_reconciler_obj_name(policy_index, account, container, obj):
    return "%(policy_index)d:/%(acc)s/%(con)s/%(obj)s" % {
        'policy_index': policy_index, 'acc': account,
        'con': container, 'obj': obj}


def get_reconciler_content_type(op):
    try:
        return {
            'put': 'application/x-put',
            'delete': 'application/x-delete',
        }[op.lower()]
    except KeyError:
        raise ValueError('invalid operation type %r' % op)


def get_row_to_q_entry_translater(broker):
    account = broker.account
    container = broker.container
    policy_index = broker.storage_policy_index
    op_type = {
        0: get_reconciler_content_type('put'),
        1: get_reconciler_content_type('delete'),
    }

    def translater(obj_info):
        name = get_reconciler_obj_name(policy_index, account, container,
                                       obj_info['name'])
        return {
            'name': name,
            'deleted': 0,
            'created_at': obj_info['created_at'],
            'etag': obj_info['created_at'],
            'content_type': op_type[obj_info['deleted']],
            'size': 0,
        }
    return translater


def add_to_reconciler_queue(container_ring, account, container, obj,
                            obj_policy_index, obj_timestamp, op,
                            force=False, conn_timeout=5, response_timeout=15):
    """
    Add an object to the container reconciler's queue. This will cause the
    container reconciler to move it from its current storage policy index to
    the correct storage policy index.

    :param container_ring: container ring
    :param account: the misplaced object's account
    :param container: the misplaced object's container
    :param obj: the misplaced object
    :param obj_policy_index: the policy index where the misplaced object
                             currently is
    :param obj_timestamp: the misplaced object's X-Timestamp. We need this to
                          ensure that the reconciler doesn't overwrite a newer
                          object with an older one.
    :param op: the method of the operation (DELETE or PUT)
    :param force: over-write queue entries newer than obj_timestamp
    :param conn_timeout: max time to wait for connection to container server
    :param response_timeout: max time to wait for response from container
                             server

    :returns: .misplaced_object container name, False on failure. "Success"
              means a quorum of containers got the update.
    """
    container_name = get_reconciler_container_name(obj_timestamp)
    object_name = get_reconciler_obj_name(obj_policy_index, account,
                                          container, obj)
    if force:
        # this allows an operator to re-enqueue an object that has already
        # been popped from the queue to be reprocessed, but could potentailly
        # prevent out of order updates from making it into the queue
        x_timestamp = normalize_timestamp(time.time())
    else:
        x_timestamp = obj_timestamp
    q_op_type = get_reconciler_content_type(op)
    headers = {
        'X-Size': 0,
        'X-Etag': obj_timestamp,
        'X-Timestamp': x_timestamp,
        'X-Content-Type': q_op_type,
    }

    def _check_success(*args, **kwargs):
        try:
            direct_put_container_object(*args, **kwargs)
            return 1
        except (ClientException, Timeout, socket.error):
            return 0

    pile = GreenPile()
    part, nodes = container_ring.get_nodes(MISPLACED_OBJECTS_ACCOUNT,
                                           container_name)
    for node in nodes:
        pile.spawn(_check_success, node, part, MISPLACED_OBJECTS_ACCOUNT,
                   container_name, object_name, headers=headers,
                   conn_timeout=conn_timeout,
                   response_timeout=response_timeout)

    successes = sum(pile)
    if successes >= quorum_size(len(nodes)):
        return container_name
    else:
        return False


def slightly_later_timestamp(ts, offset=1):
    # I'm guessing to avoid rounding errors Swift uses a 10-microsecond
    # resolution instead of Python's 1-microsecond resolution.
    offset *= 0.00001
    return normalize_timestamp(float(ts) + offset)


def parse_raw_obj(obj_info):
    raw_obj_name = obj_info['name'].encode('utf-8')

    policy_index, obj_name = raw_obj_name.split(':', 1)
    q_policy_index = int(policy_index)
    account, container, obj = split_path(obj_name, 3, 3, rest_with_last=True)
    try:
        q_op = {
            'application/x-put': 'PUT',
            'application/x-delete': 'DELETE',
        }[obj_info['content_type']]
    except KeyError:
        raise ValueError('invalid operation type %r' %
                         obj_info.get('content_type', None))
    return {
        'q_policy_index': q_policy_index,
        'account': account,
        'container': container,
        'obj': obj,
        'q_op': q_op,
        'q_timestamp': float(obj_info['hash']),
        'q_record': last_modified_date_to_timestamp(
            obj_info['last_modified']),
        'path': '/%s/%s/%s' % (account, container, obj)
    }


def translate_container_headers_to_info(headers):
    default_timestamp = normalize_timestamp(0)
    return {
        'storage_policy_index': int(headers['x-storage-policy-index']),
        'put_timestamp': headers.get('x-put-timestamp', default_timestamp),
        'delete_timestamp': headers.get('x-backend-delete-timestamp',
                                        default_timestamp),
        'status_changed_at': headers.get('x-backend-status-changed-at',
                                         default_timestamp),
    }


def direct_get_container_policy_index(container_ring, account_name,
                                      container_name):
    """
    Talk directly to the primary container servers to figure out the storage
    policy index for a given container.

    :param container_ring: ring in which to look up the container locations
    :param account_name: name of the container's account
    :param container_name: name of the container
    :returns: storage policy index, or None if it couldn't get a quorum
    """
    def _eat_client_exception(*args):
        try:
            return direct_head_container(*args)
        except ClientException as err:
            if err.http_status == 404:
                return err.http_headers
        except (Timeout, socket.error):
            pass

    pile = GreenPile()
    part, nodes = container_ring.get_nodes(account_name, container_name)
    for node in nodes:
        pile.spawn(_eat_client_exception, node, part, account_name,
                   container_name)

    headers = [x for x in pile if x is not None]
    if len(headers) < quorum_size(len(nodes)):
        return
    container_info = map(translate_container_headers_to_info, headers)
    container_info.sort(
        cmp=lambda x, y: 1 if incorrect_policy_index(x, y) else -1)
    return int(container_info[0]['storage_policy_index'])


def direct_delete_container_entry(container_ring, account_name, container_name,
                                  object_name, headers=None):
    """
    Talk directly to the primary container servers to delete a particular
    object listing. Does not talk to object servers; use this only when a
    container entry does not actually have a corresponding object.
    """
    pool = GreenPool()
    part, nodes = container_ring.get_nodes(account_name, container_name)
    for node in nodes:
        pool.spawn_n(direct_delete_container_object, node, part, account_name,
                     container_name, object_name, headers=headers)

    # This either worked or it didn't; if it didn't, we'll retry on the next
    # reconciler loop when we see the queue entry again.
    pool.waitall()


def direct_cleanup_object(object_ring, account_name, container_name,
                          object_name, headers=None):
    """
    Talk directly to the primary object servers to delete a particular object.
    Does not talk to container servers; use this only when a container entry
    does not actually need a corresponding object update.

    :returns: success if the majority of nodes accepted the DELETE
    """
    def _eat_client_exception(*args, **kwargs):
        success = False
        try:
            direct_delete_object(*args, **kwargs)
        except ClientException as e:
            if e.http_status in (404, 409):
                success = True
        except (Timeout, socket.error):
            pass
        else:
            success = True
        return success

    pile = GreenPile()
    part, nodes = object_ring.get_nodes(
        account_name, container_name, object_name)
    for node in nodes:
        pile.spawn(_eat_client_exception, node, part,
                   account_name, container_name, object_name,
                   headers=headers)

    return list(pile).count(True) > quorum_size(len(nodes))


class ContainerReconciler(Daemon):
    """
    Move objects that are in the wrong storage policy.
    """

    def __init__(self, conf):
        self.conf = conf
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))
        self.interval = int(conf.get('interval', 30))
        conf_path = conf.get('__file__') or \
            '/etc/swift/container-reconciler.conf'
        self.logger = get_logger(conf, log_route='container-reconciler')
        request_tries = int(conf.get('request_tries') or 3)
        self.swift = InternalClient(conf_path,
                                    'Swift Container Reconciler',
                                    request_tries)
        self.stats = defaultdict(int)
        self.last_stat_time = time.time()

    def run_forever(self, *args, **kwargs):
        while True:
            self.run_once(*args, **kwargs)
            self.stats = defaultdict(int)
            self.logger.info('sleeping between intervals (%s)', self.interval)
            time.sleep(self.interval)

    def pop_queue(self, container, obj, q_timestamp, q_record):
        q_path = '/%s/%s/%s' % (MISPLACED_OBJECTS_ACCOUNT, container, obj)
        x_timestamp = slightly_later_timestamp(q_record)
        self.stats_log('pop_queue', 'remove %r (%f) from the queue (%s)',
                       q_path, q_timestamp, x_timestamp)
        headers = {'X-Timestamp': x_timestamp}
        direct_delete_container_entry(
            self.swift.container_ring, MISPLACED_OBJECTS_ACCOUNT,
            container, obj, headers=headers)

    def throw_tombstones(self, account, container, obj, timestamp,
                         policy_index, path):
        x_timestamp = slightly_later_timestamp(timestamp)
        self.stats_log('cleanup_attempt', '%r (%f) from policy_index '
                       '%s (%s) will be deleted',
                       path, timestamp, policy_index, x_timestamp)
        # this is the approach that sends the container update headers
        """
        headers = {
            'X-Timestamp': x_timestamp,
            'X-Backend-Storage-Policy-Index': policy_index,
        }
        success = False
        try:
            self.swift.delete_object(account, container, obj,
                                     acceptable_statuses=(2, 4),
                                     headers=headers)
            success = True
        except UnexpectedResponse as err:
            self.stats_log('cleanup_failed', '%r (%f) delete in '
                           'policy_index %s returned %s', path, timestamp,
                           policy_index, err)
        """
        # but we roll our own...
        object_ring = self.swift.get_object_ring(policy_index)
        headers = {
            'X-Timestamp': x_timestamp,
            'X-Storage-Policy-Index': policy_index,
        }
        success = direct_cleanup_object(object_ring,
                                        account, container, obj,
                                        headers=headers)
        if success:
            self.stats_log('cleanup_success', '%r (%f) was successfully '
                           'removed from policy_index %s', path, timestamp,
                           policy_index)
        else:
            self.stats_log('cleanup_failed', '%r (%f) was not cleaned up '
                           'in storage_policy %s', path, timestamp,
                           policy_index)
        return success

    def stats_log(self, metric, msg, *args, **kwargs):
        level = kwargs.pop('level', logging.DEBUG)
        log_message = '%s: ' % metric + msg
        self.logger.log(level, log_message, *args, **kwargs)
        self.stats[metric] += 1

    def ensure_object_in_right_location(self, account, container, obj,
                                        q_policy_index, q_timestamp, q_op,
                                        path, **kwargs):
        container_policy_index = direct_get_container_policy_index(
            self.swift.container_ring, account, container)
        if container_policy_index is None:
            self.stats_log('unavailable_container', '%r (%f) unable to '
                           'determine the destination policy_index', path,
                           q_timestamp)
            return False
        if container_policy_index == q_policy_index:
            self.stats_log('noop_object', '%r (%f) container policy_index '
                           '%s matches queue policy index %s', path,
                           q_timestamp, container_policy_index,
                           q_policy_index)
            return True

        # check if object exists in the destination already
        self.logger.debug('checking for %r (%f) in destination '
                          'policy_index %s', path, q_timestamp,
                          container_policy_index)
        headers = {
            'X-Backend-Storage-Policy-Index': container_policy_index}
        dest_obj = self.swift.get_object_metadata(account, container, obj,
                                                  headers=headers,
                                                  acceptable_statuses=(2, 4))
        dest_ts = float(dest_obj.get('x-timestamp', '0.0'))
        if dest_ts >= q_timestamp:
            self.stats_log('found_object', '%r (%f) in policy_index %s '
                           'is newer than queue (%f)', path, dest_ts,
                           container_policy_index, q_timestamp)
            return self.throw_tombstones(account, container, obj, q_timestamp,
                                         q_policy_index, path)

        # object is misplaced
        self.stats_log('misplaced_object', '%r (%f) in policy_index %s '
                       'should be in policy_index %s', path, q_timestamp,
                       q_policy_index, container_policy_index)

        # fetch object from the source location
        self.logger.debug('fetching %r (%s) from storage policy %s', path,
                          q_timestamp, q_policy_index)
        headers = {
            'X-Backend-Storage-Policy-Index': q_policy_index}
        try:
            source_obj_status, source_obj_info, source_obj_iter = \
                self.swift.get_object(account, container, obj,
                                      headers=headers,
                                      acceptable_statuses=(2, 4))
        except UnexpectedResponse as err:
            source_obj_status = err.resp.status_int
            source_obj_info = {}
            source_obj_iter = None

        source_ts = float(source_obj_info.get("X-Timestamp", 0))
        if source_obj_status == 404 and q_op == 'DELETE':
            return self.delete_object_from_right_location(
                q_policy_index, account, container, obj, q_timestamp, path,
                container_policy_index, source_ts)
        else:
            return self.move_object_to_right_location(
                q_policy_index, account, container, obj, q_timestamp, path,
                container_policy_index, source_ts, source_obj_status,
                source_obj_info, source_obj_iter)

    def move_object_to_right_location(self, q_policy_index, account,
                                      container, obj, q_timestamp, path,
                                      container_policy_index, source_ts,
                                      source_obj_status, source_obj_info,
                                      source_obj_iter, **kwargs):
        if source_obj_status // 100 != 2 or source_ts < q_timestamp:
            if q_timestamp < time.time() - self.reclaim_age:
                # it's old and there are no tombstones or anything; give up
                self.stats_log('lost_source', '%r (%s) was not avaialble in '
                               'policy_index %s and has expired', path,
                               q_timestamp, q_policy_index,
                               level=logging.CRITICAL)
                return True
            # the source object is unavailable or older than the queue
            # entry; a version that will satisfy the queue entry hopefully
            # exists somewhere in the cluster, so wait and try again
            self.stats_log('unavailable_source', '%r (%f) in '
                           'policy_index %s responded %s (%f)', path,
                           q_timestamp, q_policy_index, source_obj_status,
                           source_ts, level=logging.WARNING)
            return False

        # move the object
        # XXX shouldn't this be q_timestamp
        put_timestamp = slightly_later_timestamp(source_ts, offset=2)
        self.stats_log('copy_attempt', '%r (%f) in policy_index %s will be '
                       'moved to policy_index %s (%s)', path, source_ts,
                       q_policy_index, container_policy_index, put_timestamp)
        headers = source_obj_info.copy()
        headers['X-Backend-Storage-Policy-Index'] = container_policy_index
        headers['X-Timestamp'] = put_timestamp

        # XXX sending x-timestamp header causes the proxy's ObjectController
        # PUT to do a HEAD/X-Newest

        try:
            self.swift.upload_object(
                FileLikeIter(source_obj_iter), account, container, obj,
                headers=headers)
        except UnexpectedResponse as err:
            self.stats_log('copy_failed', 'upload %r (%s) from '
                           'policy_index %s to policy_index %s '
                           'returned %s', path, source_ts, q_policy_index,
                           container_policy_index, err, level=logging.WARNING)
            return False
        except:  # noqa
            self.stats_log('unhandled_error', 'unable to upload %r (%s) '
                           'from policy_index %s to policy_index %s ', path,
                           source_ts, q_policy_index, container_policy_index,
                           level=logging.ERROR, exc_info=True)
            return False

        self.stats_log('copy_success', '%r (%f) moved from policy_index %s '
                       'to policy_index %s (%s)', path, source_ts,
                       q_policy_index, container_policy_index, put_timestamp,
                       level=logging.INFO)

        return self.throw_tombstones(account, container, obj, q_timestamp,
                                     q_policy_index, path)

    def delete_object_from_right_location(self, q_policy_index, account,
                                          container, obj, q_timestamp, path,
                                          container_policy_index, source_ts,
                                          **kwargs):
        delete_timestamp = slightly_later_timestamp(q_timestamp, offset=2)
        self.stats_log('delete_attempt', '%r (%f) in policy_index %s '
                       'will be deleted from policy_index %s (%s)', path,
                       source_ts, q_policy_index, container_policy_index,
                       delete_timestamp)
        headers = {
            'X-Backend-Storage-Policy-Index': container_policy_index,
            'X-Timestamp': delete_timestamp,
        }
        try:
            self.swift.delete_object(account, container, obj,
                                     headers=headers)
        except UnexpectedResponse as err:
            self.stats_log('delete_failed', 'delete %r (%f) from '
                           'policy_index %s (%s) returned %s', path,
                           source_ts, container_policy_index,
                           delete_timestamp, err, level=logging.WARNING)
            return False
        except:  # noqa
            self.stats_log('unhandled_error', 'unable to delete %r (%f) '
                           'from policy_index %s (%s)', path, source_ts,
                           container_policy_index, delete_timestamp,
                           level=logging.ERROR, exc_info=True)
            return False

        self.stats_log('delete_success', '%r (%f) deleted from '
                       'policy_index %s (%s)', path, source_ts,
                       container_policy_index, delete_timestamp,
                       level=logging.INFO)

        return self.throw_tombstones(account, container, obj, q_timestamp,
                                     q_policy_index, path)

    def log_stats(self, force=False):
        now = time.time()
        should_log = force or (now - self.last_stat_time > 60)
        if should_log:
            self.last_stat_time = now
            self.logger.info('Reconciler Stats: %r', dict(**self.stats))

    def _iter_misplaced_objects(self):
        container_gen = self.swift.iter_containers(MISPLACED_OBJECTS_ACCOUNT)
        while True:
            one_page = list(itertools.islice(
                container_gen, constraints.CONTAINER_LISTING_LIMIT))
            if not one_page:
                break
            for c in reversed(one_page):
                # encoding here is defensive
                container = c['name'].encode('utf8')
                self.logger.debug('looking for objects in %s', container)
                found_obj = False
                for raw_obj in self.swift.iter_objects(
                        MISPLACED_OBJECTS_ACCOUNT, container):
                    found_obj = True
                    try:
                        obj_info = parse_raw_obj(raw_obj)
                    except Exception:
                        self.stats_log('invalid_record',
                                       'invalid queue record: %r', raw_obj,
                                       level=logging.ERROR, exc_info=True)
                        continue
                    finished = yield obj_info
                    if finished:
                        self.pop_queue(container, raw_obj['name'],
                                       obj_info['q_timestamp'],
                                       obj_info['q_record'])
                self.log_stats()
                self.logger.debug('finished container %s', container)
                if float(container) < time.time() - self.reclaim_age and \
                        not found_obj:
                    # Try to delete old empty containers so the queue doesn't
                    # grow without bound. It's ok if there's a conflict.
                    self.swift.delete_container(
                        MISPLACED_OBJECTS_ACCOUNT, container,
                        acceptable_statuses=(2, 404, 409, 412))

    def run_once(self, *args, **kwargs):
        """
        Process every entry in the queue.
        """
        self.logger.debug('pulling items from the queue')
        q = self._iter_misplaced_objects()
        for info in sending(q, lambda: success):
            self.logger.debug('checking placement for %r (%s) '
                              'in policy_index %s', info['path'],
                              info['q_timestamp'], info['q_policy_index'])
            success = False
            try:
                success = self.ensure_object_in_right_location(**info)
            except:  # noqa
                self.logger.exception('Unhandled Exception trying to '
                                      'reconcile %r (%s) in policy_index %s',
                                      info['path'], info['q_timestamp'],
                                      info['q_policy_index'])
            if success:
                metric = 'success'
                msg = '%(path)r (%(q_timestamp)f) in policy_index ' \
                    '%(q_policy_index)s was handled successfully'
            else:
                metric = 'retry'
                msg = '%(path)r (%(q_timestamp)f) in policy_index ' \
                    '%(q_policy_index)s must be retried'
            self.stats_log(metric, msg, info, level=logging.INFO)
            self.log_stats()
        self.log_stats(force=True)


# TODO(clayg): try and invert control so I don't have to do this
def sending(g, sender):
    """
    Iterate over g with send

    :params g: the iterable
    :params sender: the callable returning the value to send
    """
    yield g.next()
    while True:
        yield g.send(sender())
