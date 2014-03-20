# -*- coding: utf-8 -*-
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

import logging
import os
import signal
import sys
import time
from swift import gettext_ as _
from random import random, shuffle
from tempfile import mkstemp

from eventlet import spawn, patcher, GreenPile, Timeout

import swift.common.db
from swift.container.backend import ContainerBroker, DATADIR
from swift.common.bufferedhttp import http_connect
from swift.common.direct_client import direct_head_container, ClientException
from swift.common.exceptions import ConnectionTimeout
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.ring import Ring
from swift.common.utils import get_logger, config_true_value, ismount, \
    dump_recon_cache, quorum_size, normalize_timestamp, FileLikeIter
from swift.common.daemon import Daemon
from swift.common.http import is_success, HTTP_INTERNAL_SERVER_ERROR
from swift.common.storage_policy import POLICY_INDEX


# How many object cleanups to pull out and process in a batch.
# Chosen arbitrarily.
CLEANUP_BATCH_SIZE = 100

# How many misplaced objects to pull out and process in a batch.
# Chosen arbitrarily.
MISPLACED_OBJECT_BATCH_SIZE = 25


def slightly_later_timestamp(ts):
    # For some reason, Swift uses a 10-microsecond resolution instead of
    # Python's 1-microsecond resolution.
    return float(ts) + 0.00001


def direct_get_oldest_storage_policy_index(container_ring, account_name,
                                           container_name):
    """
    Talk directly to the primary container servers to figure out the storage
    policy index for a given container. In case of disagreement, the oldest
    container is considered correct.

    :param container_ring: ring in which to look up the container locations
    :param account_name: name of the container's account
    :param container_name: name of the container
    :returns: storage policy index, or None if it couldn't get a quorum
    """
    def _eat_client_exception(*args):
        try:
            return direct_head_container(*args)
        except (ClientException, Timeout):
            pass

    pile = GreenPile()
    part, nodes = container_ring.get_nodes(account_name, container_name)
    for node in nodes:
        pile.spawn(_eat_client_exception, node, part, account_name,
                   container_name)

    headers = [x for x in pile if x is not None]
    if len(headers) < quorum_size(len(nodes)):
        return
    headers.sort(key=lambda h: h['x-timestamp'])
    return int(headers[0]['x-storage-policy-index'])


class ContainerReconciler(object):
    """
    Move misplaced objects to the proper storage policy.

    This lives in the container updater daemon because it needs to walk the
    filesystem for container DBs, but it typically has very little work to do.
    By squishing the two together, we get the reconciler FS walking for free.
    """
    def __init__(self, conf, logger):
        self.logger = logger  # share a logger to save sockets
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.concurrency = int(conf.get('reconciler_concurrency', 4))
        self.container_ring = None
        self.request_tries = int(conf.get('request_tries') or 3)
        self.conf_path = conf.get('__file__') or \
            '/etc/swift/container-updater.conf'

    def get_swift(self):
        if not self.swift:
            # XXX untested branch
            self.swift = InternalClient(
                self.conf_path, 'Swift Container Reconciler',
                self.request_tries)
        return self.swift

    def get_container_ring(self):
        """Get the container ring.  Load it if it hasn't been yet."""
        if not self.container_ring:
            # XXX untested branch
            self.container_ring = Ring(self.swift_dir, ring_name='container')
        return self.container_ring

    def reconcile_container(self, broker, info):
        # XXX throw out some statsd stats

        # XXX make sure there's not active work going on for this container,
        # and shove shit into a greenthread, but do it after the fast check
        # here:

        if (info['misplaced_object_count'] <= 0 and
                info['object_cleanup_count'] <= 0):
            return
        # If two different container DBs disagree about their policy index, we
        # could get reconciler fights: one daemon moves an object A -> B, then
        # the other back from B -> A, and so on. This is clearly wasteful, so
        # we avoid it by asking all this container's primary servers and only
        # doing work if our policy agrees with what we find.
        #
        # This means that no reconcilation will happen for a given container
        # unless a quorum of its primary servers are up.
        real_spi = direct_get_oldest_storage_policy_index(
            self.get_container_ring(), broker.account, broker.container)
        if info['storage_policy_index'] != real_spi:
            return

        cleanups = broker.list_cleanups(CLEANUP_BATCH_SIZE)
        while cleanups:
            for cleanup in cleanups:
                self.throw_tombstones(
                    broker.account, broker.container, cleanup['name'],
                    slightly_later_timestamp(cleanup['created_at']),
                    cleanup['storage_policy_index'])

                broker.delete_cleanup(cleanup)
            cleanups = broker.list_cleanups(CLEANUP_BATCH_SIZE)

        misplaced = broker.list_misplaced_objects(MISPLACED_OBJECT_BATCH_SIZE)
        while misplaced:
            for obj_rec in misplaced:
                obj, created_at, _junk, _junk, _junk, policy_index = obj_rec
                # We nudge the timestamps as follows: if the misplaced object
                # has timestamp T, we PUT it to the right policy with
                # timestamp T + 20μs, and issue a DELETE request in the wrong
                # policy with timestamp T + 10μs. That way, in the unlikely
                # event of a reconciler fight, all the container listings will
                # end up consistent once the fighting settles down.
                #
                # If we did not do the timestamp nudging, then since container
                # replication sends rows based on (name, timestamp), the
                # container listings would never be updated, and since
                # reconciliation is triggered by container listings, the
                # reconciler would run forever.
                #
                # If we nudged both the tombstones and new object to T + 10μs,
                # then there'd be a race between the two container updates.
                copied = self.reconcile_object(
                    broker.account, broker.container, obj,
                    created_at, policy_index, info['storage_policy_index'])
                if copied:
                    self.throw_tombstones(
                        broker.account, broker.container, obj,
                        slightly_later_timestamp(created_at),
                        policy_index)
                # If we're running on a primary replica, then it's possible
                # that the synchronous container updates have caused a cleanup
                # row to be created for this object, but we just cleaned it
                # up. There'd be no harm in leaving it for later (object
                # DELETE is idempotent as long as you use the same timestamp),
                # but as an optimization, let's try to remove it.
                broker.delete_cleanup({'name': obj, 'created_at': created_at,
                                       'storage_policy_index': policy_index})
            misplaced = broker.list_misplaced_objects(
                MISPLACED_OBJECT_BATCH_SIZE,
                misplaced[-1]['name'])

    def reconcile_object(self, account, container, obj, obj_listing_ts,
                         from_policy_index, to_policy_index):
        if from_policy_index == to_policy_index:
            return False
        # Check if object is still available (it could have been deleted by
        # the user or another reconciler).
        get_headers = {'X-Backend-Storage-Policy-Index': from_policy_index}
        try:
            real_obj_status, real_obj_info, real_obj_iter = \
                self.get_swift().get_object(account, container, obj,
                                            get_headers)
        except UnexpectedResponse:
            # Object is unavailable for some reason; give up and we'll try
            # again later. If the object was actually deleted, then the delete
            # will propagate to the container listng and we won't try again.
            return False

        obj_listing_ts = normalize_timestamp(obj_listing_ts)
        real_ts = normalize_timestamp(real_obj_info.get("X-Timestamp"))
        if obj_listing_ts != real_ts:
            # This isn't the object we're looking for. Give up and try again
            # later.
            return False

        # Copy the object to the proper storage policy.
        put_ts = normalize_timestamp(
            slightly_later_timestamp(
                slightly_later_timestamp(obj_listing_ts)))
        put_headers = {'X-Backend-Storage-Policy-Index': to_policy_index,
                       'X-Timestamp': put_ts}
        self.get_swift().upload_object(
            FileLikeIter(real_obj_iter), account, container, obj,
            put_headers)
        return True

    def throw_tombstones(self, account, container, obj, timestamp,
                         storage_policy_index):
        self.logger.debug('deleting "%s" (%s) from storage policy %s',
                          '%s/%s/%s' % (account, container, obj), timestamp,
                          storage_policy_index)
        headers = {'X-Timestamp': normalize_timestamp(timestamp),
                   'X-Backend-Storage-Policy-Index': storage_policy_index}
        self.get_swift().delete_object(account, container, obj,
                                       headers=headers,
                                       acceptable_statuses=(2, 4))


class ContainerUpdater(Daemon):
    """Update container information in account listings."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='container-updater')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.interval = int(conf.get('interval', 300))
        self.account_ring = None
        self.concurrency = int(conf.get('concurrency', 4))
        self.slowdown = float(conf.get('slowdown', 0.01))
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.no_changes = 0
        self.successes = 0
        self.failures = 0
        self.account_suppressions = {}
        self.account_suppression_time = \
            float(conf.get('account_suppression_time', 60))
        self.new_account_suppressions = None
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "container.recon")
        self.user_agent = 'container-updater %s' % os.getpid()
        self.reconciler = ContainerReconciler(conf, self.logger)

    def get_account_ring(self):
        """Get the account ring.  Load it if it hasn't been yet."""
        if not self.account_ring:
            self.account_ring = Ring(self.swift_dir, ring_name='account')
        return self.account_ring

    def get_paths(self):
        """
        Get paths to all of the partitions on each drive to be processed.

        :returns: a list of paths
        """
        paths = []
        for device in os.listdir(self.devices):
            dev_path = os.path.join(self.devices, device)
            if self.mount_check and not ismount(dev_path):
                self.logger.warn(_('%s is not mounted'), device)
                continue
            con_path = os.path.join(dev_path, DATADIR)
            if not os.path.exists(con_path):
                continue
            for partition in os.listdir(con_path):
                paths.append(os.path.join(con_path, partition))
        shuffle(paths)
        return paths

    def _load_suppressions(self, filename):
        try:
            with open(filename, 'r') as tmpfile:
                for line in tmpfile:
                    account, until = line.split()
                    until = float(until)
                    self.account_suppressions[account] = until
        except Exception:
            self.logger.exception(
                _('ERROR with loading suppressions from %s: ') % filename)
        finally:
            os.unlink(filename)

    def run_forever(self, *args, **kwargs):
        """
        Run the updater continuously.
        """
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin container update sweep'))
            begin = time.time()
            now = time.time()
            expired_suppressions = \
                [a for a, u in self.account_suppressions.iteritems()
                 if u < now]
            for account in expired_suppressions:
                del self.account_suppressions[account]
            pid2filename = {}
            # read from account ring to ensure it's fresh
            self.get_account_ring().get_nodes('')
            for path in self.get_paths():
                while len(pid2filename) >= self.concurrency:
                    pid = os.wait()[0]
                    try:
                        self._load_suppressions(pid2filename[pid])
                    finally:
                        del pid2filename[pid]
                fd, tmpfilename = mkstemp()
                os.close(fd)
                pid = os.fork()
                if pid:
                    pid2filename[pid] = tmpfilename
                else:
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    patcher.monkey_patch(all=False, socket=True)
                    self.no_changes = 0
                    self.successes = 0
                    self.failures = 0
                    self.new_account_suppressions = open(tmpfilename, 'w')
                    forkbegin = time.time()
                    self.container_sweep(path)
                    elapsed = time.time() - forkbegin
                    self.logger.debug(
                        _('Container update sweep of %(path)s completed: '
                          '%(elapsed).02fs, %(success)s successes, %(fail)s '
                          'failures, %(no_change)s with no changes'),
                        {'path': path, 'elapsed': elapsed,
                         'success': self.successes, 'fail': self.failures,
                         'no_change': self.no_changes})
                    sys.exit()
            while pid2filename:
                pid = os.wait()[0]
                try:
                    self._load_suppressions(pid2filename[pid])
                finally:
                    del pid2filename[pid]
            elapsed = time.time() - begin
            self.logger.info(_('Container update sweep completed: %.02fs'),
                             elapsed)
            dump_recon_cache({'container_updater_sweep': elapsed},
                             self.rcache, self.logger)
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """
        Run the updater once.
        """
        patcher.monkey_patch(all=False, socket=True)
        self.logger.info(_('Begin container update single threaded sweep'))
        begin = time.time()
        self.no_changes = 0
        self.successes = 0
        self.failures = 0
        for path in self.get_paths():
            self.container_sweep(path)
        elapsed = time.time() - begin
        self.logger.info(_(
            'Container update single threaded sweep completed: '
            '%(elapsed).02fs, %(success)s successes, %(fail)s failures, '
            '%(no_change)s with no changes'),
            {'elapsed': elapsed, 'success': self.successes,
             'fail': self.failures, 'no_change': self.no_changes})
        dump_recon_cache({'container_updater_sweep': elapsed},
                         self.rcache, self.logger)

    def container_sweep(self, path):
        """
        Walk the path looking for container DBs and process them.

        :param path: path to walk
        """
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.db'):
                    self.process_container(os.path.join(root, file))
                    time.sleep(self.slowdown)

    def process_container(self, dbfile):
        """
        Process a container: first, update the account's view of this
        container, then reconcile misplaced objects (in the background).

        :param dbfile: container DB to process
        """
        broker = ContainerBroker(dbfile, logger=self.logger)
        info = broker.get_info()

        self.send_update_to_account(broker, info)
        self.reconcile_container(broker, info)

    def send_update_to_account(self, broker, info):
        """
        Update the account's information about this container

        :param broker: container broker
        :param info: container stats; comes from broker.get_info()
        """
        start_time = time.time()
        # Don't send updates if the container was auto-created since it
        # definitely doesn't have up to date statistics.
        if float(info['put_timestamp']) <= 0:
            return
        if self.account_suppressions.get(info['account'], 0) > time.time():
            return
        if info['put_timestamp'] > info['reported_put_timestamp'] or \
                info['delete_timestamp'] > info['reported_delete_timestamp'] \
                or info['object_count'] != info['reported_object_count'] or \
                info['bytes_used'] != info['reported_bytes_used']:
            container = '/%s/%s' % (info['account'], info['container'])
            part, nodes = self.get_account_ring().get_nodes(info['account'])
            events = [spawn(self.container_report, node, part, container,
                            info['put_timestamp'], info['delete_timestamp'],
                            info['object_count'], info['bytes_used'],
                            info['storage_policy_index'])
                      for node in nodes]
            successes = 0
            for event in events:
                if is_success(event.wait()):
                    successes += 1
            if successes >= quorum_size(len(events)):
                self.logger.increment('successes')
                self.successes += 1
                self.logger.debug(
                    _('Update report sent for %(container)s %(broker)s'),
                    {'container': container, 'broker': broker})
                broker.reported(info['put_timestamp'],
                                info['delete_timestamp'], info['object_count'],
                                info['bytes_used'])
            else:
                self.logger.increment('failures')
                self.failures += 1
                self.logger.debug(
                    _('Update report failed for %(container)s %(broker)s'),
                    {'container': container, 'broker': broker})
                self.account_suppressions[info['account']] = until = \
                    time.time() + self.account_suppression_time
                if self.new_account_suppressions:
                    print >>self.new_account_suppressions, \
                        info['account'], until
            # Only track timing data for attempted updates:
            self.logger.timing_since('timing', start_time)
        else:
            self.logger.increment('no_changes')
            self.no_changes += 1

    def container_report(self, node, part, container, put_timestamp,
                         delete_timestamp, count, bytes,
                         storage_policy_index):
        """
        Report container info to an account server.

        :param node: node dictionary from the account ring
        :param part: partition the account is on
        :param container: container name
        :param put_timestamp: put timestamp
        :param delete_timestamp: delete timestamp
        :param count: object count in the container
        :param bytes: bytes used in the container
        :param storage_policy_index: the policy index for the container
        """
        with ConnectionTimeout(self.conn_timeout):
            try:
                headers = {
                    'X-Put-Timestamp': put_timestamp,
                    'X-Delete-Timestamp': delete_timestamp,
                    'X-Object-Count': count,
                    'X-Bytes-Used': bytes,
                    'X-Account-Override-Deleted': 'yes',
                    POLICY_INDEX: storage_policy_index,
                    'user-agent': self.user_agent}
                conn = http_connect(
                    node['ip'], node['port'], node['device'], part,
                    'PUT', container, headers=headers)
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR account update failed with '
                    '%(ip)s:%(port)s/%(device)s (will retry later): '), node)
                return HTTP_INTERNAL_SERVER_ERROR
        with Timeout(self.node_timeout):
            try:
                resp = conn.getresponse()
                resp.read()
                return resp.status
            except (Exception, Timeout):
                if self.logger.getEffectiveLevel() <= logging.DEBUG:
                    self.logger.exception(
                        _('Exception with %(ip)s:%(port)s/%(device)s'), node)
                return HTTP_INTERNAL_SERVER_ERROR
            finally:
                conn.close()

    def reconcile_container(self, broker, info):
        """
        Move misplaced objects to the proper storage policies.

        All the work here happens asynchronously to avoid damaging the
        timeliness of account stats.

        :param broker: container broker
        :param info: container stats; comes from broker.get_info()
        """
        self.reconciler.reconcile_container(broker, info)
