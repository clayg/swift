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

import errno
import os
import sys
import time
import signal
import math
from re import sub

import eventlet.debug

from swift.common import utils
from swift.common.storage_policy import RingMetadataCache, POLICIES, EC_POLICY


class Daemon(object):
    """Daemon base class"""

    def __init__(self, conf):
        self.conf = conf
        self.logger = utils.get_logger(conf, log_route='daemon')

    def run_once(self, *args, **kwargs):
        """Override this to run the script once"""
        raise NotImplementedError('run_once not implemented')

    def run_forever(self, *args, **kwargs):
        """Override this to run forever"""
        raise NotImplementedError('run_forever not implemented')

    def run(self, once=False, **kwargs):
        if once:
            self.run_once(**kwargs)
        else:
            self.run_forever(**kwargs)


class DaemonStrategy(object):

    def __init__(self, daemon, logger):
        self.daemon = daemon
        self.logger = logger
        self.running = False

    def setup(self, **kwargs):
        utils.validate_configuration()
        utils.drop_privileges(self.daemon.conf.get('user', 'swift'))
        utils.capture_stdio(self.logger, **kwargs)

        def kill_children(*args):
            self.running = False
            self.logger.info('SIGTERM received')
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.killpg(0, signal.SIGTERM)
            os._exit(0)

        signal.signal(signal.SIGTERM, kill_children)

    def _run(self, once=False, **kwargs):
        """Run the daemon"""
        self.daemon.run(once=once, **kwargs)

    def run(self, once=False, **kwargs):
        """Daemonize and execute our strategy"""
        self.setup(**kwargs)
        self.running = True
        try:
            self._run(once=once, **kwargs)
        except KeyboardInterrupt:
            self.logger.notice('User quit')
            self.running = False


class DisksPerWorkerStrategy(DaemonStrategy):

    def __init__(self, *args, **kwargs):
        super(DisksPerWorkerStrategy, self).__init__(*args, **kwargs)
        self.disks_per_worker = self.daemon.disks_per_worker
        self.workers_by_dev = {}
        # XXX: something is leaking here
        ec_policies = [p for p in POLICIES if p.policy_type == EC_POLICY]
        self._device_cache = RingMetadataCache.from_policies(
            ec_policies, 'device',
            self.daemon.swift_dir, self.daemon.bind_ip, self.daemon.port)
        self.last_reload = 0
        self.device_check_interval = self.daemon.ring_check_interval
        self._devices = set()

    def _fork(self, once, **kwargs):
        pid = os.fork()
        if pid == 0:
            signal.signal(signal.SIGHUP, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)

            kwargs['devices'] = ','.join(kwargs.pop('devices', [])) or None
            self.daemon.run(once, **kwargs)

            self.logger.debug('forked worker %s finished', os.getpid())
        else:
            self.register_worker_start(kwargs.get('devices'), pid)
        return pid

    def devices(self):
        """
        Maybe refresh our devices from ring cache.
        """
        now = time.time()
        if now - self.last_reload >= self.device_check_interval:
            self.last_reload = now
            self._devices = \
                self._device_cache.unique_values_for_devs_from_rings()
        return self._devices

    def register_worker_start(self, devices, pid):
        self.logger.debug('spawned worker %s for %s', pid,
                          ', '.join(devices))
        for d in devices:
            self.workers_by_dev[d] = pid

    def register_worker_exit(self, pid):
        for d, p in self.workers_by_dev.items():
            if p == pid:
                del self.workers_by_dev[d]

    def unspawned_device_groups(self, override_devices):
        """
        Take the set of all devices for this node from the cache, filter those
        which are not serviced by a running worker and distribute them evenly
        into new workers to be spawned according to the configured devices per
        worker count.

        :param override_devices: list of srings, can come from commandline
        """
        unhandled_devices = set()
        for device in self.devices():
            if device not in self.workers_by_dev:
                unhandled_devices.add(device)
        if override_devices:
            unhandled_devices &= override_devices
        seq = list(unhandled_devices)
        if not seq:
            return []
        if self.disks_per_worker < 1:
            return [seq]
        n = int(math.ceil(1.0 * len(seq) / self.disks_per_worker))
        return [seq[i::n] for i in range(n)]

    def _run(self, once, **kwargs):
        override_devices = set(utils.list_from_csv(kwargs.pop(
            'devices', None)))
        for devices in self.unspawned_device_groups(override_devices):
            kwargs['devices'] = devices
            if self._fork(once, **kwargs) == 0:
                return 0
        while self.running:
            saw_some_action = False
            if not once:
                for devices in self.unspawned_device_groups(override_devices):
                    saw_some_action = True
                    kwargs['devices'] = devices
                    if self._fork(once, **kwargs) == 0:
                        return 0
            for p in set(self.workers_by_dev.values()):
                try:
                    pid, status = os.waitpid(p, os.WNOHANG)
                except OSError as err:
                    if err.errno not in (errno.EINTR, errno.ECHILD):
                        raise
                else:
                    if pid == 0:
                        # child still running
                        continue
                    saw_some_action = True
                    self.register_worker_exit(pid)
                    if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                        self.logger.debug('worker %s exited', pid)
                    else:
                        self.logger.notice('worker %s died', pid)

            if not saw_some_action:
                time.sleep(0.01)
            if once and not self.workers_by_dev:
                self.logger.notice('Finished %s', os.getpid())
                break

        # TODO: this doesn't run on ctrl-c
        for p in set(self.workers_by_dev.values()):
            try:
                os.kill(p)
            except OSError as err:
                if err.errno not in (errno.EINTR, errno.ECHILD):
                    raise
            else:
                self.logger.debug('killed %s', p)

        return 0


def run_daemon(klass, conf_file, section_name='', once=False, **kwargs):
    """
    Loads settings from conf, then instantiates daemon "klass" and runs the
    daemon with the specified once kwarg.  The section_name will be derived
    from the daemon "klass" if not provided (e.g. ObjectReplicator =>
    object-replicator).

    :param klass: Class to instantiate, subclass of common.daemon.Daemon
    :param conf_file: Path to configuration file
    :param section_name: Section name from conf file to load config from
    :param once: Passed to daemon run method
    """
    # very often the config section_name is based on the class name
    # the None singleton will be passed through to readconf as is
    if section_name is '':
        section_name = sub(r'([a-z])([A-Z])', r'\1-\2',
                           klass.__name__).lower()
    try:
        conf = utils.readconf(conf_file, section_name,
                              log_name=kwargs.get('log_name'))
    except (ValueError, IOError) as e:
        # The message will be printed to stderr
        # and results in an exit code of 1.
        sys.exit(e)

    # once on command line (i.e. daemonize=false) will over-ride config
    once = once or not utils.config_true_value(conf.get('daemonize', 'true'))

    # pre-configure logger
    if 'logger' in kwargs:
        logger = kwargs.pop('logger')
    else:
        logger = utils.get_logger(conf, conf.get('log_name', section_name),
                                  log_to_console=kwargs.pop('verbose', False),
                                  log_route=section_name)

    # optional nice/ionice priority scheduling
    utils.modify_priority(conf, logger)

    # disable fallocate if desired
    if utils.config_true_value(conf.get('disable_fallocate', 'no')):
        utils.disable_fallocate()
    # set utils.FALLOCATE_RESERVE if desired
    utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
        utils.config_fallocate_value(conf.get('fallocate_reserve', '1%'))

    # By default, disable eventlet printing stacktraces
    eventlet_debug = utils.config_true_value(conf.get('eventlet_debug', 'no'))
    eventlet.debug.hub_exceptions(eventlet_debug)

    # Ensure TZ environment variable exists to avoid stat('/etc/localtime') on
    # some platforms. This locks in reported times to UTC.
    os.environ['TZ'] = 'UTC+0'
    time.tzset()

    if klass.__name__ == 'ObjectReconstructor':
        run_strategy = DisksPerWorkerStrategy
    else:
        run_strategy = DaemonStrategy

    logger.notice('Starting %s', os.getpid())
    try:
        run_strategy(klass(conf), logger).run(once=once, **kwargs)
    except KeyboardInterrupt:
        logger.info('User quit')
    logger.notice('Exited %s', os.getpid())
