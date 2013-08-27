# Copyright (c) 2010-2013 OpenStack, LLC.
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

""" Disk File Interface for Swift Object Server"""

from __future__ import with_statement
import cPickle as pickle
import errno
import os
import time
import uuid
import hashlib
import logging
import traceback
from gettext import gettext as _
from os.path import basename, dirname, exists, getmtime, getsize, join
from tempfile import mkstemp
from contextlib import contextmanager

from xattr import getxattr, setxattr
from eventlet import Timeout

from swift.common.constraints import check_mount
from swift.common.utils import mkdirs, normalize_timestamp, \
    storage_directory, hash_path, renamer, fallocate, fsync, \
    fdatasync, drop_buffer_cache, ThreadPool, lock_path, write_pickle
from swift.common.exceptions import DiskFileSizeInvalid, DiskFileNotExist, \
    DiskFileCollision, DiskFileDeleted, DiskFileExpired, DiskWriterNotReady, \
    DiskFileNoSpace, DiskFileDeviceUnavailable, PathNotDir
from swift.common.swob import multi_range_iterator


PICKLE_PROTOCOL = 2
ONE_WEEK = 604800
HASH_FILE = 'hashes.pkl'
METADATA_KEY = 'user.swift.metadata'
# These are system-set metadata keys that cannot be changed with a POST.
# They should be lowercase.
DATAFILE_SYSTEM_META = set('content-length content-type deleted etag'.split())


def read_metadata(fd):
    """
    Helper function to read the pickled metadata from an object file.

    :param fd: file descriptor to load the metadata from

    :returns: dictionary of metadata
    """
    metadata = ''
    key = 0
    try:
        while True:
            metadata += getxattr(fd, '%s%s' % (METADATA_KEY, (key or '')))
            key += 1
    except IOError:
        pass
    return pickle.loads(metadata)


def write_metadata(fd, metadata):
    """
    Helper function to write pickled metadata for an object file.

    :param fd: file descriptor to write the metadata
    :param metadata: metadata to write
    """
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        setxattr(fd, '%s%s' % (METADATA_KEY, key or ''), metastr[:254])
        metastr = metastr[254:]
        key += 1


def quarantine_renamer(device_path, corrupted_file_path):
    """
    In the case that a file is corrupted, move it to a quarantined
    area to allow replication to fix it.

    :params device_path: The path to the device the corrupted file is on.
    :params corrupted_file_path: The path to the file you want quarantined.

    :returns: path (str) of directory the file was moved to
    :raises OSError: re-raises non errno.EEXIST / errno.ENOTEMPTY
                     exceptions from rename
    """
    from_dir = dirname(corrupted_file_path)
    to_dir = join(device_path, 'quarantined', 'objects', basename(from_dir))
    invalidate_hash(dirname(from_dir))
    try:
        renamer(from_dir, to_dir)
    except OSError, e:
        if e.errno not in (errno.EEXIST, errno.ENOTEMPTY):
            raise
        to_dir = "%s-%s" % (to_dir, uuid.uuid4().hex)
        renamer(from_dir, to_dir)
    return to_dir


def hash_suffix(path, reclaim_age):
    """
    Performs reclamation and returns an md5 of all (remaining) files.

    :param reclaim_age: age in seconds at which to remove tombstones
    :raises PathNotDir: if given path is not a valid directory
    :raises OSError: for non-ENOTDIR errors
    """
    md5 = hashlib.md5()
    try:
        path_contents = sorted(os.listdir(path))
    except OSError, err:
        if err.errno in (errno.ENOTDIR, errno.ENOENT):
            raise PathNotDir()
        raise
    for hsh in path_contents:
        hsh_path = join(path, hsh)
        try:
            files = os.listdir(hsh_path)
        except OSError, err:
            if err.errno == errno.ENOTDIR:
                partition_path = dirname(path)
                objects_path = dirname(partition_path)
                device_path = dirname(objects_path)
                quar_path = quarantine_renamer(device_path, hsh_path)
                logging.exception(
                    _('Quarantined %s to %s because it is not a directory') %
                    (hsh_path, quar_path))
                continue
            raise
        if len(files) == 1:
            if files[0].endswith('.ts'):
                # remove tombstones older than reclaim_age
                ts = files[0].rsplit('.', 1)[0]
                if (time.time() - float(ts)) > reclaim_age:
                    os.unlink(join(hsh_path, files[0]))
                    files.remove(files[0])
        elif files:
            files.sort(reverse=True)
            meta = data = tomb = None
            for filename in list(files):
                if not meta and filename.endswith('.meta'):
                    meta = filename
                if not data and filename.endswith('.data'):
                    data = filename
                if not tomb and filename.endswith('.ts'):
                    tomb = filename
                if (filename < tomb or       # any file older than tomb
                    filename < data or       # any file older than data
                    (filename.endswith('.meta') and
                     filename < meta)):      # old meta
                    os.unlink(join(hsh_path, filename))
                    files.remove(filename)
        if not files:
            os.rmdir(hsh_path)
        for filename in files:
            md5.update(filename)
    try:
        os.rmdir(path)
    except OSError:
        pass
    return md5.hexdigest()


def invalidate_hash(suffix_dir):
    """
    Invalidates the hash for a suffix_dir in the partition's hashes file.

    :param suffix_dir: absolute path to suffix dir whose hash needs
                       invalidating
    """

    suffix = basename(suffix_dir)
    partition_dir = dirname(suffix_dir)
    hashes_file = join(partition_dir, HASH_FILE)
    with lock_path(partition_dir):
        try:
            with open(hashes_file, 'rb') as fp:
                hashes = pickle.load(fp)
            if suffix in hashes and not hashes[suffix]:
                return
        except Exception:
            return
        hashes[suffix] = None
        write_pickle(hashes, hashes_file, partition_dir, PICKLE_PROTOCOL)


def get_hashes(partition_dir, recalculate=None, do_listdir=False,
               reclaim_age=ONE_WEEK):
    """
    Get a list of hashes for the suffix dir.  do_listdir causes it to mistrust
    the hash cache for suffix existence at the (unexpectedly high) cost of a
    listdir.  reclaim_age is just passed on to hash_suffix.

    :param partition_dir: absolute path of partition to get hashes for
    :param recalculate: list of suffixes which should be recalculated when got
    :param do_listdir: force existence check for all hashes in the partition
    :param reclaim_age: age at which to remove tombstones

    :returns: tuple of (number of suffix dirs hashed, dictionary of hashes)
    """

    hashed = 0
    hashes_file = join(partition_dir, HASH_FILE)
    modified = False
    force_rewrite = False
    hashes = {}
    mtime = -1

    if recalculate is None:
        recalculate = []

    try:
        with open(hashes_file, 'rb') as fp:
            hashes = pickle.load(fp)
        mtime = getmtime(hashes_file)
    except Exception:
        do_listdir = True
        force_rewrite = True
    if do_listdir:
        for suff in os.listdir(partition_dir):
            if len(suff) == 3:
                hashes.setdefault(suff, None)
        modified = True
    hashes.update((hash_, None) for hash_ in recalculate)
    for suffix, hash_ in hashes.items():
        if not hash_:
            suffix_dir = join(partition_dir, suffix)
            try:
                hashes[suffix] = hash_suffix(suffix_dir, reclaim_age)
                hashed += 1
            except PathNotDir:
                del hashes[suffix]
            except OSError:
                logging.exception(_('Error hashing suffix'))
            modified = True
    if modified:
        with lock_path(partition_dir):
            if force_rewrite or not exists(hashes_file) or \
                    getmtime(hashes_file) == mtime:
                write_pickle(
                    hashes, hashes_file, partition_dir, PICKLE_PROTOCOL)
                return hashed, hashes
        return get_hashes(partition_dir, recalculate, do_listdir,
                          reclaim_age)
    else:
        return hashed, hashes


class DiskWriter(object):
    """
    Encapsulation of the write context for servicing PUT REST API requests.
    Serves as the context manager object for DiskFile's "create" method.

    :param device_path: path to device (e.g. /srv/node/sdb)
    :param datadir: path to destination hash
    :param name: name of the object (e.g. /a/c/o)
    :param size: optional size in bytes to fallocate
    :param bytes_per_sync: number of bytes between fdatasync calls
    :param threadpool: thread pool in which to do blocking operations

    """
    def __init__(self, device_path, datadir, name, size=None,
                 bytes_per_sync=(512 * 1024 * 1024),
                 threadpool=None):
        self.tmpdir = join(device_path, 'tmp')
        self.datadir = datadir
        self.name = name
        self.size = size
        self.fd = None
        self.tmppath = None
        self.upload_size = 0
        self.last_sync = 0
        self.bytes_per_sync = bytes_per_sync
        self.threadpool = threadpool or ThreadPool(nthreads=0)

    def __enter__(self):
        """
        Open handle to temporary path, creating directory if needed.  If size
        was specified it will be fallocated here and raise if unavailable.

        :raises: DiskFileNoSpace
        """
        if not exists(self.tmpdir):
            mkdirs(self.tmpdir)
        self.fd, self.tmppath = mkstemp(dir=self.tmpdir)
        if self.size is not None and self.size > 0:
            try:
                fallocate(self.fd, self.size)
            except OSError:
                raise DiskFileNoSpace()
        self._timestamp = None
        self._save_old_data = False
        return self

    def __exit__(self, t, v, tb):
        try:
            if self.fd:
                os.close(self.fd)
        except OSError:
            pass
        try:
            if self.tmppath:
                os.unlink(self.tmppath)
        except OSError:
            pass
        if not self._save_old_data:
            self.unlinkold()

    def write(self, chunk):
        """
        Write a chunk of data into the temporary file.

        :param chunk: the chunk of data to write as a string object

        :raises: DiskWriterNotReady if called out of context
        """
        if not self.fd:
            raise DiskWriterNotReady()

        def _write_entire_chunk(chunk):
            while chunk:
                written = os.write(self.fd, chunk)
                self.upload_size += written
                chunk = chunk[written:]

        self.threadpool.run_in_thread(_write_entire_chunk, chunk)

        # For large files sync every 512MB (by default) written
        diff = self.upload_size - self.last_sync
        if diff >= self.bytes_per_sync:
            self.threadpool.force_run_in_thread(fdatasync, self.fd)
            drop_buffer_cache(self.fd, self.last_sync, diff)
            self.last_sync = self.upload_size

    def _put(self, metadata, extension='.data'):
        """
        Finalize writing the file on disk, and renames it from the temp file
        to the real location.  This should be called after the data has been
        written to the temp file.

        :param metadata: dictionary of metadata to be written
        :param extension: extension to be used when making the file

        :raises: DiskWriterNotReady if called out of context
        """
        if not self.fd:
            raise DiskWriterNotReady()

        self._timestamp = normalize_timestamp(metadata['X-Timestamp'])
        metadata['name'] = self.name

        def finalize_put():
            # Write the metadata before calling fsync() so that both data and
            # metadata are flushed to disk.
            write_metadata(self.fd, metadata)
            # We call fsync() before calling drop_cache() to lower the amount
            # of redundant work the drop cache code will perform on the pages
            # (now that after fsync the pages will be all clean).
            fsync(self.fd)
            # From the Department of the Redundancy Department, make sure
            # we call drop_cache() after fsync() to avoid redundant work
            # (pages all clean).
            drop_buffer_cache(self.fd, 0, self.upload_size)
            invalidate_hash(dirname(self.datadir))
            # After the rename completes, this object will be available for
            # other requests to reference.
            data_file = join(self.datadir, self._timestamp + extension)
            renamer(self.tmppath, data_file)
            self._data_file = data_file

        self.threadpool.force_run_in_thread(finalize_put)
        self._metadata = metadata

    def put(self, metadata):
        return self._put(metadata)

    def put_metadata(self, metadata):
        self._save_old_data = True
        return self._put(metadata, extension='.meta')

    def put_tombstone(self, req_timestamp):
        metadata = {'X-Timestamp': req_timestamp}
        return self._put(metadata, extension='.ts')

    def unlinkold(self):
        """
        Remove any older versions of the object file.  Any file that has an
        older timestamp than timestamp will be deleted.

        :param timestamp: timestamp to compare with each file
        """
        if not self._timestamp:
            return

        def _unlinkold():
            for fname in os.listdir(self.datadir):
                if fname < self._timestamp:
                    try:
                        os.unlink(join(self.datadir, fname))
                    except OSError, err:    # pragma: no cover
                        if err.errno != errno.ENOENT:
                            raise
        self.threadpool.run_in_thread(_unlinkold)


class DiskReader(object):
    """
    Read object from storage.

    :param device_path: path to device (e.g. /srv/node/sdb)
    :param datadir: path to destination hash
    :param name: name of the object (e.g. /a/c/o)
    :param disk_chunk_size: size of chunks on file reads
    :param iter_hook: called when __iter__ returns a chunk
    :param threadpool: thread pool in which to do blocking operations


    :raises DiskFileNotExist: if the data file can't be found
    :raises DiskFileDeleted: if the object is deleted
    :raises DiskFileExpired: if the object is expired
    :raises DiskFileCollision: on md5 collision
    """
    def __init__(self, device_path, datadir, name, logger=None,
                 disk_chunk_size=65536, iter_hook=None, threadpool=None):
        self.device_path = device_path
        self.datadir = datadir
        self.name = name
        self.logger = logger or \
            logging.getLogger('swift.obj.diskfile.diskreader')
        self.disk_chunk_size = disk_chunk_size
        self.iter_hook = iter_hook
        self.threadpool = threadpool or ThreadPool(nthreads=0)
        self.quarantined_dir = None

        # FIXME(clayg): internal state set after open to change behavior
        self.keep_cache = False

        self._iter_etag = None
        self._started_at_0 = False
        self._read_to_eof = False
        self._suppress_file_closing = False

        self._meta_file = None
        self._data_file = None
        self._find_data_file()

        self.fp = open(self._data_file, 'rb')
        self._metadata = self._read_metadata()

        self._obj_size = None

    def _find_data_file(self):
        if not exists(self.datadir):
            raise DiskFileNotExist()
        files = sorted(os.listdir(self.datadir), reverse=True)
        for afile in files:
            if afile.endswith('.ts'):
                self._data_file = None
                with open(join(self.datadir, afile)) as mfp:
                    metadata = read_metadata(mfp)
                metadata['deleted'] = True
                raise DiskFileDeleted(metadata['X-Timestamp'])
            if afile.endswith('.meta') and not self._meta_file:
                self._meta_file = join(self.datadir, afile)
            if afile.endswith('.data') and not self._data_file:
                self._data_file = join(self.datadir, afile)
                break
        if not self._data_file:
            raise DiskFileNotExist()

    def _check_collision(self, metadata):
        """
        :raises DiskFileCollision: on md5 collision
        """
        if 'name' in metadata and metadata['name'] != self.name:
            self.logger.error(_('Client path %(client)s does not match path '
                                'stored in object metadata %(meta)s'),
                              {'client': self.name, 'meta': metadata['name']})
            raise DiskFileCollision('Client path does not match path stored '
                                    'in object metadata')

    def _check_expired(self, metadata):
        """
        :raises DiskFileExpired: if the object is expired
        """
        if ('X-Delete-At' in metadata and
                int(metadata['X-Delete-At']) <= time.time()):
            raise DiskFileExpired(metadata['X-Delete-At'])

    def _check_deleted(self, metadata):
        """
        :raises DiskFileDeleted: if the object is deleted
        """
        if 'deleted' in metadata:
            raise DiskFileDeleted(metadata['X-Timestamp'])

    def _check_metadata(self, metadata):
        """
        :raises DiskFileDeleted: if the object is deleted
        :raises DiskFileExpired: if the object is expired
        :raises DiskFileCollision: on md5 collision
        """
        self._check_collision(metadata)
        self._check_deleted(metadata)
        self._check_expired(metadata)

    def _read_metadata(self):
        datafile_metadata = read_metadata(self.fp)
        if self._meta_file:
            with open(self._meta_file) as mfp:
                metadata = read_metadata(mfp)
            sys_metadata = dict(
                [(key, val) for key, val in datafile_metadata.iteritems()
                 if key.lower() in DATAFILE_SYSTEM_META])
            metadata.update(sys_metadata)
        else:
            metadata = datafile_metadata
        self._check_metadata(metadata)
        return metadata

    def get_metadata(self):
        """
        Public accessor for consistency with get_obj_size.
        """
        return self._metadata

    def _read_data_file_size(self):
        try:
            file_size = 0
            file_size = self.threadpool.run_in_thread(
                getsize, self._data_file)
            if 'Content-Length' in self._metadata:
                metadata_size = int(self._metadata['Content-Length'])
                if file_size != metadata_size:
                    self.quarantine()
                    raise DiskFileSizeInvalid(
                        'Content-Length of %s does not match file size '
                        'of %s' % (metadata_size, file_size))
            return file_size
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise
        raise DiskFileNotExist('Data File does not exist.')

    def get_obj_size(self):
        """
        Returns the os.path.getsize for the file.  Raises an exception if this
        file does not match the Content-Length stored in the metadata. Or if
        self._data_file does not exist.

        :returns: file size as an int
        :raises DiskFileSizeInvalid: on file size mismatch.
        :raises DiskFileNotExist: on file not existing (including deleted)
        """
        if self._obj_size is None:
            self._obj_size = self._read_data_file_size()
        return self._obj_size

    def _drop_cache(self, fd, offset, length):
        """Method for no-oping buffer cache drop method."""
        if not self.keep_cache:
            drop_buffer_cache(fd, offset, length)

    def __iter__(self):
        """Returns an iterator over the data file."""
        self.get_obj_size()
        try:
            dropped_cache = 0
            read = 0
            self._started_at_0 = False
            self._read_to_eof = False
            if self.fp.tell() == 0:
                self._started_at_0 = True
                self._iter_etag = hashlib.md5()
            while True:
                chunk = self.threadpool.run_in_thread(
                    self.fp.read, self.disk_chunk_size)
                if chunk:
                    if self._iter_etag:
                        self._iter_etag.update(chunk)
                    read += len(chunk)
                    if read - dropped_cache > (1024 * 1024):
                        self._drop_cache(self.fp.fileno(), dropped_cache,
                                         read - dropped_cache)
                        dropped_cache = read
                    yield chunk
                    if self.iter_hook:
                        self.iter_hook()
                else:
                    self._read_to_eof = True
                    self._drop_cache(self.fp.fileno(), dropped_cache,
                                     read - dropped_cache)
                    break
        finally:
            if not self._suppress_file_closing:
                self.close()

    def app_iter_range(self, start, stop):
        """Returns an iterator over the data file for range (start, stop)"""
        if start or start == 0:
            self.fp.seek(start)
        if stop is not None:
            length = stop - start
        else:
            length = None
        try:
            for chunk in self:
                if length is not None:
                    length -= len(chunk)
                    if length < 0:
                        # Chop off the extra:
                        yield chunk[:length]
                        break
                yield chunk
        finally:
            if not self._suppress_file_closing:
                self.close()

    def app_iter_ranges(self, ranges, content_type, boundary, size):
        """Returns an iterator over the data file for a set of ranges"""
        if not ranges:
            yield ''
        else:
            try:
                self._suppress_file_closing = True
                for chunk in multi_range_iterator(
                        ranges, content_type, boundary, size,
                        self.app_iter_range):
                    yield chunk
            finally:
                self._suppress_file_closing = False
                self.close()

    def quarantine(self):
        """
        In the case that a file is corrupted, move it to a quarantined
        area to allow replication to fix it.

        :returns: if quarantine is successful, path to quarantined
                  directory otherwise None
        """
        if self._data_file and not self.quarantined_dir:
            self.quarantined_dir = self.threadpool.run_in_thread(
                quarantine_renamer, self.device_path, self._data_file)
            self.logger.increment('quarantines')
            return self.quarantined_dir

    def _handle_close_quarantine(self):
        """Check if file needs to be quarantined"""
        if self.quarantined_dir:
            return
        if self._iter_etag and self._started_at_0 and self._read_to_eof and \
                'ETag' in self._metadata and \
                self._iter_etag.hexdigest() != self._metadata.get('ETag'):
            self.quarantine()

    def close(self):
        """
        Close the file. Will handle quarantining file if necessary.
        """
        if self.fp:
            try:
                self._handle_close_quarantine()
            except (Exception, Timeout), e:
                self.logger.error(_(
                    'ERROR DiskFile %(data_file)s in '
                    '%(data_dir)s close failure: %(exc)s : %(stack)s'),
                    {'exc': e, 'stack': ''.join(traceback.format_stack()),
                     'data_file': self._data_file, 'data_dir': self.datadir})
            finally:
                self.fp.close()
                self.fp = None

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.close()


class DiskFile(object):
    """
    Manage object files on disk.

    :param path: path to devices on the node
    :param device: device name
    :param partition: partition on the device the object lives in
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param disk_chunk_size: size of chunks on file reads
    :param bytes_per_sync: number of bytes between fdatasync calls
    :param iter_hook: called when __iter__ returns a chunk
    :param threadpool: thread pool in which to do blocking operations

    """

    def __init__(self, path, device, partition, account, container, obj,
                 logger, disk_chunk_size=65536,
                 bytes_per_sync=(512 * 1024 * 1024),
                 iter_hook=None, threadpool=None, obj_dir='objects',
                 mount_check=False):
        if mount_check and not check_mount(path, device):
            raise DiskFileDeviceUnavailable()
        self.disk_chunk_size = disk_chunk_size
        self.bytes_per_sync = bytes_per_sync
        self.iter_hook = iter_hook
        self.name = '/' + '/'.join((account, container, obj))
        name_hash = hash_path(account, container, obj)
        self.datadir = join(
            path, device, storage_directory(obj_dir, partition, name_hash))
        self.device_path = join(path, device)
        self.logger = logger
        self.threadpool = threadpool or ThreadPool(nthreads=0)
        self._reader = None

    def open(self):
        """
        Read metadata and prepare DiskFile for reading.
        """
        return DiskReader(self.device_path, self.datadir, self.name,
                          logger=self.logger,
                          disk_chunk_size=self.disk_chunk_size,
                          iter_hook=self.iter_hook,
                          threadpool=self.threadpool)

    def read_metadata(self, verify_data_file_size=True):
        with self.open() as dfr:
            if verify_data_file_size:
                dfr.get_obj_size()
            return dfr.get_metadata()

    @contextmanager
    def create(self, size=None):
        """
        Context manager to create a file. We create a temporary file first, and
        then return a DiskWriter object to encapsulate the state.

        :param size: optional initial size of file to explicitly allocate on
                     disk
        :raises DiskFileNoSpace: if a size is specified and allocation fails
        """
        with DiskWriter(self.device_path, self.datadir, self.name,
                        size=size) as writer:
            yield writer
        # FIXME(clayg): this is handy for tests, but sorta sneaky
        self._data_file = getattr(writer, '_data_file', None)
        self._metadata = getattr(writer, '_metadata', None)

    def update_metadata(self, metadata):
        """
        Update metadata associated with an object.

        :param metadata: dictionary of metadata to be written
        """
        with self.create() as writer:
            writer.put_metadata(metadata)

    def delete(self, req_timestamp):
        """
        Write out tombstone and unlink old files.

        :param req_timestamp: timestamp of the request to delete object
        """
        with self.create() as writer:
            writer.put_tombstone(req_timestamp)
