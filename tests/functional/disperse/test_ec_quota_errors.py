#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from math import ceil
from random import sample
from time import sleep, time
from unittest import SkipTest

from glusto.core import Glusto as g

from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import remove_file
from glustolibs.gluster.lib_utils import (append_string_to_file,
                                          get_disk_usage,
                                          search_pattern_in_file)
from glustolibs.gluster.quota_ops import (quota_enable, quota_fetch_list,
                                          quota_limit_usage,
                                          quota_set_alert_time,
                                          quota_set_hard_timeout,
                                          quota_set_soft_timeout)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.io.utils import validate_io_procs, wait_for_io_to_complete
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestEcQuotaError(GlusterBaseClass):
    """
    Description: To check EIO errors changes to EDQUOTE errors when the
    specified quota limits are breached
    """
    # pylint: disable=too-many-instance-attributes, too-many-statements
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        if cls.volume_type == 'distributed-dispersed':
            raise SkipTest('BZ #1707813 limits the functionality of fallocate')
        cls.script_path = '/usr/share/glustolibs/io/scripts/fd_writes.py'
        ret = upload_scripts(cls.clients, cls.script_path)
        if not ret:
            raise ExecutionError('Failed to upload IO script to client')

    def setUp(self):
        self.get_super_method(self, 'setUp')()
        self.num_of_dirs = 2

        # For test_ec_quota_errors_on_limit only one client is needed
        if 'on_limit' in self.id().split('.')[-1]:
            self.num_of_dirs = 1
            self.mounts = [self.mounts[0]]
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        self.all_mount_procs = []
        self.offline_bricks = []
        if not ret:
            raise ExecutionError('Failed to setup and mount volume')

    def tearDown(self):
        if self.offline_bricks:
            ret, _, _ = volume_start(self.mnode, self.volname, force=True)
            if ret:
                raise ExecutionError('Not able to force start volume to bring '
                                     'offline bricks online')
        if self.all_mount_procs:
            ret = wait_for_io_to_complete(self.all_mount_procs, self.mounts)
            if not ret:
                raise ExecutionError('Wait for IO completion failed')
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError('Failed to unmount and cleanup volume')
        self.get_super_method(self, 'tearDown')()

    def _get_free_space_in_gb(self, host, path):
        """
        Return available space on the provided `path`
        """
        space_avail = get_disk_usage(host, path)
        self.assertIsNotNone(
            space_avail, 'Failed to get disk usage stats of '
            '{} on {}'.format(host, path))
        return ceil(space_avail['free'])

    def _insert_bp(self, host, logpath):
        """
        Generates and inserts a breakpoint in the given logpath on the host
        """
        append_string = self.bp_text + str(self.bp_count)
        ret = append_string_to_file(host, logpath, append_string)
        self.assertTrue(
            ret, 'Not able to append string to the file {} '
            'on {}'.format(logpath, host))
        self.bp_count += 1

    def _fallocate_file(self):
        """
        Perform `fallocate -l <alloc_size> <fqpath>` on <client>
        """

        # Delete the file if exists (sparsefile is created on absolute sizes)
        ret = remove_file(self.client, self.fqpath, force=True)
        self.assertTrue(
            ret, 'Not able to delete existing file for '
            '`fallocate` of new file')
        sleep(5)
        ret, _, _ = g.run(
            self.client, 'fallocate -l {}G {}'.format(self.alloc_size,
                                                      self.fqpath))
        self.assertEqual(
            ret, 0, 'Not able to fallocate {}G to {} file on {}'.format(
                self.alloc_size, self.fqpath, self.client))

    def _validate_error_in_mount_log(self, pattern, exp_pre=True):
        """
        Validate type of error from mount log on setting quota
        """
        assert_method = self.assertTrue
        assert_msg = ('Fail: Not able to validate presence of "{}" '
                      'in mount log'.format(pattern))
        if not exp_pre:
            assert_method = self.assertFalse
            assert_msg = ('Fail: Not able to validate absence of "{}" '
                          'in mount log'.format(pattern))
        ret = search_pattern_in_file(self.client, pattern, self.logpath,
                                     self.bp_text + str(self.bp_count - 2),
                                     self.bp_text + str(self.bp_count - 1))
        assert_method(ret, assert_msg)

        # Validate against `quota list` command
        if 'quota' in pattern.lower():
            dir_path = '/dir/dir1'
            ret = quota_fetch_list(self.mnode, self.volname)
            self.assertIsNotNone(
                ret.get(dir_path),
                'Not able to get quota list for the path {}'.format(dir_path))
            ret = ret.get(dir_path)
            verified = False
            if ret['sl_exceeded'] is exp_pre and ret['hl_exceeded'] is exp_pre:
                verified = True
            self.assertTrue(
                verified, 'Failed to validate Quota list command against '
                'soft and hard limits')

    def _perform_quota_ops_before_brick_down(self):
        """
        Refactor of common test steps across three test functions
        """
        self.client, self.m_point = (self.mounts[0].client_system,
                                     self.mounts[0].mountpoint)
        ret = mkdir(self.client, '%s/dir/dir1' % self.m_point, parents=True)
        self.assertTrue(ret, 'Failed to create first dir on mountpoint')
        if self.num_of_dirs == 2:
            ret = mkdir(self.client, '%s/dir/dir' % self.m_point)
            self.assertTrue(ret, 'Failed to create second dir on mountpoint')

        # Types of errors
        self.space_error = 'Input/output error|No space left on device'
        self.quota_error = 'Disk quota exceeded'

        # Start IO from the clients
        cmd = ('/usr/bin/env python {} -n 10 -t 480 -d 10 -c 256 --dir '
               '{}/dir/dir{}')
        for count, mount in enumerate(self.mounts, start=1):
            proc = g.run_async(
                mount.client_system,
                cmd.format(self.script_path, mount.mountpoint, count))
            self.all_mount_procs.append(proc)

        # fallocate a large file and perform IO on remaining space
        self.free_disk_size = self._get_free_space_in_gb(
            self.client, self.m_point)
        self.fqpath = self.m_point + '/sparsefile'
        self.rem_size = 1  # Only 1G will be available to the mount
        self.alloc_size = self.free_disk_size - self.rem_size
        self._fallocate_file()

        # Insert breakpoint in the log
        self.bp_text = 'breakpoint_' + str(ceil(time())) + '_'
        self.bp_count = 1
        self.logpath = ('/var/log/glusterfs/mnt-' + self.volname +
                        '_glusterfs.log')
        self._insert_bp(self.client, self.logpath)

        # Create file with size greater than available mount space
        self.cmd = ('cd {}; cat /dev/urandom | tr -dc [:space:][:print:] '
                    '| head -c {}G > datafile_{};')
        self.fqpath = self.m_point + '/dir/dir1'
        proc = g.run_async(
            self.client,
            self.cmd.format(self.fqpath, self.rem_size * 2, self.bp_count))
        self.assertFalse(
            validate_io_procs([proc], self.mounts[0]),
            'Fail: Process should not allow data more '
            'than available space to be written')
        sleep(10)
        self._insert_bp(self.client, self.logpath)

        # Validate space error in the mount log
        self._validate_error_in_mount_log(pattern=self.space_error)

        # Enable quota and set all alert timeouts to 0secs
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Not able to enable quota on the volume')
        for alert_type, msg in ((quota_set_alert_time,
                                 'alert'), (quota_set_soft_timeout, 'soft'),
                                (quota_set_hard_timeout, 'hard')):
            ret, _, _ = alert_type(self.mnode, self.volname, '0sec')
            self.assertEqual(
                ret, 0, 'Failed to set quota {} timeout to 0sec'.format(msg))

        # Expose only 20G and set quota's on the dir
        self.rem_size = 20  # Only 20G will be available to whole mount
        self.alloc_size = self.free_disk_size - self.rem_size
        self.fqpath = self.m_point + '/sparsefile'
        self._fallocate_file()

        self._insert_bp(self.client, self.logpath)
        ret, _, _ = quota_limit_usage(self.mnode,
                                      self.volname,
                                      path='/dir/dir1',
                                      limit='10GB')
        self.assertEqual(ret, 0, 'Not able to set quota limit on /dir/dir1')
        if self.num_of_dirs == 2:
            ret, _, _ = quota_limit_usage(self.mnode,
                                          self.volname,
                                          path='/dir/dir2',
                                          limit='5GB')
            self.assertEqual(ret, 0, 'Not able to set quota limit on '
                             '/dir/dir2')

        # Write data more than available quota and validate error
        sleep(10)
        self.rem_size = 1  # Only 1G will be availble to /dir/dir1
        self.alloc_size = 9
        self.fqpath = self.m_point + '/dir/dir1/sparsefile'
        self._fallocate_file()

        self.fqpath = self.m_point + '/dir/dir1'
        proc = g.run_async(
            self.client,
            self.cmd.format(self.fqpath, self.rem_size * 2, self.bp_count))
        self.assertFalse(
            validate_io_procs([proc], self.mounts[0]),
            'Fail: Process should not allow data more '
            'than available space to be written')
        sleep(10)
        self._insert_bp(self.client, self.logpath)
        self._validate_error_in_mount_log(pattern=self.quota_error)
        self._validate_error_in_mount_log(pattern=self.space_error,
                                          exp_pre=False)

    def _perform_quota_ops_after_brick_down(self):
        """
        Refactor of common test steps across three test functions
        """
        # Increase the quota limit on dir/dir1 and validate no errors on writes
        self.alloc_size = self.free_disk_size - 50
        self.fqpath = self.m_point + '/sparsefile'
        self._fallocate_file()
        ret, _, _ = quota_limit_usage(self.mnode,
                                      self.volname,
                                      path='/dir/dir1',
                                      limit='40GB')
        self.assertEqual(ret, 0, 'Not able to expand quota limit on /dir/dir1')
        sleep(15)
        self._insert_bp(self.client, self.logpath)
        self.fqpath = self.m_point + '/dir/dir1'
        proc = g.run_async(
            self.client,
            self.cmd.format(self.fqpath, self.rem_size * 3, self.bp_count))
        self.assertTrue(
            validate_io_procs([proc], self.mounts[0]),
            'Fail: Not able to write data even after expanding quota limit')
        sleep(10)
        self._insert_bp(self.client, self.logpath)
        self._validate_error_in_mount_log(pattern=self.quota_error,
                                          exp_pre=False)
        self._validate_error_in_mount_log(pattern=self.space_error,
                                          exp_pre=False)

        # Decrease the quota limit and validate error on reaching quota
        self._insert_bp(self.client, self.logpath)
        ret, _, _ = quota_limit_usage(self.mnode,
                                      self.volname,
                                      path='/dir/dir1',
                                      limit='15GB')
        self.assertEqual(ret, 0, 'Not able to expand quota limit on /dir/dir1')
        sleep(10)
        self.fqpath = self.m_point + '/dir/dir1'
        self.rem_size = self._get_free_space_in_gb(self.client, self.fqpath)
        proc = g.run_async(
            self.client,
            self.cmd.format(self.fqpath, self.rem_size * 3, self.bp_count))
        self.assertFalse(
            validate_io_procs([proc], self.mounts[0]),
            'Fail: Process should not allow data more '
            'than available space to be written')
        sleep(10)
        self._insert_bp(self.client, self.logpath)
        self._validate_error_in_mount_log(pattern=self.quota_error)
        self._validate_error_in_mount_log(pattern=self.space_error,
                                          exp_pre=False)

    def test_ec_quota_errors_on_brick_down(self):
        """
        Steps:
        - Create and mount EC volume on two clients
        - Create two dirs on the mount and perform parallel IO from clients
        - Simulate disk full to validate EIO errors when no space is left
        - Remove simulation and apply different quota limits on two dirs
        - Bring down redundant bricks from the volume
        - Validate EDQUOTE error on reaching quota limit and extend quota to
          validate absence of EDQUOTE error
        - Reduce the quota limit and validate EDQUOTE error upon reaching quota
        - Remove quota limits, unmount and cleanup the volume
        """
        self._perform_quota_ops_before_brick_down()

        # Bring redundant bricks offline
        subvols = get_subvols(self.mnode, self.volname)
        self.assertTrue(subvols.get('volume_subvols'), 'Not able to get '
                        'subvols of the volume')
        self.offline_bricks = []
        for subvol in subvols['volume_subvols']:
            self.offline_bricks.extend(
                sample(subvol,
                       self.volume.get('voltype')['redundancy_count']))
        ret = bring_bricks_offline(self.volname, self.offline_bricks)
        self.assertTrue(ret, 'Not able to bring redundant bricks offline')

        self._perform_quota_ops_after_brick_down()

        # Bring offline bricks online
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, 'Not able to bring offline bricks online')
        self.offline_bricks *= 0

        g.log.info('Pass: Validating quota errors on brick down is successful')

    def test_ec_quota_errors_with_multiple_ios(self):
        """
        Steps:
        - Create and mount EC volume on two clients
        - Create two dirs on the mount and perform parallel IO from clients
        - Simulate disk full to validate EIO errors when no space is left
        - Remove simulation and apply quota limits on base dir
        - Validate EDQUOTE error on reaching quota limit and extend quota to
          validate absence of EDQUOTE error
        - Reduce the quota limit and validate EDQUOTE error upon reaching quota
        - Remove quota limits, unmount and cleanup the volume
        """
        self._perform_quota_ops_before_brick_down()
        self._perform_quota_ops_after_brick_down()
        g.log.info('Pass: Validating quota errors with multiple IOs is '
                   'successful')

    def test_ec_quota_errors_on_limit(self):
        """
        Steps:
        - Create and mount EC volume on one client
        - Create a dir on the mount and perform IO from clients
        - Simulate disk full to validate EIO errors when no space is left
        - Remove simulation and apply quota limits on the dir
        - Validate EDQUOTE error on reaching quota limit and extend quota to
          validate absence of EDQUOTE error
        - Reduce the quota limit and validate EDQUOTE error upon reaching quota
        - Remove quota limits, unmount and cleanup the volume
        """

        # Only a single client is used
        self._perform_quota_ops_before_brick_down()
        self._perform_quota_ops_after_brick_down()
        g.log.info('Pass: Validating quota errors on limit breach is '
                   'successful')
