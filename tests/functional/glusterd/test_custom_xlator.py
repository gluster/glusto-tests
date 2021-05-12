#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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

import os
from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.gluster_init import get_gluster_version
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.lib_utils import is_rhel7
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (
    expand_volume, get_subvols, log_volume_info_and_status,
    replace_brick_from_volume, wait_for_volume_process_to_be_online)
from glustolibs.gluster.volume_ops import (get_volume_info, set_volume_options,
                                           volume_reset, volume_start,
                                           volume_stop)
from glustolibs.io.utils import list_all_files_and_dirs_mounts


@runs_on([[
    'distributed', 'arbiter', 'replicated', 'dispersed', 'distributed-arbiter',
    'distributed-replicated', 'distributed-dispersed'
], ['glusterfs', 'nfs']])
# pylint: disable=too-many-statements
class TestCustomXlator(GlusterBaseClass):
    '''
    Description:
    - Use read-only xlator as a custom xlator to test custom-xlator
    framework introduced in https://github.com/gluster/glusterfs/pull/1974 and
    https://github.com/gluster/glusterfs/pull/2371

    Out of scope (or probable bugs):
    - Options validation of xlator loaded using custom xlator framework
    - Using existing xlator name for custom xlator in nfs-ganesha
    - Using custom xlator as parent for another custom/existing xlator
    '''
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        version = get_gluster_version(cls.mnode).strip()
        xlator_dir = '/usr/lib64/glusterfs/{0}/xlator'.format(version)
        cls.usr_dir = os.path.join(xlator_dir, 'user', '')

        # Create 'user' dir and copy read-only xlator as 'ro' and 'posix' as
        # a custom xlator, 'posix' is used to validate negative scenario
        cmd = ('mkdir -p {0} && cp {1}/features/read-only.so '
               '{1}/user/ro.so && cp {1}/user/ro.so {1}/user/posix.so'.format(
                   cls.usr_dir, xlator_dir))

        out = g.run_parallel(cls.servers, cmd)
        for host in out:
            ret, _, _ = out[host]
            if ret:
                raise ExecutionError('Unable to create user xlator directory '
                                     'or failed to copy custom xlator')

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # A single mount is enough for all the tests
        self.mounts = self.mounts[0:1]
        self.client = self.mounts[0].client_system
        self.m_point = self.mounts[0].mountpoint

        # Command to run minimal IO
        self.io_cmd = 'cat /dev/urandom | tr -dc [:space:][:print:] | head -c '

        if not self.setup_volume_and_mount_volume(mounts=self.mounts):
            raise ExecutionError('Failed to setup and mount '
                                 '{}'.format(self.volname))

        self.timeout = 5 if is_rhel7(self.mnode) else 2

    def tearDown(self):
        if not self.unmount_volume_and_cleanup_volume(mounts=self.mounts):
            raise ExecutionError('Not able to unmount and cleanup '
                                 '{}'.format(self.volname))
        self.get_super_method(self, 'tearDown')()

    @classmethod
    def tearDownClass(cls):
        # Delete user xlator directory
        cmd = 'rm -rf {0}'.format(cls.usr_dir)
        out = g.run_parallel(cls.servers, cmd)
        for host in out:
            ret, _, _ = out[host]
            if ret:
                raise ExecutionError('Unable to delete user xlator directory')

        cls.get_super_method(cls, 'tearDownClass')()

    def _simple_io(self, xfail=False):
        '''Writes a simple random IO on mount'''
        cmd = ('cd %s; mkdir -p $RANDOM && cd $_; for i in $(seq 1 5); do '
               '%s ${RANDOM:0:4}K > ${RANDOM}_$i; echo file $i created; done' %
               (self.m_point, self.io_cmd))
        # STDERR will be empty on success
        _, _, err = g.run(self.client, cmd)
        assert_method = self.assertFalse
        assert_msg = 'Unable to perform IO on the client'
        if xfail:
            assert_method = self.assertTrue
            assert_msg = 'Should not be able to perform IO on the client'
        assert_method(err, assert_msg)

    def _verify_position(self, xlator, parent, xtype):
        '''Verify 'xlator' subvolume entry matches 'parent' xlator on all
        bricks'''
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        # 'xlator' type to be verified in volfile
        usr_vol = 'type {0}/{1}'.format(xtype, xlator)
        # 'parent xlator' position to be verified
        parent_vol = 'subvolumes {0}-{1}'.format(self.volname, parent)
        for subvol in subvols:
            for brick in subvol:
                # Don't mutate while iterating, so take a ref to list
                if brick not in self.verified_bricks[:]:
                    host = brick[:brick.find(':')]

                    # Construct volfile name using available info
                    volfile = '/var/lib/glusterd/vols/{0}/{0}.{1}.vol'.format(
                        self.volname,
                        brick.replace(':/', '.').replace('/', '-'))

                    # Get 'subgraph' wrt 'xlator' in volfile
                    cmd = ("sed -n '/^volume %s-%s/,${p;/^end-volume/q}' %s" %
                           (self.volname, xlator, volfile))
                    ret, out, _ = g.run(host, cmd)
                    self.assertEqual(
                        ret, 0, 'Unable to query vol file for {0} '
                        'xlator in {1}'.format(xtype, volfile))
                    self.assertIn(
                        usr_vol, out,
                        'Unable to find {0} in {1}'.format(usr_vol, volfile))
                    self.assertIn(
                        parent_vol, out, 'Parent for {0} should be {1} '
                        'in {2}'.format(usr_vol, parent_vol, volfile))
                    # No need to verify on bricks which are already verfied
                    # Useful in add and replace brick ops
                    self.verified_bricks.append(brick)

    def _set_and_assert_volume_option(self, key, value, xfail=False):
        '''Set and assert volume option'''
        ret = set_volume_options(self.mnode, self.volname, {key: value})
        assert_method = self.assertTrue
        assert_msg = 'Unable to set {0} to {1}'.format(key, value)
        if xfail:
            assert_method = self.assertFalse
            assert_msg = 'Should not be able to set {0} to {1}'.format(
                key, value)
        assert_method(ret, assert_msg)

    def _enable_xlator(self, xlator, parent, xtype, xsfail=False):
        self.verified_bricks = []
        option = '{0}{1}.{2}'.format(xtype,
                                     '.xlator' if xtype == 'user' else '',
                                     xlator)
        self._set_and_assert_volume_option(option, parent)
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Unable to stop volume')
        sleep(self.timeout)
        ret, _, _ = volume_start(self.mnode, self.volname)
        if xsfail:
            self.assertNotEqual(ret, 0, 'Expected volume start to fail')
            return
        self.assertEqual(ret, 0, 'Unable to start a stopped volume')
        self._verify_position(xlator, parent, xtype)
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(
            ret, 'Not all volume processes are online after '
            'starting a stopped volume')
        sleep(self.timeout)

    def test_custom_xlator_ops(self):
        '''
        Steps:
        - Perform minimal IO on the mount
        - Enable custom xlator and verify xlator position in the volfile
        - After performing any operation on the custom xlator set options using
          'storage.reserve' to validate other xlators aren't effected
        - Add brick to the volume and verify the xlator position in volfile in
          the new brick
        - Replace brick and verify the xlator position in new brick volfile
        - Verify debug xlator is reflected correctly in the volfile when set
        - Validate unexisting xlator position should fail
        - Reset the volume and verify all the options set above are reset

        For more details refer inline comments
        '''

        # Write IO on the mount
        self._simple_io()

        # Set storage.reserve option, just a baseline that set options are
        # working
        self._set_and_assert_volume_option('storage.reserve', '2%')

        # Test mount is accessible in RW
        self._simple_io()

        # Position custom xlator in the graph
        xlator, parent, xtype = 'ro', 'worm', 'user'
        self._enable_xlator(xlator, parent, xtype)

        # Verify mount is accessible as we didn't set any options yet
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, 'Failed to list all files and dirs')

        # Set 'read-only' to 'on'
        self._set_and_assert_volume_option('user.xlator.ro.read-only', 'on')

        # Functional verification that mount should be RO
        self._simple_io(xfail=True)
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, 'Failed to list all files and dirs')

        # Shouldn't effect other xlator options
        self._set_and_assert_volume_option('storage.reserve', '3%')

        # Functional validation that mount should be RW
        self._set_and_assert_volume_option('user.xlator.ro.read-only', 'off')
        self._simple_io()

        # Shouldn't effect other xlator options
        self._set_and_assert_volume_option('storage.reserve', '4%')

        # Add brick to the volume and new brick volfile should have custom
        # xlator
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, 'Unable to expand volume')
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, 'Unable to log volume info and status')
        self._verify_position(xlator, parent, xtype)
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(
            ret, 0, 'Unable to start rebalance operaiont post '
            'expanding volume')
        sleep(.5)
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Rebalance on the volume is not completed')

        # Replace on 'pure distribute' isn't recommended
        if self.volume['voltype']['type'] != 'distributed':

            # Replace brick and new brick volfile should have custom xlator
            ret = replace_brick_from_volume(self.mnode, self.volname,
                                            self.servers,
                                            self.all_servers_info)
            self.assertTrue(ret, 'Unable to perform replace brick operation')
            self._verify_position(xlator, parent, xtype)
            ret = monitor_heal_completion(self.mnode, self.volname)
            self.assertTrue(
                ret, 'Heal is not yet completed after performing '
                'replace brick operation')

        # Regression cases
        # Framework should fail when non existing xlator position is supplied
        self._set_and_assert_volume_option('user.xlator.ro',
                                           'unknown',
                                           xfail=True)

        # Any failure in setting xlator option shouldn't result in degraded
        # volume
        self._simple_io()
        self._set_and_assert_volume_option('storage.reserve', '5%')

        # Custom xlator framework touches existing 'debug' xlators and minimal
        # steps to verify no regression
        xlator, parent, xtype = 'delay-gen', 'posix', 'debug'
        self._enable_xlator(xlator, parent, xtype)

        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, 'Failed to list all files and dirs')

        # Volume shouldn't be able to start on using same name for custom
        # xlator and existing xlator
        if self.mount_type != 'nfs':
            xlator, parent, xtype = 'posix', 'posix', 'user'
            self._enable_xlator(xlator, parent, xtype, xsfail=True)

        # Volume reset should remove all the options that are set upto now
        ret, _, _ = volume_reset(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Unable to reset volume')

        # Volume start here is due to earlier failure starting the volume and
        # isn't related to 'volume_reset'
        if self.mount_type != 'nfs':
            ret, _, _ = volume_start(self.mnode, self.volname)
            self.assertEqual(ret, 0, 'Unable to start a stopped volume')
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(
            ret, 'Not all volume processes are online after '
            'starting a stopped volume')
        sleep(self.timeout)
        self._simple_io()

        # Verify options are reset
        vol_info = get_volume_info(self.mnode, self.volname)
        options = vol_info[self.volname]['options']
        negate = ['user.xlator.ro', 'debug.delay-gen', 'storage.reserve']
        for option in negate:
            self.assertNotIn(
                option, options, 'Found {0} in volume info even '
                'after volume reset'.format(option))

        g.log.info(
            'Pass: Validating custom xlator framework for volume %s '
            'is successful', self.volname)
