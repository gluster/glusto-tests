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

from random import choice

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           are_bricks_online, get_all_bricks)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.lib_utils import collect_bricks_arequal
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.io.utils import collect_mounts_arequal, run_linux_untar


@runs_on([['distributed-replicated', 'replicated'], ['glusterfs']])
class TestCompileLinuxKernelWithSelfHeal(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        self.first_client = self.mounts[0].client_system
        self.mountpoint = self.mounts[0].mountpoint

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to setup and mount volume")

    def tearDown(self):

        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _untar_linux_kernel_in_a_specific_dir(self):
        """A function to create files and dirs on mount point"""
        # Create a parent directory test_link_self_heal on mount point
        ret = mkdir(self.first_client,
                    '{}/{}'.format(self.mountpoint, 'test_self_heal'))
        self.assertTrue(ret, "Failed to create dir test_self_heal")

        # Start linux untar on dir linuxuntar
        proc = run_linux_untar(self.clients[0], self.mounts[0].mountpoint,
                               dirs=tuple(['test_self_heal']))[0]
        try:
            ret, _, _ = proc.async_communicate()
            if not ret:
                untar_done = False
            untar_done = True
        except ValueError:
            untar_done = True
        self.assertTrue(untar_done,
                        "Kernel untar not done on client mount point")

    def _bring_bricks_offline(self):
        """Brings bricks offline and confirms if they are offline"""
        # Select bricks to bring offline from a replica set
        subvols_dict = get_subvols(self.mnode, self.volname)
        subvols = subvols_dict['volume_subvols']
        self.bricks_to_bring_offline = []
        self.bricks_to_bring_offline.append(choice(subvols[0]))

        # Bring bricks offline
        ret = bring_bricks_offline(self.volname, self.bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        self.bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 self.bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % self.bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   self.bricks_to_bring_offline)

    def _restart_volume_and_bring_all_offline_bricks_online(self):
        """Restart volume and bring all offline bricks online"""
        ret = bring_bricks_online(self.mnode, self.volname,
                                  self.bricks_to_bring_offline,
                                  bring_bricks_online_methods=[
                                      'volume_start_force'])
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        self.bricks_to_bring_offline)

        # Check if bricks are back online or not
        ret = are_bricks_online(self.mnode, self.volname,
                                self.bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks not online %s even after restart' %
                        self.bricks_to_bring_offline)

        g.log.info('Bringing bricks %s online is successful',
                   self.bricks_to_bring_offline)

    def _check_arequal_on_bricks_with_a_specific_arequal(self, arequal,
                                                         brick_list):
        """
        Compare an inital arequal checksum with bricks from a given brick list
        """
        init_val = arequal[0].splitlines()[-1].split(':')[-1]
        ret, arequals = collect_bricks_arequal(brick_list)
        self.assertTrue(ret, 'Failed to get arequal on bricks')
        for brick_arequal in arequals:
            brick_total = brick_arequal.splitlines()[-1].split(':')[-1]
            self.assertEqual(init_val, brick_total, 'Arequals not matching')

    @staticmethod
    def _add_dir_path_to_brick_list(brick_list):
        """Add test_self_heal at the end of brick path"""
        dir_brick_list = []
        for brick in brick_list:
            dir_brick_list.append('{}/{}'.format(brick,
                                                 'test_self_heal'))
        return dir_brick_list

    def _check_arequal_checksum_for_the_volume(self):
        """
        Check if arequals of mount point and bricks are
        are the same.
        """
        if self.volume_type == "replicated":
            # Check arequals for "replicated"
            brick_list = get_all_bricks(self.mnode, self.volname)
            dir_brick_list = self._add_dir_path_to_brick_list(brick_list)

            # Get arequal before getting bricks offline
            work_dir = '{}/test_self_heal/'.format(self.mountpoint)
            ret, arequals = collect_mounts_arequal([self.mounts[0]],
                                                   path=work_dir)
            self.assertTrue(ret, 'Failed to get arequal')
            g.log.info('Getting arequal before getting bricks offline '
                       'is successful')

            # Get arequal on bricks and compare with mount_point_total
            self._check_arequal_on_bricks_with_a_specific_arequal(
                arequals, dir_brick_list)

        # Check arequals for "distributed-replicated"
        if self.volume_type == "distributed-replicated":
            # Get the subvolumes
            subvols_dict = get_subvols(self.mnode, self.volname)
            num_subvols = len(subvols_dict['volume_subvols'])

            # Get arequals and compare
            for i in range(0, num_subvols):
                # Get arequal for first brick
                brick_list = subvols_dict['volume_subvols'][i]
                dir_brick_list = self._add_dir_path_to_brick_list(brick_list)
                ret, arequals = collect_bricks_arequal([dir_brick_list[0]])
                self.assertTrue(ret, 'Failed to get arequal on first brick')

                # Get arequal for every brick and compare with first brick
                self._check_arequal_on_bricks_with_a_specific_arequal(
                    arequals, dir_brick_list)

    def test_compile_linux_kernel_with_self_heal(self):
        """
        Test Case:
        1. Create a volume of any type, start it and mount it.
        2. Perform linux untar on the mount point and wait for it to complete.
        3. Bring down brick processes of volume.
        4. Compile linux kernel on monut point.
        5. Bring offline bricks back online.
        6. Wait for heal to complete.
        7. Check arequal checksum on bricks and make sure there is
           no data loss.

        Note:
        Run the below command before this test:
        yum install elfutils-libelf-devel openssl-devel git fakeroot
        ncurses-dev xz-utils libssl-dev bc flex libelf-dev bison

        OR

        dnf install make flex bison elfutils-libelf-devel openssl-devel

        The below test takes anywhere between 27586.94 sec to 35396.90 sec,
        as kernel compilation is a very slow process in general.
        """
        # Perform linux untar on the mount point and wait for it to complete
        self._untar_linux_kernel_in_a_specific_dir()

        # Bring down brick processes of volume
        self._bring_bricks_offline()

        # Compile linux kernel on monut point
        cmd = ('cd {}/{}/linux-5.4.54/; make defconfig; make'.format(
            self.mountpoint, 'test_self_heal'))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to compile linux kernel")
        g.log.info("linux kernel compliation successful")

        # Bring offline bricks back online
        self._restart_volume_and_bring_all_offline_bricks_online()

        # Wait for heal to complete
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=3600)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check arequal checksum on bricks and make sure there is
        # no data loss
        self._check_arequal_checksum_for_the_volume()
