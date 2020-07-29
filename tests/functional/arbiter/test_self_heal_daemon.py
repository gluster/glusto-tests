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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.brick_libs import (
    select_volume_bricks_to_bring_offline,
    bring_bricks_offline,
    bring_bricks_online,
    are_bricks_offline)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_volume_in_split_brain)
from glustolibs.io.utils import (collect_mounts_arequal)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import get_file_stat


@runs_on([['arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestSelfHealDaemon(GlusterBaseClass):
    """
    Description:
        Arbiter Test cases related to self heal
        of data and hardlink
    """
    def setUp(self):
        # Calling GlusterBaseClass
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_self_heal_daemon(self):
        """
        Test Data-Self-Heal(heal command)
        Description:
        - Create directory test_hardlink_self_heal
        - Create directory test_data_self_heal
        - Creating files for hardlinks and data files
        - Get arequal before getting bricks offline
        - Select bricks to bring offline
        - Bring brick offline
        - Create hardlinks and append data to data files
        - Bring brick online
        - Wait for volume processes to be online
        - Verify volume's all process are online
        - Monitor heal completion
        - Check for split-brain
        - Get arequal after getting bricks online
        - Select bricks to bring offline
        - Bring brick offline
        - Truncate data to data files and verify hardlinks
        - Bring brick online
        - Wait for volume processes to be online
        - Verify volume's all process are online
        - Monitor heal completion
        - Check for split-brain
        - Get arequal again

        """
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        # Creating directory test_hardlink_self_heal
        ret = mkdir(self.mounts[0].client_system, "{}/test_hardlink_self_heal"
                    .format(self.mounts[0].mountpoint))
        self.assertTrue(ret, "Failed to create directory")
        g.log.info("Directory 'test_hardlink_self_heal' on %s created "
                   "successfully", self.mounts[0])

        # Creating directory test_data_self_heal
        ret = mkdir(self.mounts[0].client_system, "{}/test_data_self_heal"
                    .format(self.mounts[0].mountpoint))
        self.assertTrue(ret, "Failed to create directory")
        g.log.info("Directory test_hardlink_self_heal on %s created "
                   "successfully", self.mounts[0])

        # Creating files for hardlinks and data files
        cmd = ('cd %s/test_hardlink_self_heal;for i in `seq 1 5`;'
               'do mkdir dir.$i ; for j in `seq 1 10` ; do dd if='
               '/dev/urandom of=dir.$i/file.$j bs=1k count=$j;done; done;'
               'cd ..' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create file on mountpoint")
        g.log.info("Successfully created files on mountpoint")

        cmd = ('cd %s/test_data_self_heal;for i in `seq 1 100`;'
               'do dd if=/dev/urandom of=file.$i bs=128K count=$i;done;'
               'cd ..' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create file on mountpoint")
        g.log.info("Successfully created files on mountpoint")

        # Get arequal before getting bricks offline
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Arequal before getting bricks online-%s',
                   result_before_online)

        # Select bricks to bring offline
        bricks_to_bring_offline = select_volume_bricks_to_bring_offline(
            self.mnode, self.volname)
        self.assertIsNotNone(bricks_to_bring_offline, "List is empty")

        # Bring brick offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks {} offline'.
                        format(bricks_to_bring_offline))

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks {} are not offline'.
                        format(bricks_to_bring_offline))
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Append data to data files and create hardlinks
        cmd = ('cd %s/test_data_self_heal;for i in `seq 1 100`;'
               'do dd if=/dev/urandom of=file.$i bs=512K count=$i ; done ;'
               'cd .. ' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to modify data files.")
        g.log.info("Successfully modified data files")

        cmd = ('cd %s/test_hardlink_self_heal;for i in `seq 1 5` ;do '
               'for j in `seq 1 10`;do ln dir.$i/file.$j dir.$i/link_file.$j;'
               'done ; done ; cd .. ' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Hardlinks creation failed")
        g.log.info("Successfully created hardlinks of files")

        # Bring bricks online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks {} online'.format
                        (bricks_to_bring_offline))
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume {} processes to "
                              "be online".format(self.volname)))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume {} : All process are not online".format
                              (self.volname)))
        g.log.info("Volume %s : All process are online", self.volname)

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')

        # Get arequal after getting bricks online
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Arequal after getting bricks online '
                   'is %s', result_after_online)

        # Select bricks to bring offline
        bricks_to_bring_offline = select_volume_bricks_to_bring_offline(
            self.mnode, self.volname)
        self.assertIsNotNone(bricks_to_bring_offline, "List is empty")

        # Bring brick offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks {} offline'.format
                        (bricks_to_bring_offline))

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks {} are not offline'.format
                        (bricks_to_bring_offline))
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Truncate data to data files and verify hardlinks
        cmd = ('cd %s/test_data_self_heal ; for i in `seq 1 100` ;'
               'do truncate -s $(( $i * 128)) file.$i ; done ; cd ..'
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to truncate files")
        g.log.info("Successfully truncated files on mountpoint")

        file_path = ('%s/test_hardlink_self_heal/dir{1..5}/file{1..10}'
                     % (self.mounts[0].mountpoint))
        link_path = ('%s/test_hardlink_self_heal/dir{1..5}/link_file{1..10}'
                     % (self.mounts[0].mountpoint))
        file_stat = get_file_stat(self.mounts[0], file_path)
        link_stat = get_file_stat(self.mounts[0], link_path)
        self.assertEqual(file_stat, link_stat, "Verification of hardlinks "
                         "failed")
        g.log.info("Successfully verified hardlinks")

        # Bring brick online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks {} online'.format
                        (bricks_to_bring_offline))
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume {} processes to "
                              "be online".format(self.volname)))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume {} : All process are not online".format
                              (self.volname)))
        g.log.info("Volume %s : All process are online", self.volname)

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')
