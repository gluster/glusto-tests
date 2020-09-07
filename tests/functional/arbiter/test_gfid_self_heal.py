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
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestGFIDSelfHeal(GlusterBaseClass):

    """
    Description:
        Arbiter Test cases related to GFID self heal
    """
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

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

    def test_gfid_self_heal(self):
        """
        Test GFID self heal
        Description:
        - Creating directory test_compilation
        - Write Deep directories and files
        - Get arequal before getting bricks offline
        - Select bricks to bring offline
        - Bring brick offline
        - Delete directory on mountpoint where data is writte
        - Create the same directory and write same data
        - Bring bricks online
        - Wait for volume processes to be online
        - Verify volume's all process are online
        - Monitor heal completion
        - Check for split-brain
        - Get arequal after getting bricks online
        """
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        # Creating directory test_compilation
        ret = mkdir(self.mounts[0].client_system, "{}/test_gfid_self_heal"
                    .format(self.mounts[0].mountpoint))
        self.assertTrue(ret, "Failed to create directory")
        g.log.info("Directory 'test_gfid_self_heal' on %s created "
                   "successfully", self.mounts[0])

        # Write Deep directories and files
        count = 1
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d --dir-depth 2 "
                   "--dir-length 10 --max-num-of-dirs 5 "
                   "--num-of-files 5 %s/dir1" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            ret, _, _ = g.run(self.mounts[0].client_system, cmd)
            self.assertEqual(ret, 0, "Failed to create files on mountpoint")
            g.log.info("Successfully created files on mountpoint")
            count += 10

        # Get arequal before getting bricks offline
        ret, result_before_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Arequal after getting bricks offline '
                   'is %s', result_before_offline)

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

        # Delete directory on mountpoint where data is written
        cmd = ('rm -rf -v %s/test_gfid_self_heal' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to delete directory")
        g.log.info("Directory deleted successfully for %s", self.mounts[0])

        # Create the same directory and write same data
        ret = mkdir(self.mounts[0].client_system, "{}/test_gfid_self_heal"
                    .format(self.mounts[0].mountpoint))
        self.assertTrue(ret, "Failed to create directory")
        g.log.info("Directory 'test_gfid_self_heal' on %s created "
                   "successfully", self.mounts[0])

        # Write the same files again
        count = 1
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d --dir-depth 2 "
                   "--dir-length 10 --max-num-of-dirs 5 "
                   "--num-of-files 5 %s/dir1" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            ret, _, _ = g.run(self.mounts[0].client_system, cmd)
            self.assertEqual(ret, 0, "Failed to create files on mountpoint")
            g.log.info("Successfully created files on mountpoint")
            count += 10

        # Bring bricks online
        ret = bring_bricks_online(
            self.mnode, self.volname,
            bricks_to_bring_offline,
            bring_bricks_online_methods=['volume_start_force'])
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
