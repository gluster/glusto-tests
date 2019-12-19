#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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

"""
Description:

Test Cases in this module tests the
Creation of clone from snapshot of volume.

"""
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.mount_ops import (mount_volume, umount_volume,
                                          is_mounted)
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.gluster.brick_libs import (
    get_all_bricks, are_bricks_online, bring_bricks_offline,
    select_bricks_to_bring_offline,
    get_offline_bricks_list, get_online_bricks_list, bring_bricks_online)
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.volume_libs import (
    cleanup_volume,
    get_subvols,
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_list,
                                         snap_activate,
                                         snap_clone)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed-replicated'],
          ['glusterfs']])
class SnapshotSelfheal(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        cls.snap = "snap1"
        cls.clone = "clone1"
        cls.mount1 = "/mnt/clone1"

    def setUp(self):

        # Uploading file_dir script in all client direcotries
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", self.clients[0])
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                   "file_dir_ops.py")
        ret = upload_scripts(self.clients[0], self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 self.clients[0])
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   self.clients[0])

        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_snap_self_heal(self):
        """
        Steps:

        1. create a volume
        2. mount volume
        3. create snapshot of that volume
        4. Activate snapshot
        5. Clone snapshot and Mount
        6. Perform I/O
        7. Bring Down Few bricks from volume without
           affecting the volume or cluster.
        8. Perform I/O
        9. Bring back down bricks to online
        10. Validate heal is complete with areequal

        """
        # pylint: disable=too-many-statements, too-many-locals
        # Creating snapshot:
        g.log.info("Starting to Create snapshot")
        ret, _, _ = snap_create(self.mnode, self.volname, self.snap)
        self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                  % self.volname))
        g.log.info("Snapshot %s created successfully for volume %s", self.snap,
                   self.volname)

        # Activating snapshot
        g.log.info("Starting to Activate Snapshot")
        ret, _, _ = snap_activate(self.mnode, self.snap)
        self.assertEqual(ret, 0, ("Failed to Activate snapshot %s"
                                  % self.snap))
        g.log.info("Snapshot %s activated successfully", self.snap)

        # snapshot list
        ret, _, _ = snap_list(self.mnode)
        self.assertEqual(ret, 0, ("Failed to list all the snapshot"))
        g.log.info("Snapshot list command was successful")

        # Creating a Clone volume from snapshot:
        g.log.info("Starting to Clone volume from Snapshot")
        ret, _, _ = snap_clone(self.mnode, self.snap, self.clone)
        self.assertEqual(ret, 0, ("Failed to clone %s from snapshot %s"
                                  % (self.clone, self.snap)))
        g.log.info("%s created successfully", self.clone)

        #  start clone volumes
        g.log.info("start to created clone volumes")
        ret, _, _ = volume_start(self.mnode, self.clone)
        self.assertEqual(ret, 0, "Failed to start clone %s" % self.clone)
        g.log.info("clone volume %s started successfully", self.clone)

        # Mounting a clone volume
        g.log.info("Mounting a clone volume")
        ret, _, _ = mount_volume(self.clone, self.mount_type, self.mount1,
                                 self.mnode, self.clients[0])
        self.assertEqual(ret, 0, "Failed to mount clone Volume %s"
                         % self.clone)
        g.log.info("Clone volume %s mounted Successfully", self.clone)

        # Checking cloned volume mounted or not
        ret = is_mounted(self.clone, self.mount1, self.mnode,
                         self.clients[0], self.mount_type)
        self.assertTrue(ret, "Failed to mount clone volume on mount point: %s"
                        % self.mount1)
        g.log.info("clone Volume %s mounted on %s", self.clone, self.mount1)

        # write files on all mounts
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mount1)
        all_mounts_procs = []
        cmd = ("/usr/bin/env python%d %s create_files "
               "-f 10 --base-file-name file %s" % (
                   sys.version_info.major, self.script_upload_path,
                   self.mount1))
        proc = g.run(self.clients[0], cmd)
        all_mounts_procs.append(proc)
        g.log.info("Successful in creating I/O on mounts")

        # get the bricks from the volume
        g.log.info("Fetching bricks for the volume : %s", self.clone)
        bricks_list = get_all_bricks(self.mnode, self.clone)
        g.log.info("Brick List : %s", bricks_list)

        # Select bricks to bring offline
        g.log.info("Starting to bring bricks to offline")
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = list(filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks'])))
        g.log.info("Brick to bring offline: %s ", bricks_to_bring_offline)
        ret = bring_bricks_offline(self.clone, bricks_to_bring_offline)
        self.assertTrue(ret, "Failed to bring the bricks offline")
        g.log.info("Successful in bringing bricks: %s offline",
                   bricks_to_bring_offline)

        # Offline Bricks list
        offline_bricks = get_offline_bricks_list(self.mnode, self.clone)
        self.assertIsNotNone(offline_bricks, "Failed to get offline bricklist"
                             "for volume %s" % self.clone)
        for bricks in offline_bricks:
            self.assertIn(bricks, bricks_to_bring_offline,
                          "Failed to validate "
                          "Bricks offline")
        g.log.info("Bricks Offline: %s", offline_bricks)

        # Online Bricks list
        online_bricks = get_online_bricks_list(self.mnode, self.clone)
        self.assertIsNotNone(online_bricks, "Failed to get online bricks"
                             " for volume %s" % self.clone)
        g.log.info("Bricks Online: %s", online_bricks)

        # write files mountpoint
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mount1)
        all_mounts_procs = []
        cmd = ("/usr/bin/env python%d %s create_files "
               "-f 10 --base-file-name file %s" % (
                   sys.version_info.major, self.script_upload_path,
                   self.mount1))
        proc = g.run(self.clients[0], cmd)
        all_mounts_procs.append(proc)
        g.log.info("Successful in creating I/O on mounts")

        # Bring all bricks online
        g.log.info("bring all bricks online")
        ret = bring_bricks_online(self.mnode, self.clone,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, "Failed to bring bricks online")
        g.log.info("Successful in bringing all bricks online")

        # Validate Bricks are online
        g.log.info("Validating all bricks are online")
        ret = are_bricks_online(self.mnode, self.clone, bricks_list)
        self.assertTrue(ret, "Failed to bring all the bricks online")
        g.log.info("bricks online: %s", bricks_list)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.clone)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online" % self.clone))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.clone)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.clone)
        self.assertTrue(ret, ("Volume %s : All process are not online"
                              % self.clone))
        g.log.info("Volume %s : All process are online", self.clone)

        # wait for the heal process to complete
        g.log.info("waiting for heal process to complete")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to complete the heal process")
        g.log.info("Successfully completed heal process")

        # Check areequal
        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.clone)
        subvols = get_subvols(self.mnode, self.clone)
        num_subvols = len(subvols['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s:", num_subvols)

        # Get arequals and compare
        g.log.info("Starting to Compare areequals")
        for i in range(0, num_subvols):
            # Get arequal for first brick
            subvol_brick_list = subvols['volume_subvols'][i]
            node, brick_path = subvol_brick_list[0].split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan'
                       % brick_path)
            ret, arequal, _ = g.run(node, command)
            first_brick_total = arequal.splitlines()[-1].split(':')[-1]

        # Get arequal for every brick and compare with first brick
        for brick in subvol_brick_list:
            node, brick_path = brick.split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan'
                       % brick_path)
            ret, brick_arequal, _ = g.run(node, command)
            self.assertFalse(ret,
                             'Failed to get arequal on brick %s'
                             % brick)
            g.log.info('Getting arequal for %s is successful', brick)
            brick_total = brick_arequal.splitlines()[-1].split(':')[-1]
            self.assertEqual(first_brick_total, brick_total,
                             'Arequals for subvol and %s are not equal'
                             % brick)
            g.log.info('Arequals for subvol and %s are equal', brick)
        g.log.info('All arequals are equal for distributed-replicated')

    def tearDown(self):

        # Cleanup and umount cloned volume
        g.log.info("Starting to umount Volume")
        ret = umount_volume(self.clients[0], self.mount1)
        if not ret:
            raise ExecutionError("Failed to unmount the cloned volume")
        g.log.info("Successfully Unmounted the cloned volume")
        g.log.info("Starting to cleanup volume")
        ret = cleanup_volume(self.mnode, self.clone)
        if not ret:
            raise ExecutionError("Failed to cleanup the cloned volume")
        g.log.info("Successful in cleanup Cloned volume")

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")
