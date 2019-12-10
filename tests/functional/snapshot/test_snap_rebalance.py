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
Creation of clone from snapshot of one volume.

"""
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.mount_ops import mount_volume, is_mounted
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.gluster.volume_libs import cleanup_volume, expand_volume
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              rebalance_status,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_activate,
                                         snap_clone)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed', 'distributed-replicated'],
          ['glusterfs']])
class SnapshotRebalance(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        cls.snap = "snap0"
        cls.clone = "clone1"
        cls.mount1 = "/mnt/clone1"
        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients ")
        g.log.info("Successfully uploaded IO scripts to clients")

    def check_arequal(self):
        # Check arequals
        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.clone)
        subvols_dict = get_subvols(self.mnode, self.clone)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s:", num_subvols)

        # Get arequals and compare
        for i in range(0, num_subvols):
            # Get arequal for first brick
            subvol_brick_list = subvols_dict['volume_subvols'][i]
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
        g.log.info('All arequals are equal for %s', self.volname)

    def setUp(self):

        # SetUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_snap_rebalance(self):
        # pylint: disable=too-many-statements, too-many-locals
        """

        Snapshot rebalance contains tests which verifies snapshot clone,
        creating snapshot and performing I/O on mountpoints

        Steps:

        1. Create snapshot of a volume
        2. Activate snapshot
        3. Clone snapshot and Activate
        4. Mount Cloned volume
        5. Perform I/O on mount point
        6. Calculate areequal for bricks and mountpoints
        7. Add-brick more brick to cloned volume
        8. Initiate Re-balance
        9. validate areequal of bricks and mountpoints
        """

        # Creating snapshot:
        g.log.info("Starting to Create snapshot")
        ret, _, _ = snap_create(self.mnode, self.volname, self.snap)
        self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                  % self.volname))
        g.log.info("Snapshot %s created successfully for volume %s",
                   self.snap, self.volname)

        # Activating snapshot
        g.log.info("Starting to Activate Snapshot")
        ret, _, _ = snap_activate(self.mnode, self.snap)
        self.assertEqual(ret, 0, ("Failed to Activate snapshot %s"
                                  % self.snap))
        g.log.info("Snapshot %s activated successfully", self.snap)

        # Creating a Clone of snapshot:
        g.log.info("creating Clone Snapshot")
        ret, _, _ = snap_clone(self.mnode, self.snap, self.clone)
        self.assertEqual(ret, 0, ("Failed to clone volume %s" % self.clone))
        g.log.info("clone volume %s created successfully", self.clone)

        # Starting clone volume
        g.log.info("starting clone volume")
        ret, _, _ = volume_start(self.mnode, self.clone)
        self.assertEqual(ret, 0, "Failed to start %s" % self.clone)
        g.log.info("clone volume %s started successfully", self.clone)

        # Mounting a clone volume
        g.log.info("Mounting created clone volume")
        ret, _, _ = mount_volume(self.clone, self.mount_type, self.mount1,
                                 self.mnode, self.clients[0])
        self.assertEqual(ret, 0, "clone Volume mount failed for %s"
                         % self.clone)
        g.log.info("cloned volume %s mounted Successfully", self.clone)

        # Validate clone volume mounted or not
        g.log.info("Validate clone volume mounted or not")
        ret = is_mounted(self.clone, self.mount1, self.mnode,
                         self.clients[0], self.mount_type)
        self.assertTrue(ret, "Cloned Volume not mounted on mount point: %s"
                        % self.mount1)
        g.log.info("Cloned Volume %s mounted on %s", self.clone, self.mount1)

        # write files to mountpoint
        g.log.info("Starting IO on %s mountpoint...", self.mount1)
        all_mounts_procs = []
        cmd = ("/usr/bin/env python%d %s create_files "
               "-f 10 --base-file-name file %s" % (
                   sys.version_info.major, self.script_upload_path,
                   self.mount1))
        proc = g.run(self.clients[0], cmd)
        all_mounts_procs.append(proc)

        self.check_arequal()

        # expanding volume
        g.log.info("Starting to expand volume")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to expand volume %s" % self.clone)
        g.log.info("Expand volume successful")

        ret, _, _ = rebalance_start(self.mnode, self.clone)
        self.assertEqual(ret, 0, "Failed to start rebalance")
        g.log.info("Successfully started rebalance on the "
                   "volume %s", self.clone)

        # Log Rebalance status
        g.log.info("Log Rebalance status")
        _, _, _ = rebalance_status(self.mnode, self.clone)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.clone)
        self.assertTrue(ret, ("Rebalance is not yet complete "
                              "on the volume %s", self.clone))
        g.log.info("Rebalance is successfully complete on "
                   "the volume %s", self.clone)

        # Check Rebalance status after rebalance is complete
        g.log.info("Checking Rebalance status")
        ret, _, _ = rebalance_status(self.mnode, self.clone)
        self.assertEqual(ret, 0, ("Failed to get rebalance status for "
                                  "the volume %s", self.clone))
        g.log.info("Successfully got rebalance status of the "
                   "volume %s", self.clone)

        self.check_arequal()

    def tearDown(self):

        # Cleanup clone volume
        g.log.info("Starting to cleanup volume %s", self.clone)
        ret = cleanup_volume(self.mnode, self.clone)
        if not ret:
            raise ExecutionError("Failed to Cleanup cloned volume")
        g.log.info("Successful in clone volume cleanup")
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount"
                                 "the volume & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()
