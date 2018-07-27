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
Description : The purpose of this test is to validate snapshot create
              during rebalance

"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.volume_libs import (
    expand_volume, log_volume_info_and_status,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete,
                                              rebalance_status)
from glustolibs.gluster.snap_ops import get_snap_list, snap_delete_all


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class SnapCreateRebal(GlusterBaseClass):
    """
    Test for snapshot create during rebalance
    Steps:
        1. Create and start a volume
        2. Mount the volume on client
        3. Perform some heavy IO
        4. Create one snapshot with option no-timestamp
        5. Add bricks to the volume using gluster volume
        6. Start Rebalance using gluster v rebalance <vol-name> start
        7. While rebalance is in progress, create gluster snapshot
           -snapshot creation should fail with message : rebalance is runinng
            on the volume. Please try after sometime
        8. Check for snap name and number to validate snaps created or not
           during rebalance
        9. After rebalance is completed, create snapshots with the same name as
           in Step 7
           -- this operation should be successful
        10. Cleanup

    """
    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)

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
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to "
                   "clients %s", cls.clients)

    def setUp(self):
        """
        setUp method
        """
        # Setup_Volume
        GlusterBaseClass.setUpClass.im_func(self)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=True)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume")
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):
        """
        tearDown
        """
        ret, _, _ = snap_delete_all(self.mnode)
        if not ret:
            raise ExecutionError("Failed to delete all snaps")
        GlusterBaseClass.tearDown.im_func(self)

        # Clean up the volume & mount
        g.log.info("Starting volume and  mount cleanup")
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to cleanup volume and mount")
        g.log.info("Cleanup successful for the volume and mount")

    def test_snapshot_while_rebalance(self):
        # pylint: disable=too-many-statements, missing-docstring
        # Start IO on all mounts.
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (self.script_upload_path, count,
                                            mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Validate IO
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # Create one snapshot of volume using no-timestamp option
        cmd_str = ("gluster snapshot create %s %s %s"
                   % ("snapy", self.volname, "no-timestamp"))
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                  % self.volname))
        g.log.info("Snapshot snapy created successfully "
                   "for volume %s", self.volname)

        # Check for no of snaps using snap_list it should be 1
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(1, len(snap_list), "Expected 1 snapshot "
                         "found %s snapshots" % len(snap_list))
        g.log.info("Successfully validated number of snaps.")

        # validate snap name
        self.assertIn("snapy", snap_list, " snap not found")
        g.log.info("Successfully validated names of snap")

        # get the bricks for the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # expanding volume
        g.log.info("Start adding bricks to volume %s", self.volname)
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to add bricks to "
                              "volume %s " % self.volname))
        g.log.info("Add brick successful")

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed "
                              "on volume %s", self.volname))
        g.log.info("Successful in logging volume info and status "
                   "of volume %s", self.volname)

        # Verify volume's all process are online for 60 sec
        g.log.info("Verifying volume's all process are online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname,
                                                   60)
        self.assertTrue(ret, ("Volume %s : All process are not "
                              "online", self.volname))
        g.log.info("Successfully Verified volume %s "
                   "processes are online", self.volname)

        # Start Rebalance
        g.log.info("Starting Rebalance on the volume")
        ret, _, err = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on "
                                  "the volume %s with error %s" %
                                  (self.volname, err)))
        g.log.info("Successfully started rebalance on the "
                   "volume %s", self.volname)

        # Log Rebalance status
        g.log.info("Log Rebalance status")
        ret, _, _ = rebalance_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to log rebalance status")
        g.log.info("successfully logged rebalance status")

        # Create one snapshot of volume during rebalance
        cmd_str = ("gluster snapshot create %s %s %s"
                   % ("snapy_rebal", self.volname, "no-timestamp"))
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertNotEqual(ret, 0, ("successfully created 'snapy_rebal'"
                                     " for %s" % self.volname))
        g.log.info("Snapshot 'snapy_rebal' not created as rebalance is in "
                   "progress check log")
        # Check for no of snaps using snap_list it should be 1
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(1, len(snap_list), "Expected 1 snapshot "
                         "found %s snapshot" % len(snap_list))
        g.log.info("Successfully validated number of snaps.")

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete "
                              "on the volume %s", self.volname))
        g.log.info("Rebalance is successfully complete on "
                   "the volume %s", self.volname)

        # Check Rebalance status after rebalance is complete
        g.log.info("Checking Rebalance status")
        ret, _, _ = rebalance_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to get rebalance status for "
                                  "the volume %s", self.volname))
        g.log.info("Successfully got rebalance status of the "
                   "volume %s", self.volname)

        # Create one snapshot of volume post rebalance with same name
        cmd_str = ("gluster snapshot create %s %s %s"
                   % ("snapy_rebal", self.volname, "no-timestamp"))
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                  % self.volname))
        g.log.info("Snapshot snapy_rebal created successfully "
                   "for volume  %s", self.volname)

        # Check for no of snaps using snap_list it should be 2
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(2, len(snap_list), "Expected 2 snapshots "
                         "found %s snapshot" % len(snap_list))
        g.log.info("Successfully validated number of snaps.")

        # validate snap name
        self.assertIn("snapy_rebal", snap_list, " snap not found")
        g.log.info("Successfully validated names of snap")
