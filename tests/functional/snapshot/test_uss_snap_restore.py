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
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.io.utils import (
    wait_for_io_to_complete,
    get_mounts_stat)
from glustolibs.gluster.snap_ops import (
    snap_create,
    get_snap_list,
    snap_activate,
    snap_restore_complete)
from glustolibs.gluster.uss_ops import (
    enable_uss,
    is_uss_enabled,
    get_uss_list_snaps,
    is_snapd_running,
    disable_uss)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class TestUssSnapRestore(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload IO scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [cls.script_upload_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")
        self.snapshots = [('snap-test-uss-snap-restore-%s-%s'
                           % (self.volname, i))for i in range(0, 2)]

    def tearDown(self):

        # Disable uss for volume
        ret, _, _ = disable_uss(self.mnode, self.volname)
        if ret:
            raise ExecutionError("Failed to disable uss")
        g.log.info("Successfully disabled uss for volume %s", self.volname)

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount and cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_uss_snap_restore(self):
        """
        Description:
            This test case will validate USS after Snapshot restore.
            The restored snapshot should not be listed under the '.snaps'
            directory.

        * Perform I/O on mounts
        * Enable USS on volume
        * Validate USS is enabled
        * Create a snapshot
        * Activate the snapshot
        * Perform some more I/O
        * Create another snapshot
        * Activate the second
        * Restore volume to the second snapshot
        * From mount point validate under .snaps
          - first snapshot should be listed
          - second snapshot should not be listed
        """

        # pylint: disable=too-many-statements
        # Perform I/O
        cmd = (
            "/usr/bin/env python %s create_files "
            "-f 10 --base-file-name firstfiles %s"
            % (self.script_upload_path,
               self.mounts[0].mountpoint))
        proc = g.run_async(
            self.mounts[0].client_system, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete and validate IO
        self.assertTrue(
            wait_for_io_to_complete(self.all_mounts_procs, self.mounts[0]),
            "IO failed on %s" % self.mounts[0])
        g.log.info("IO is successful on all mounts")

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # Enable USS
        ret, _, _ = enable_uss(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable USS on volume")
        g.log.info("Successfully enabled USS on volume")

        # Validate USS is enabled
        ret = is_uss_enabled(self.mnode, self.volname)
        self.assertTrue(ret, "USS is disabled on volume %s" % self.volname)
        g.log.info("USS enabled on volume %s", self.volname)

        # Create a snapshot
        ret, _, _ = snap_create(self.mnode, self.volname, self.snapshots[0])
        self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                  % self.volname))
        g.log.info("Snapshot %s created successfully for volume  %s",
                   self.snapshots[0], self.volname)

        # Check for number of snaps using snap_list it should be 1 now
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(1, len(snap_list), "No of snaps not consistent "
                         "for volume %s" % self.volname)
        g.log.info("Successfully validated number of snapshots")

        # Activate the snapshot
        ret, _, _ = snap_activate(self.mnode, self.snapshots[0])
        self.assertEqual(ret, 0, ("Failed to activate snapshot %s"
                                  % self.snapshots[0]))
        g.log.info("Snapshot %s activated successfully", self.snapshots[0])

        # Perform I/O
        self.all_mounts_procs = []
        cmd = (
            "/usr/bin/env python %s create_files "
            "-f 10 --base-file-name secondfiles %s"
            % (self.script_upload_path,
               self.mounts[0].mountpoint))
        proc = g.run_async(
            self.mounts[0].client_system, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete and validate IO
        self.assertTrue(
            wait_for_io_to_complete(self.all_mounts_procs, self.mounts[0]),
            "IO failed on %s" % self.mounts[0])
        g.log.info("IO is successful on all mounts")

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # Create another snapshot
        ret, _, _ = snap_create(self.mnode, self.volname, self.snapshots[1])
        self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                  % self.volname))
        g.log.info("Snapshot %s created successfully for volume  %s",
                   self.snapshots[1], self.volname)

        # Check for number of snaps using snap_list it should be 2 now
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(2, len(snap_list), "No of snaps not consistent "
                         "for volume %s" % self.volname)
        g.log.info("Successfully validated number of snapshots")

        # Activate the second snapshot
        ret, _, _ = snap_activate(self.mnode, self.snapshots[1])
        self.assertEqual(ret, 0, ("Failed to activate snapshot %s"
                                  % self.snapshots[1]))
        g.log.info("Snapshot %s activated successfully", self.snapshots[1])

        # Restore volume to the second snapshot
        ret = snap_restore_complete(
            self.mnode, self.volname, self.snapshots[1])
        self.assertTrue(ret, ("Failed to restore snap %s on the "
                              "volume %s" % (self.snapshots[1], self.volname)))
        g.log.info("Restore of volume is successful from %s on "
                   "volume %s", self.snapshots[1], self.volname)

        # Verify all volume processes are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, "Failed: All volume processes are not online")
        g.log.info("All volume processes are online")
        ret = is_snapd_running(self.mnode, self.volname)
        self.assertTrue(
            ret, "Failed: snapd is not running for volume %s" % self.volname)
        g.log.info("Successful: snapd is running")

        # List activated snapshots under the .snaps directory
        snap_dir_list = get_uss_list_snaps(self.mounts[0].client_system,
                                           self.mounts[0].mountpoint)
        self.assertIsNotNone(
            snap_dir_list, "Failed to list snapshots under .snaps directory")
        g.log.info("Successfully gathered list of snapshots under the .snaps"
                   " directory")

        # Check for first snapshot as it should get listed here
        self.assertIn(self.snapshots[0], snap_dir_list,
                      ("Unexpected : %s not listed under .snaps "
                       "directory" % self.snapshots[0]))
        g.log.info("Activated Snapshot %s listed Successfully",
                   self.snapshots[0])

        # Check for second snapshot as it should not get listed here
        self.assertNotIn(self.snapshots[1], snap_dir_list,
                         ("Unexpected : %s listed in .snaps "
                          "directory" % self.snapshots[1]))
        g.log.info("Restored Snapshot %s not listed ", self.snapshots[1])
