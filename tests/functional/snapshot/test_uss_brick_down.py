#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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
    This test case will validate USS behaviour when we
    enable USS on the volume when brick is down.
"""


from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.snap_ops import (snap_create, snap_delete_all,
                                         get_snap_list, snap_activate)
from glustolibs.gluster.uss_ops import (enable_uss, is_uss_enabled,
                                        uss_list_snaps, disable_uss)
from glustolibs.gluster.brick_libs import get_all_bricks, bring_bricks_offline
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class SnapUssBrickDown(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients ")
        g.log.info("Successfully uploaded IO scripts to clients %s")

    def setUp(self):

        # SettingUp and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp Volume and mount volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):

        # Deleting all snapshot
        g.log.info("Deleting all snapshots created")
        ret, _, _ = snap_delete_all(self.mnode)
        if ret != 0:
            raise ExecutionError("Snapshot Delete Failed")
        g.log.info("Successfully deleted all snapshots")

        # Disable uss for volume
        g.log.info("Disabling uss for volume")
        ret, _, _ = disable_uss(self.mnode, self.volname)
        if ret != 0:
            raise ExecutionError("Failed to disable uss")
        g.log.info("Successfully disabled uss for volume"
                   "%s", self.volname)

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount and cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

    def test_uss_brick_down(self):

        # pylint: disable=too-many-statements
        """
        Steps:
        * Create volume
        * Mount volume
        * Perform I/O on mounts
        * Bring down one brick
        * Enable USS
        * Validate USS is enabled
        * Bring the brick online using gluster v start force
        * Create 2 snapshots snapy1 & snapy2
        * Validate snap created
        * Activate snapy1 & snapy2
        * List snaps under .snap directory
          -- snap1 and snap2 should be listed under .snaps
        """

        # Perform I/O
        g.log.info("Starting IO on all mounts...")
        self.counter = 1
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 2 "
                   "--max-num-of-dirs 2 "
                   "--num-of-files 2 %s" % (
                       self.script_upload_path,
                       self.counter, mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Bring down 1 brick from brick list
        g.log.info("Getting all the bricks of the volume")
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        ret = bring_bricks_offline(self.volname, bricks_list[0])
        self.assertTrue(ret, ("Failed to bring down the brick %s ",
                              bricks_list[0]))
        g.log.info("Successfully brought the brick %s down", bricks_list[0])

        # Enable USS
        g.log.info("Enable USS on volume")
        ret, _, _ = enable_uss(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable USS on volume")
        g.log.info("Successfully enabled USS on volume")

        # Validate USS is enabled
        g.log.info("Validating USS is enabled")
        ret = is_uss_enabled(self.mnode, self.volname)
        self.assertTrue(ret, "USS is disabled on volume "
                        "%s" % self.volname)
        g.log.info("USS enabled on volume %s", self.volname)

        #  Bring the brick online using gluster v start force
        g.log.info("Bring the brick online using gluster v start force")
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Volume start with force failed")
        g.log.info("Volume start with force successful")

        # Create 2 snapshot
        g.log.info("Creating 2 snapshots for volume %s", self.volname)
        for i in range(0, 2):
            ret, _, _ = snap_create(self.mnode, self.volname, "snapy%s" % i)
            self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                      % self.volname))
            g.log.info("Snapshot %s created successfully for volume  %s",
                       "snapy%s" % i, self.volname)

        # Check for no of snaps using snap_list it should be 2 now
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(2, len(snap_list), "No of snaps not consistent "
                         "for volume %s" % self.volname)
        g.log.info("Successfully validated number of snaps.")

        # Activate snapshot snapy1 & snapy2
        g.log.info("Activating snapshot snapy1 & snapy2")
        for i in range(0, 2):
            ret, _, _ = snap_activate(self.mnode, "snapy%s" % i)
            self.assertEqual(ret, 0, "Failed to activate snapshot snapy%s" % i)
        g.log.info("Both snapshots activated successfully")

        # list activated snapshots directory under .snaps
        g.log.info("Listing activated snapshots under .snaps")
        for mount_obj in self.mounts:
            ret, out, _ = uss_list_snaps(mount_obj.client_system,
                                         mount_obj.mountpoint)
            self.assertEqual(ret, 0, "Directory Listing Failed for"
                             " Activated Snapshot")
            validate_dir = out.split('\n')
            for i in range(0, 2):
                self.assertIn("snapy%s" % i, validate_dir, "Failed to "
                              "validate snapy%s under .snaps directory")
                g.log.info("Activated Snapshot snapy%s listed Successfully", i)
