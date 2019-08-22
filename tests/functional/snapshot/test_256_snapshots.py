#  Copyright (C) 2016-2020  Red Hat, Inc. <http://www.redhat.com>
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

import sys
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (
    wait_for_io_to_complete,
    get_mounts_stat)
from glustolibs.gluster.snap_ops import (
    snap_create,
    set_snap_config,
    get_snap_list,
    snap_delete_all)


@runs_on([['distributed', 'replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'], ['glusterfs', 'nfs', 'cifs']])
class TestValidateSnaps256(GlusterBaseClass):
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
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        setUp method
        """
        self.get_super_method(self, 'setUp')()

        # Setup_Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume")
        g.log.info("Volume %s has been setup successfully", self.volname)

        self.all_mounts_procs = []
        self.snapshots = [('snap-test-validate-256-snapshots-%s-%s'
                           % (self.volname, i))for i in range(0, 256)]

    def tearDown(self):
        """
        tearDown
        """
        self.get_super_method(self, 'tearDown')()

        # Delete all snapshots
        ret, _, _ = snap_delete_all(self.mnode)
        if ret:
            raise ExecutionError("Failed to delete all snapshots")
        g.log.info("Successfully deleted all snapshots")

        # Unmount and cleanup volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to cleanup volume and mount")
        g.log.info("Cleanup successful for the volume and mount")

    def test_validate_snaps_256(self):
        """
        Validate snapshot creation for 256 snapshots

        * Perform some IO
        * Set snapshot config option snap-max-hard-limit to 256
        * Create 256 snapshots
        * Verify 256 created successfully
        * Create 257th snapshot - creation should fail as it will
          exceed the hard-limit
        * Verify snapshot list for 256 snapshots

        """
        # pylint: disable=too-many-statements
        # Start IO on all mounts
        cmd = (
            "/usr/bin/env python%d %s create_files "
            "-f 10 --base-file-name firstfiles %s"
            % (sys.version_info.major,
               self.script_upload_path,
               self.mounts[0].mountpoint))
        proc = g.run_async(
            self.mounts[0].client_system, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete
        self.assertTrue(
            wait_for_io_to_complete(self.all_mounts_procs, self.mounts[0]),
            "IO failed on %s" % self.mounts[0])
        g.log.info("IO is successful on all mounts")

        # Perform stat on all the files/dirs created
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully performed stat on all files/dirs created")

        # Set config option snap-max-hard-limit to 256
        # This is to make sure to override
        max_hard_limit = {'snap-max-hard-limit': '256'}
        ret, _, _ = set_snap_config(self.mnode, max_hard_limit)
        self.assertEqual(ret, 0, "Failed to set snapshot config option "
                         "snap-max-hard-limit to 256")
        g.log.info("Successfully set snapshot config option "
                   "snap-max-hard-limit to 256")

        # Create 256 snapshots
        for snapname in self.snapshots:
            ret, _, _ = snap_create(self.mnode, self.volname, snapname)
            self.assertEqual(ret, 0, ("Failed to create snapshot %s for %s"
                                      % (snapname, self.volname)))
            sleep(1)
        g.log.info("Snapshots created successfully for volume %s",
                   self.volname)

        # Validate snapshot list for 256 snapshots
        snap_list = get_snap_list(self.mnode)
        self.assertTrue((len(snap_list) == 256), "Failed: Number of snapshots "
                        "is not consistent for volume %s" % self.volname)
        g.log.info("Successfully validated number of snapshots")

        # Validate snapshot existence using snap-name
        for snapname in self.snapshots:
            self.assertIn(snapname, snap_list,
                          "Failed: Snapshot %s not found" % snapname)
        g.log.info("Successfully validated snapshots existence using "
                   "snap-name")

        # Try to exceed snap-max-hard-limit by creating 257th snapshot
        snap_257 = "snap-test-validate-256-snapshots-%s-257" % (self.volname)
        ret, _, _ = snap_create(self.mnode, self.volname, snap_257)
        self.assertEqual(
            ret, 1, ("Unexpected: Successfully created %s for  volume %s"
                     % (snap_257, self.volname)))
        g.log.info("Snapshot %s not created as it exceeds the "
                   "snap-max-hard-limit", snap_257)

        # Validate snapshot list for 256 snapshots
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(len(snap_list), 256, "Failed: Number of snapshots "
                         "is not consistent for volume %s" % self.volname)
        g.log.info("Successfully validated number of snapshots")
