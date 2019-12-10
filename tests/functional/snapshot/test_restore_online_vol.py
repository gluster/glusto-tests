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
    This test case will validate snap restore on online volume.
    When we try to restore online volume it should fail.
"""

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.snap_ops import (snap_create, snap_delete_all,
                                         get_snap_list, snap_restore)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class SnapRSOnline(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

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

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount and cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

    def test_restore_online_vol(self):

        # pylint: disable=too-many-statements
        """
        Steps:
        1. Create volume
        2. Mount volume
        3. Perform I/O on mounts
        4. Create 1 snapshots snapy1
        5. Validate snap created
        6. Perform some more I/O
        7. Create 1 more snapshot snapy2
        8. Restore volume to snapy1
          -- Restore should fail with message
             "volume needs to be stopped before restore"
        """

        # Performing step 3 to 7 in loop here
        for i in range(1, 3):
            # Perform I/O
            g.log.info("Starting IO on all mounts...")
            self.counter = 1
            self.all_mounts_procs = []
            for mount_obj in self.mounts:
                g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                           mount_obj.mountpoint)
                cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                       "--dirname-start-num %d "
                       "--dir-depth 2 "
                       "--dir-length 2 "
                       "--max-num-of-dirs 2 "
                       "--num-of-files 2 %s" % (
                           sys.version_info.major, self.script_upload_path,
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

            # Get stat of all the files/dirs created.
            g.log.info("Get stat of all the files/dirs created.")
            ret = get_mounts_stat(self.mounts)
            self.assertTrue(ret, "Stat failed on some of the clients")
            g.log.info("Successfully got stat of all files/dirs created")

            # Create snapshot
            g.log.info("Creating snapshot for volume %s", self.volname)
            ret, _, _ = snap_create(self.mnode, self.volname, "snapy%s" % i)
            self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                      % self.volname))
            g.log.info("Snapshot created successfully for volume  %s",
                       self.volname)

            # Check for no of snaps using snap_list
            snap_list = get_snap_list(self.mnode)
            self.assertEqual(i, len(snap_list), "No of snaps not consistent "
                             "for volume %s" % self.volname)
            g.log.info("Successfully validated number of snaps.")

            # Increase counter for next iteration
            self.counter = 1000

        # Restore volume to snapshot snapy2, it should fail
        i = 2
        g.log.info("Starting to restore volume to snapy%s", i)
        ret, _, err = snap_restore(self.mnode, "snapy%s" % i)
        errmsg = ("snapshot restore: failed: Volume (%s) has been started. "
                  "Volume needs to be stopped before restoring a snapshot.\n" %
                  self.volname)
        log_msg = ("Expected : %s, but Returned : %s", errmsg, err)
        self.assertEqual(err, errmsg, log_msg)
        g.log.info("Expected : Failed to restore volume to snapy%s", i)
