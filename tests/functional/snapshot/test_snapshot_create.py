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

"""

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.snap_ops import (get_snap_list, snap_delete_all)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs', 'nfs', 'cifs']])
class SnapCreate(GlusterBaseClass):
    """
    Test for snapshot create
    Steps :
        1. Create and start a volume
        2. Create a snapshot of volume using
           -- gluster snapshot create <snap1> <vol-name(s)>
        3. Create snapshot of volume using
           -- gluster snapshot create <snap2> <vol-name(s)> [description
              <description with words and quotes>]
        4. Create one more snapshot of volume using
           -- gluster snapshot create <snap3> <vol-name(s)> force
        5. Create one snapshot with option no-timestamp
        6. Mount the volume on a client
        7. Perform some heavy IO
        8. While files and directory creation is in progress,
           create multiple gluster snapshots
        9. Do a snapshot list to see if all the snapshots are present
        10. Do a snapshot info to see all the snapshots information
        11. Verify that the IO is not hindered
        12. Arequal all the bricks in the snap volume
        13. Cleanup

    """
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
        # Setup_Volume
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume")
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):
        """
        tearDown
        """
        ret, _, _ = snap_delete_all(self.mnode)
        if ret != 0:
            raise ExecutionError("Failed to delete all snaps")
        self.get_super_method(self, 'tearDown')()

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume & mount
        """
        g.log.info("Starting volume and  mount cleanup")
        ret = cls.unmount_volume_and_cleanup_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to cleanup volume and mount")
        g.log.info("Cleanup successful for the volume and mount")

        cls.get_super_method(cls, 'tearDownClass')()

    def test_validate_snaps_create(self):
        """
        Creating snapshot using gluster snapshot create <snap1> <vol-name>
        """
        cmd_str = "gluster snapshot create %s %s" % ("snap1", self.volname)
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                  % self.volname))
        g.log.info("Snapshot snap1 created successfully for volume  %s",
                   self.volname)

        # Create snapshot of volume using
        # -- gluster snapshot create <snap2> <vol-name(s)> [description
        # <description with words and quotes>]
        desc = 'description "this is a snap with snap2 name and description"'
        cmd_str = ("gluster snapshot create %s %s %s"
                   % ("snap2", self.volname, desc))
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                  % self.volname))
        g.log.info("Snapshot snap2 created successfully for volume  %s",
                   self.volname)

        # Create one more snapshot of volume using force
        cmd_str = ("gluster snapshot create %s %s %s"
                   % ("snap3", self.volname, "force"))
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                  % self.volname))
        g.log.info("Snapshot snap3 created successfully for volume  %s",
                   self.volname)

        # Create one more snapshot of volume using no-timestamp option
        cmd_str = ("gluster snapshot create %s %s %s"
                   % ("snap4", self.volname, "no-timestamp"))
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                  % self.volname))
        g.log.info("Snapshot snap4 created successfully for volume  %s",
                   self.volname)

        # Delete all snaps
        g.log.info("delete all snapshots present")
        ret, _, _ = snap_delete_all(self.mnode)
        self.assertEqual(ret, 0, "Snapshot delete failed.")
        g.log.info("Successfully deleted all snaps")

        # Start IO on all mounts.
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       sys.version_info.major, self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Create 5 snaps while IO is in progress
        for i in range(0, 5):
            cmd_str = "gluster snapshot create %s %s %s" % (
                "snapy%s" % i, self.volname, "no-timestamp")
            ret, _, _ = g.run(self.mnode, cmd_str)
            self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                      % self.volname))
            g.log.info("Snapshot %s created successfully for volume  %s",
                       "snapy%s" % i, self.volname)

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

        # Check for no of snaps using snap_list it should be 5 now
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(5, len(snap_list), "No of snaps not consistent "
                         "for volume %s" % self.volname)
        g.log.info("Successfully validated number of snaps.")

        # Validate all snaps created during IO
        for i in range(0, 5):
            self.assertIn("snapy%s" % i, snap_list, "%s snap not "
                          "found " % ("snapy%s" % i))
        g.log.info("Successfully validated names of snap")
