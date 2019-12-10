#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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
Description : The purpose of this test is to validate create snap>256
"""

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.snap_ops import get_snap_list, snap_delete_all


@runs_on([['distributed', 'replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'], ['glusterfs', 'nfs', 'cifs']])
class SanpCreate256(GlusterBaseClass):
    """
    Test for snapshot create for max 256
    Steps :
        1. Create and start a volume
        2. Mount the volume on a client
        3. Perform some heavy IO
        4. Varify IO
        5. modify max snap limit to default to 256.
        6. Create 256 snapshots
        7. Varify 256 created successfully
        8. Create 257th snapshot -  check for failure
          -- it should fail.
        9. Cleanup

    """
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

    def test_validate_snaps_256(self):

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

        # set config for 256 snpas (to make sure to override)
        cmd_str = ("gluster snapshot config snap-max-hard-limit 256"
                   " --mode=script")
        ret = g.run(self.mnode, cmd_str)
        self.assertTrue(ret, "Failed to set snap-max-hard-limit to 256.")
        g.log.info("snap-max-hard limit successfully set for 256.")

        # Create 256 snaps
        for i in range(1, 257, 1):
            cmd_str = "gluster snapshot create %s %s %s" % (
                "snapy%s" % i, self.volname, "no-timestamp")
            ret = g.run(self.mnode, cmd_str)
            self.assertTrue(ret, ("Failed to create snapshot for %s"
                                  % self.volname))
            g.log.info("Snapshot %s created successfully for volume  %s",
                       "snapy%s" % i, self.volname)

        # Check for no. of snaps using snap_list it should be 256
        snap_list = get_snap_list(self.mnode)
        self.assertTrue((len(snap_list) == 256), "No of snaps not consistent "
                        "for volume %s" % self.volname)
        g.log.info("Successfully validated number of snaps.")

        # Validate all 256 snap names created during
        for i in range(1, 257, 1):
            self.assertTrue(("snapy%s" % i in snap_list), "%s snap not "
                            "found " % ("snapy%s" % i))
        g.log.info("Successfully validated names of snap")

        # Try to create 257th snapshot
        cmd_str = "gluster snapshot create %s %s %s" % ("snap", self.volname,
                                                        "no-timestamp")
        ret = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 1, ("Unexpected: Successfully created 'snap'"
                                  " for  volume %s" % self.volname))
        g.log.info("Snapshot 'snap' not created as it is 257th snap")

        # Check for no. of snaps using snap_list it should be 256
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(256, len(snap_list), "No of snaps not consistent "
                         "for volume %s" % self.volname)
        g.log.info("Successfully validated number of snaps.")
