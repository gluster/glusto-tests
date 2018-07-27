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
Description : The purpose of this test is to validate snapshot
              hard and soft max-limt options.

Steps :
    1. Create and start a volume
    2. Mount the volume on a client
    3. Perform some heavy IO
    4. Varify IO
    5. modify max snap limit to default to 10.
    6. modify soft-limit to 50%
    6. Create 5 snapshots
    7. Varify 5 created successfully
    8. Create 6th snapshot -  check for warning
       -- it should not fail.
    9. modify soft-limit to 100%
    10. Create 7th snapshot -  check for warning
       -- it should not show warning.
    11. Reach the snap-max-hard-limit by creating more snapshots
    12. Create 11th snapshot - check for failure
       -- it shoul fail.
    13. varify the no. of snpas it should be 10.
    14. modify max snap limit to 20
    15. create 10 more snaps
    16. varify the no. of snpas it should be 20
    14. Cleanup

"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.snap_ops import get_snap_list, snap_delete_all


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs', 'nfs', 'cifs']])
class SnapCreateMax(GlusterBaseClass):
    """
    Test for snapshot create max limits
    Steps :
        1. Create and start a volume
        2. Mount the volume on a client
        3. Perform some heavy IO
        4. Varify IO
        5. modify max snap limit to default to 10.
        6. modify soft-limit to 50%
        6. Create 5 snapshots
        7. Varify 5 created successfully
        8. Create 6th snapshot -  check for warning
           -- it should not fail.
        9. modify soft-limit to 100%
        10. Create 7th snapshot -  check for warning
           -- it should not show warning.
        11. Reach the snap-max-hard-limit by creating more snapshots
        12. Create 11th snapshot - check for failure
           -- it shoul fail.
        13. varify the no. of snpas it should be 10.
        14. modify max snap limit to 20
        15. create 10 more snaps
        16. varify the no. of snpas it should be 20
        14. Cleanup

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
        g.log.info("Successfully uploaded IO scripts "
                   "to clients %s", cls.clients)

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
        if ret != 0:
            raise ExecutionError("Failed to delete all snapshots.")
        GlusterBaseClass.tearDown.im_func(self)

        # Clean up the volume & mount
        g.log.info("Starting volume and  mount cleanup")
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to cleanup volume and mount")
        g.log.info("Cleanup successful for the volume and mount")

    def test_validate_snaps_max_limit(self):
        # pylint: disable=too-many-statements
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

        # set config snap-max-hard-limit for 10 snpas
        cmd_str = ("gluster snapshot config snap-max-hard-limit 10"
                   " --mode=script")
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, "Failed to set snap-max-hard-limit to 10.")
        g.log.info("snap-max-hard-limit successfully set for 10.")

        # set config snap-max-soft-limit to 50%
        cmd_str = ("gluster snapshot config snap-max-soft-limit 50"
                   " --mode=script")
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, "Failed to set snap-max-soft-limit to 50%.")
        g.log.info("snap-max-soft-limit successfully set for 50%.")

        # Create 5 snaps
        for i in range(1, 6):
            cmd_str = "gluster snapshot create %s %s %s" % ("snapy%s" % i,
                                                            self.volname,
                                                            "no-timestamp")
            ret, _, _ = g.run(self.mnode, cmd_str)
            self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                      % self.volname))
            g.log.info("Snapshot snapy%s created successfully"
                       " for volume  %s", i, self.volname)

        # Check for no. of snaps using snap_list it should be 5
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(5, len(snap_list), "Expected 5 snapshots. "
                         "Found %s snapshots" % len(snap_list))
        g.log.info("Successfully validated number of snapshots.")

        # Validate all 5 snap names created during
        for i in range(1, 6):
            self.assertTrue(("snapy%s" % i in snap_list), "%s snap not "
                            "found " % ("snapy%s" % i))
        g.log.info("Successfully validated names of snapshots")

        # create 6th snapshot
        cmd_str = "gluster snapshot create %s %s %s" % ("snapy6", self.volname,
                                                        "no-timestamp")
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, ("Failed to create snap6 "
                                  "for %s" % self.volname))
        g.log.info("Snapshot 'snapy6' created as it is 6th snap")

        # set config snap-max-soft-limit to 100%
        cmd_str = ("gluster snapshot config snap-max-soft-limit 100"
                   " --mode=script")
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, "Failed to set snap-max-soft-limit to 100%.")
        g.log.info("snap-max-soft-limit successfully set for 100%.")

        # create 7th snapshot
        cmd_str = "gluster snapshot create %s %s %s" % ("snapy7", self.volname,
                                                        "no-timestamp")
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, ("Failed to create "
                                  "snap7 for %s" % self.volname))
        g.log.info("Snapshot 'snapy7' created as it is 7th snap")

        # Create 3 snaps
        for i in range(8, 11, 1):
            cmd_str = "gluster snapshot create %s %s %s" % ("snapy%s" % i,
                                                            self.volname,
                                                            "no-timestamp")
            ret, _, _ = g.run(self.mnode, cmd_str)
            self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                      % self.volname))
            g.log.info("Snapshot snapy%s created successfully "
                       "for volume  %s", i, self.volname)

        # Check for no. of snaps using snap_list it should be 10
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(len(snap_list), 10, "Expected 10 snapshots. "
                         "found %s snapshots" % len(snap_list))
        g.log.info("Successfully validated number of snapshots.")

        # Validate all 10 snap names created
        for i in range(1, 11, 1):
            self.assertTrue(("snapy%s" % i in snap_list), "%s snap not "
                            "found " % ("snapy%s" % i))
        g.log.info("Successfully validated names of snapshots")

        # create 11th snapshot
        cmd_str = "gluster snapshot create %s %s %s" % ("snap", self.volname,
                                                        "no-timestamp")
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertNotEqual(ret, 0, ("Unexpected: successfully created 'snap' "
                                     "for %s" % self.volname))
        g.log.info("Expected: Snapshot 'snap' not created as it is 11th snap")

        # Check for no. of snaps using snap_list it should be 10
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(len(snap_list), 10, "Expected 10 snapshots. "
                         "found %s snapshots" % len(snap_list))
        g.log.info("Successfully validated number of snapshots.")

        # modify config snap-max-hard-limit for 20 snpas
        cmd_str = ("gluster snapshot config snap-max-hard-limit 20"
                   " --mode=script")
        ret, _, _ = g.run(self.mnode, cmd_str)
        self.assertEqual(ret, 0, "Failed to set snap-max-hard-limit to 20.")
        g.log.info("snap-max-hard-limit successfully set for 20.")

        # Create 10 snaps
        for i in range(11, 21, 1):
            cmd_str = "gluster snapshot create %s %s %s" % ("snapy%s" % i,
                                                            self.volname,
                                                            "no-timestamp")
            ret, _, _ = g.run(self.mnode, cmd_str)
            self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                      % self.volname))
            g.log.info("Snapshot snapy%s created successfully for "
                       "volume  %s", i, self.volname)

        # Check for no. of snaps using snap_list it should be 20
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(len(snap_list), 20, "Expected 20 snapshots. "
                         "found %s snapshots" % len(snap_list))
        g.log.info("Successfully validated number of snaps.")
