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
        The purpose of this test case is to ensure that USS validation.
        Where .snaps folder is only readable and is listing all the snapshots
        and it's content. Also ensures that deactivated snapshot
        doesn't get listed.

"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs,
                                 view_snaps_from_mount)
from glustolibs.gluster.uss_ops import (enable_uss, disable_uss,
                                        is_uss_enabled,
                                        uss_list_snaps)
from glustolibs.gluster.snap_ops import (snap_create,
                                         get_snap_list,
                                         snap_delete_all,
                                         snap_activate)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class TestValidateUss(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        """
        setup volume and initialize necessary variables
        """

        GlusterBaseClass.setUpClass.im_func(cls)
        # Setup volume and mount
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

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
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def tearDown(self):
        """
        tearDown for every test
        """

        g.log.info("Deleting all snapshots created")
        ret, _, _ = snap_delete_all(self.mnode)
        if ret != 0:
            raise ExecutionError("Snapshot Delete Failed")
        g.log.info("Successfully deleted all snapshots")

        # disable uss for volume
        g.log.info("Disabling uss for volume")
        ret, _, _ = disable_uss(self.mnode, self.volname)
        if ret != 0:
            raise ExecutionError("Failed to disable uss")
        g.log.info("Successfully disabled uss for volume"
                   "%s", self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume & mount
        """
        # stopping the volume and clean up the volume
        g.log.info("Starting to Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume and mount")
        g.log.info("Successful in Cleanup Volume and mount")

        # calling GlusterBaseClass tearDownClass
        GlusterBaseClass.tearDownClass.im_func(cls)

    def test_validate_snaps_dir_over_uss(self):

        # pylint: disable=too-many-statements
        """
        Run IOs on mount and take 2 snapshot.
        Activate 1 snapshot and check directory listing.
        Try to write to .snaps should not allow.
        Try listing the other snapshot should fail.
        """

        # run IOs
        self.counter = 1
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 2 "
                   "--max-num-of-dirs 2 "
                   "--num-of-files 2 %s" % (self.script_upload_path,
                                            self.counter,
                                            mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("I/O successful on clients")

        # get the snapshot list.
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(len(snap_list), 0, "Unexpected: %s snapshots"
                         "present" % len(snap_list))
        g.log.info("Expected: No snapshots present")

        # Create 2 snapshot
        g.log.info("Starting to Create Snapshots")
        for snap_num in range(0, 2):
            ret, _, _ = snap_create(self.mnode, self.volname,
                                    "snap-%s" % snap_num)
            self.assertEqual(ret, 0, "Snapshot Creation failed"
                             " for snap-%s" % snap_num)
            g.log.info("Snapshot snap-%s of volume %s created"
                       " successfully", snap_num, self.volname)

        # Activate snap-0
        g.log.info("Activating snapshot snap-0")
        ret, _, _ = snap_activate(self.mnode, "snap-0")
        self.assertEqual(ret, 0, "Failed to activate "
                         "Snapshot snap-0")
        g.log.info("Snapshot snap-0 Activated Successfully")

        # Enable USS for volume
        g.log.info("Enable uss for volume")
        ret, _, _ = enable_uss(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable USS for "
                         " volume %s" % self.volname)
        g.log.info("Successfully enabled USS "
                   "for volume %s", self.volname)

        # Validate uss enabled
        g.log.info("Validating uss enabled")
        ret = is_uss_enabled(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to validate uss enable")
        g.log.info("Successfully validated uss enable for volume"
                   "%s", self.volname)

        # list activated snapshots directory under .snaps
        g.log.info("Listing activated snapshots under .snaps")
        for mount_obj in self.mounts:
            ret, out, _ = uss_list_snaps(mount_obj.client_system,
                                         mount_obj.mountpoint)
            self.assertEqual(ret, 0, "Directory Listing Failed for"
                             " Activated Snapshot")
            validate_dir = out.split('\n')
            self.assertIn('snap-0', validate_dir, "Failed to "
                          "validate snap-0 under .snaps directory")
            g.log.info("Activated Snapshot Successfully listed")
            self.assertNotIn('snap-1', validate_dir, "Unexpected: "
                             "Successfully listed snap-1 under "
                             ".snaps directory")
            g.log.info("Expected: De-activated Snapshot not listed")

        # start I/0 ( write and read )
        g.log.info("Starting IO on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name file %s/.snaps/abc/"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # IO should fail
        g.log.info("IO should Fail with ROFS error.....")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertFalse(ret, "Unexpected: IO successfully completed")
        g.log.info("Expected: IO failed to complete")

        # validate snap-0 present in mountpoint
        ret = view_snaps_from_mount(self.mounts, "snap-0")
        self.assertTrue(ret, "UnExpected: Unable to list content "
                        "in activated snapshot"
                        " activated snapshot")
        g.log.info("Expected: Successfully listed contents in"
                   " activated snapshot")
