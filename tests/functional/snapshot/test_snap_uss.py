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
Creation of snapshot and USS feature.

"""
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_list,
                                         snap_delete_all,
                                         snap_activate)
from glustolibs.io.utils import view_snaps_from_mount
from glustolibs.gluster.uss_ops import (is_uss_enabled,
                                        enable_uss, uss_list_snaps,
                                        disable_uss)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import file_exists


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs']])
class SnapshotUssSnap(GlusterBaseClass):

    def setUp(self):
        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", self.clients)
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                   "file_dir_ops.py")
        ret = upload_scripts(self.clients, self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    def validate_snaps(self):
        # Validate snapshots under .snaps folder
        g.log.info("Validating snapshots under .snaps")
        ret = view_snaps_from_mount(self.mounts, self.snaps_list)
        self.assertTrue(ret, "Failed to lists .snaps folder")
        g.log.info("Successfully validated snapshots under .snaps")

    def test_snap_uss(self):
        # pylint: disable=too-many-statements
        """
        Steps:
        1. Create a volume and mount it.
        2. Perform I/O on mounts
        3. create a .snaps directory and create some files
        4. Create Multiple snapshots of volume
        5. Check info of volume
        6. Enable USS for volume
        7. Validate files created under .snaps
        8. Disable USS
        9. Again Validate the files created under .snaps directory
        """
        # write files on all mounts
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python%d %s create_files "
                   "-f 10 --base-file-name file %s" % (
                       sys.version_info.major, self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # starting I/O
        g.log.info("Starting IO on all mounts...")
        for mount_obj in self.mounts:
            self.mpoint = "%s/.snaps" % mount_obj.mountpoint
            ret = file_exists(mount_obj.client_system, self.mpoint)
            if not ret:
                ret = mkdir(mount_obj.client_system, self.mpoint)
                self.assertTrue(ret, "Failed to create .snaps directory")
                g.log.info("Successfully created .snaps directory")
                break
            else:
                # Validate USS running
                g.log.info("Validating USS enabled or disabled")
                ret = is_uss_enabled(self.mnode, self.volname)
                if not ret:
                    break
                else:
                    g.log.info("USS is enabled in volume %s", self.volname)
                    ret, _, _ = disable_uss(self.mnode, self.volname)
                    self.assertEqual(ret, 0, "Failed to disable USS on "
                                     " volume %s" % self.volname)
                    g.log.info("USS disabled in Volume %s", self.volname)
                    ret = mkdir(mount_obj.client_system, self.mpoint)
                    self.assertTrue(ret, "Failed to create .snaps directory")
                    g.log.info("Successfully created .snaps directory")
            cmd = ("/usr/bin/env python%d %s create_files "
                   "-f 10 --base-file-name foo %s" % (
                       sys.version_info.major, self.script_upload_path,
                       self.mpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # List files under created .snaps directory
        g.log.info("Starting to list files under .snaps directory")
        for mount_obj in self.mounts:
            ret, out, _ = uss_list_snaps(mount_obj.client_system,
                                         mount_obj.mountpoint)
            self.assertEqual(ret, 0, "Failed to list files under .snaps")
            g.log.info("Successfully Created files under .snaps directory")
            before_uss_enable = out.strip().split('\n')
            # deleting the mount path from list
            del before_uss_enable[0]

        # Create Multiple snapshots for volume
        g.log.info("Creating snapshots")
        self.snaps_list = []
        for snap_count in range(1, 5):
            self.snap = "snap%s" % snap_count
            ret, _, _ = snap_create(self.mnode, self.volname, self.snap)
            self.assertEqual(ret, 0, "Failed to create snapshot "
                             "%s for volume %s"
                             % (self.snap, self.volname))
            self.snaps_list.append(self.snap)
            g.log.info("Snapshot %s created successfully for volume %s",
                       self.snap, self.volname)
        g.log.info("Snapshot Creation Successful")

        # Activate the snapshots
        g.log.info("Activating snapshots")
        for snap_count in range(1, 5):
            self.snap = "snap%s" % snap_count
            ret, _, _ = snap_activate(self.mnode, self.snap)
            self.assertEqual(ret, 0, ("Failed to activate snapshot %s"
                                      % self.snap))
            g.log.info("Snapshot snap%s activated successfully",
                       self.snap)

        # snapshot list
        g.log.info("Starting to list snapshots")
        ret, out, _ = snap_list(self.mnode)
        self.assertEqual(ret, 0, "Failed to list snapshot")
        snap_count = out.strip().split("\n")
        self.assertEqual(len(snap_count), 4, "Failed to list all snaps")
        g.log.info("Snapshot list Validated successfully")

        # Enable USS
        g.log.info("Enable USS on volume")
        ret, _, _ = enable_uss(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable USS on cloned volume")
        g.log.info("Successfully enabled USS on Cloned volume")

        # Validate USS running
        g.log.info("Validating USS enabled or disabled")
        ret = is_uss_enabled(self.mnode, self.volname)
        self.assertTrue(ret, ("USS is disabled in volume %s" % self.volname))
        g.log.info("USS enabled in Volume %s", self.volname)

        # Validate snapshots under .snaps folder
        self.validate_snaps()

        # check snapshots are listed
        g.log.info(".snaps Containing:")
        for mount_obj in self.mounts:
            ret, _, _ = uss_list_snaps(mount_obj.client_system,
                                       mount_obj.mountpoint)
            self.assertEqual(ret, 0, "Failed to list snapshot information")
            g.log.info("Successfully Listed snapshots Created")

        # Disable USS running
        g.log.info("Disable USS on volume")
        ret, _, _ = disable_uss(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to disable USS on volume")
        g.log.info("Successfully disabled USS on volume")

        # check snapshots are listed
        g.log.info(".snaps Containing:")
        for mount_obj in self.mounts:
            ret, out, _ = uss_list_snaps(mount_obj.client_system,
                                         mount_obj.mountpoint)
            self.assertEqual(ret, 0, "Failed to list snapshot information")
            g.log.info("Successfully listed snapshots Created")

        # Validate after disabling USS, all files should be same
        g.log.info("Validate files after disabling uss")
        after_uss_disable = out.strip().split('\n')
        # deleting the mount path from list
        del after_uss_disable[0]
        for files in before_uss_enable:
            self.assertIn(files, after_uss_disable,
                          "Files are Same under .snaps")
        g.log.info("Validated files under .snaps directory")

    def tearDown(self):
        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

        # deleting created snapshots
        g.log.info("Deleting all snapshots")
        ret, _, _ = snap_delete_all(self.mnode)
        self.assertEqual(ret, 0, ("Failed to delete snapshot of volume"
                                  "%s" % self.volname))
        g.log.info("Successfully deleted snapshots of volume %s",
                   self.volname)

        # Unmount Volume
        g.log.info("Starting to Unmount Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")
