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
uss functionality while io is going on.

"""
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.io.utils import (validate_io_procs,
                                 view_snaps_from_mount)
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_activate,
                                         snap_list)
from glustolibs.gluster.uss_ops import (enable_uss, is_uss_enabled,
                                        is_snapd_running)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class SnapshotUssWhileIo(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.snap_count = 10
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

        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp Volume and mount volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

    def test_snap_uss_while_io(self):
        # pylint: disable=too-many-statements
        """
        Steps:
        1. Create volume
        2. enable uss on created volume
        3. validate uss running
        4. validate snapd running on all nodes
        5. perform io on mounts
        6. create 10 snapshots with description
        7. validate with snapshot list
        8. validate io is completed
        9. Activate snapshots to list all snaps
           under .snaps
        10. validate snapshots under .snaps directory
        """
        # Enable USS
        g.log.info("Enable USS for volume")
        ret, _, _ = enable_uss(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable USS on volume"
                         "%s" % self.volname)
        g.log.info("Successfully enabled USS on volume %s",
                   self.volname)

        # Validate USS running
        g.log.info("Validating USS enabled or disabled")
        ret = is_uss_enabled(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to validate USS for volume "
                        "%s" % self.volname)
        g.log.info("Successfully validated USS for Volume"
                   "%s", self.volname)

        # Validate snapd running
        for server in self.servers:
            g.log.info("Validating snapd daemon on:%s", server)
            ret = is_snapd_running(server, self.volname)
            self.assertTrue(ret, "Snapd is Not running on "
                            "%s" % server)
            g.log.info("Snapd Running on node: %s", server)

        # Perform I/O
        g.log.info("Starting to Perform I/O")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:"
                       "%s", mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("/usr/bin/env python%d %s create_files -f 100 "
                       "--fixed-file-size 1M %s" % (
                           sys.version_info.major, self.script_upload_path,
                           mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Creating snapshot with description
        g.log.info("Starting to Create snapshot")
        for count in range(0, self.snap_count):
            self.snap = "snap%s" % count
            ret, _, _ = snap_create(self.mnode, self.volname,
                                    self.snap,
                                    description='$p3C!@l C#@R@cT#R$')
            self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                      % self.volname))
            g.log.info("Snapshot %s created successfully"
                       " for volume %s", self.snap, self.volname)

        # Validate snapshot list
        g.log.info("Starting to list all snapshots")
        ret, out, _ = snap_list(self.mnode)
        self.assertEqual(ret, 0, ("Failed to list snapshot of volume %s"
                                  % self.volname))
        s_list = out.strip().split('\n')
        self.assertEqual(len(s_list), self.snap_count, "Failed to validate "
                         "all snapshots")
        g.log.info("Snapshot listed and  Validated for volume %s"
                   " successfully", self.volname)

        # Activating snapshot
        g.log.info("Activating snapshot")
        for count in range(0, self.snap_count):
            self.snap = "snap%s" % count
            ret, _, _ = snap_activate(self.mnode, self.snap)
            self.assertEqual(ret, 0, "Failed to Activate snapshot "
                             "%s" % self.snap)
            g.log.info("snapshot %s activated successfully", self.snap)

        # Validate IO is completed
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # validate snapshots are listed under .snaps directory
        g.log.info("Validating snaps under .snaps")
        ret = view_snaps_from_mount(self.mounts, s_list)
        self.assertTrue(ret, "Failed to list snaps under .snaps"
                        "directory")
        g.log.info("Snapshots Validated successfully")
