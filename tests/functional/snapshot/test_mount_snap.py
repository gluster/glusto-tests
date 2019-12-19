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
       Creation of snapshot and mounting that
       snapshot in the client.

"""
import os
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ConfigError, ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.io.utils import validate_io_procs
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_delete_by_volumename,
                                         snap_activate)
from glustolibs.gluster.mount_ops import create_mount_objs


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestSnapMountSnapshot(GlusterBaseClass):
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
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        Mount snap volume contains tests which verifies snapshot mount,
        creating snapshot and performing I/O on snapshot mount point.
        """
        self.mount1 = []
        self.mpoint = "/mnt/snap1"
        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_mount_snap_delete(self):
        """
        Mount the snap volume
        * Create volume, FUSE mount the volume
        * perform I/O on mount points
        * Creating snapshot and activate snapshot
        * FUSE mount the snapshot created
        * Perform I/O on mounted snapshot
        * I/O should fail
        """
        # pylint: disable=too-many-statements
        # starting I/O
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

        # Validate I/O
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Creating snapshot
        g.log.info("Starting to create snapshots")
        ret, _, _ = snap_create(self.mnode, self.volname, "snap1")
        self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                  % self.volname))
        g.log.info("Snapshot snap1 created successfully "
                   "for volume  %s", self.volname)

        # Activating snapshot
        g.log.info("Activating snapshot")
        ret, _, _ = snap_activate(self.mnode, "snap1")
        self.assertEqual(ret, 0, ("Failed to Activate snapshot snap1"))
        g.log.info("snap1 activated successfully")

        # redefine mounts
        self.mount_points = []
        self.mounts_dict_list = []
        for client in self.all_clients_info:
            mount = {
                'protocol': self.mount_type,
                'server': self.mnode,
                'volname': self.volname,
                'client': self.all_clients_info[client],
                'mountpoint': (os.path.join(
                    "/mnt/snap1")),
                'options': ''
            }
            self.mounts_dict_list.append(mount)
        self.mount1 = create_mount_objs(self.mounts_dict_list)
        g.log.info("Successfully made entry in self.mount1")

        # FUSE mount snap1 snapshot
        g.log.info("Mounting snapshot snap1")
        cmd = "mkdir -p  %s" % self.mpoint
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, ("Creation of directory %s"
                                  "for mounting"
                                  "volume snap1 failed"
                                  % (self.mpoint)))
        self.mount_points.append(self.mpoint)
        cmd = "mount -t glusterfs %s:/snaps/snap1/%s %s" % (self.mnode,
                                                            self.volname,
                                                            self.mpoint)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, ("Failed to mount snap1"))
        g.log.info("snap1 is mounted Successfully")

        # starting I/O
        g.log.info("Starting IO on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python%d %s create_files "
                   "-f 10 --base-file-name file %s" % (
                       sys.version_info.major, self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate I/O
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # start I/O
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mount1)
        all_mounts_procs = []
        for mount_obj in self.mount1:
            cmd = ("/usr/bin/env python%d %s create_files "
                   "-f 10 --base-file-name file %s" % (
                       sys.version_info.major, self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # validate io should fail
        self.assertFalse(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Unexpected: IO Successful on all clients"
        )
        g.log.info("Expected: IO failed on clients")

    def tearDown(self):
        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

        # unmounting volume from Custom mount point
        g.log.info("UnMounting mount point %s", self.mpoint)
        cmd = "umount %s" % self.mpoint
        ret, _, _ = g.run(self.clients[0], cmd)
        if ret != 0:
            raise ExecutionError("Unmounted Successfully"
                                 "from %s"
                                 % (self.mpoint))
        g.log.info("Successful in Unmounting %s", self.mpoint)
        ret, _, _ = snap_delete_by_volumename(self.mnode, self.volname)
        if ret != 0:
            raise ExecutionError("Failed to delete %s "
                                 "volume" % self.volname)
        g.log.info("Successfully deleted %s", self.volname)

        # Unmount and cleanup-volume
        g.log.info("Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ConfigError("Failed to Unmount and Cleanup Volume")
        g.log.info("Cleanup Volume Successfully")
