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

""" Description:
        Test Cases in this module tests the volume stop, start, reset
        operations when sub-dirs are mounted
"""
import copy
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import (volume_stop, volume_start,
                                           volume_reset)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class VolumeSetOpsSubDirsMounted(GlusterBaseClass):
    """
    Tests to verify volume stop, start, reset operations when sub-directories
    are mounted on fuse clients
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup and mount volume
        """
        cls.get_super_method(cls, 'setUpClass')()
        # Setup Volume and Mount Volume
        g.log.info("Starting volume setup and mount %s",
                   cls.volname)
        ret = cls.setup_volume_and_mount_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup "
                                 "and Mount_Volume %s" % cls.volname)
        g.log.info("Successfully set and mounted the volume: %s", cls.volname)

        # Upload io scripts for running IO on mounts
        g.log.info("Uploading IO scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)

        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to "
                   "clients %s", cls.clients)

    def test_volume_set_ops_sub_dirs_mounted(self):
        """
        Check volume start/volume stop/volume reset operations while sub-dirs
        are mounted

        Steps:
        1. Create two sub-directories on mounted volume.
        2. Unmount volume from clients.
        3. Mount each sub-directory to two different clients.
        4. Perform IO on mounts.
        5. Perform volume stop operation.
        6. Perform volume start operation.
        7. Perform volume reset operation.
        """
        # Creating two sub directories on mounted volume
        ret = mkdir(self.mounts[0].client_system, "%s/d1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'd1' in volume %s "
                              "from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))
        ret = mkdir(self.mounts[0].client_system, "%s/d2"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'd2' in volume %s "
                              "from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))

        # Unmounting volumes
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Failed to un mount one or more volumes")
        g.log.info("Successfully un mounted all volumes")

        # Mounting one sub directory on each client.
        self.subdir_mounts = [copy.deepcopy(self.mounts[0]),
                              copy.deepcopy(self.mounts[1])]
        self.subdir_mounts[0].volname = "%s/d1" % self.volname
        self.subdir_mounts[1].volname = "%s/d2" % self.volname
        for mount_obj in self.subdir_mounts:
            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount sub directory %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted sub directory %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully mounted sub directories to clients.")

        # Start IO on all mounts.
        all_mounts_procs = []
        count = 1
        for mount_obj in self.subdir_mounts:
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
        ret = validate_io_procs(all_mounts_procs, self.subdir_mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.subdir_mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # Stop volume
        g.log.info("Stopping volume: %s", self.volname)
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to stop volume: %s" % self.volname)

        # Start volume
        g.log.info("Starting volume again: %s", self.volname)
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start volume: %s" % self.volname)

        # Reset volume
        g.log.info("Resetting volume: %s", self.volname)
        ret, _, _ = volume_reset(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to reset volume: %s" % self.volname)

    def tearDown(self):
        """
        Unmounting and cleaning up
        """
        g.log.info("Unmounting sub-dir mounts")
        ret = self.unmount_volume_and_cleanup_volume(self.subdir_mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and cleanup sub-dir "
                                 "mounts")
        g.log.info("Successfully un mounted sub-dir mounts and cleaned")
