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
        Test Cases in this module tests the failover operation when sub-dir
        is mounted
"""
import copy
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import (upload_scripts,
                                       reboot_nodes_and_wait_to_come_online)
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.auth_ops import set_auth_allow
from glustolibs.gluster.brick_libs import get_all_bricks, are_bricks_online


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'],
          ['glusterfs']])
class NodeRebootSubDirsMounted(GlusterBaseClass):
    """
    Tests to verify failover operation when sub-dir is mounted
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup and mount volume
        """
        GlusterBaseClass.setUpClass.im_func(cls)
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

    def test_node_reboot_subdir_mounted_io_running(self):
        """
        Verify node reboot operation when sub-dirs are mounted and IOs are
        running

        Steps:
        1. Create two sub-directories on mounted volume.
        2. Un mount volume from clients.
        3. Set auth.allow on sub dir d1 for client1 and d2 for client2.
        4. Mount sub-dir d1 on client1 and d2 on client2.
        5. Perform IO on mounts.
        6. Reboot the node from which sub-dirs are
           mounted and wait for node to come up.
        7. Verify if peers are connected.
        8. Check whether bricks are online.
        9. Validate IO process.
        """
        # Creating two sub directories on mounted volume
        ret = mkdir(self.mounts[0].client_system, "%s/d1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'd1' in volume %s "
                              "from client %s"
                              % (self.volname,
                                 self.mounts[0].client_system)))
        ret = mkdir(self.mounts[0].client_system, "%s/d2"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'd2' in volume %s "
                              "from client %s"
                              % (self.volname,
                                 self.mounts[0].client_system)))

        # Unmounting volumes
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Failed to unmount one or more volumes")
        g.log.info("Successfully unmounted all volumes")

        # Setting authentication for directories
        auth_dict = {'/d1': [self.mounts[0].client_system],
                     '/d2': [self.mounts[1].client_system]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set authentication")
        g.log.info("Successfully set authentication on sub directories")

        # Creating mounts list
        self.subdir_mounts = [copy.deepcopy(self.mounts[0]),
                              copy.deepcopy(self.mounts[1])]
        self.subdir_mounts[0].volname = "%s/d1" % self.volname
        self.subdir_mounts[1].volname = "%s/d2" % self.volname

        # Mounting sub directories to authenticated clients
        for mount_obj in self.subdir_mounts:
            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount sub directory %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted sub directory %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully mounted sub directories to authenticated "
                   "clients")

        # Start IO on all mounts.
        all_mounts_procs = []
        count = 1
        for mount_obj in self.subdir_mounts:
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

        # Reboot node and wait for node to come up.
        ret, _ = reboot_nodes_and_wait_to_come_online(self.mnode)
        self.assertTrue(ret, "Node reboot failed. Node %s has not came up"
                        % self.mnode)

        # Check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        self.assertTrue(ret, "All nodes are not in connected state.")

        # Get the bricks list of the volume
        g.log.info("Fetching bricks list of the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # Check whether all bricks are online
        g.log.info("Verifying whether all bricks are online.")
        ret = are_bricks_online(self.mnode, self.volname, bricks_list)
        self.assertTrue(ret, "All bricks are not online.")
        g.log.info("All bricks are online.")

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

        # Unmount sub-directories
        ret = self.unmount_volume(self.subdir_mounts)
        self.assertTrue(ret, "Failed to unmount one or more sub-directories")
        g.log.info("Successfully unmounted all sub-directories")

    def tearDown(self):
        """
        Unmounting and cleaning up
        """
        g.log.info("Un mounting sub-dir mounts")
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Unmount and cleanup sub-dir "
                                 "mounts")
        g.log.info("Successfully un mounted sub-dir mounts and cleaned")
