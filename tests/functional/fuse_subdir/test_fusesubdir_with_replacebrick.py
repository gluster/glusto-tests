#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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
import copy

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.auth_ops import set_auth_allow
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status,
    replace_brick_from_volume,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.heal_libs import monitor_heal_completion


@runs_on([['replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class SubdirWithReplaceBrick(GlusterBaseClass):
    """
    This test case validates fuse subdir functionality when replace-brick
    operation is performed
    """
    @classmethod
    def setUpClass(cls):
        """
        setup volume and mount volume
        calling GlusterBaseClass setUpClass
        """
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup and Mount Volume %s",
                   cls.volname)
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume "
                                 "and Mount_Volume %s" % cls.volname)
        g.log.info("Successful in Setup and Mount Volume %s", cls.volname)

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

    def test_subdir_with_replacebrick(self):

        # pylint: disable=too-many-statements
        """
        Mount the volume
        Create 50 directories on mount point
        Unmount volume
        Auth allow - Client1(subdir25),Client2(subdir15)
        Mount the subdir to their authorized respective clients
        Start IO's on both subdirs
        Perform replace-brick
        Validate on client if subdir's are mounted post replace-brick
        operation is performed
        Stat data on subdirs
        """
        # Create  directories on mount point
        for i in range(0, 50):
            ret = mkdir(self.mounts[0].client_system, "%s/subdir%s"
                        % (self.mounts[0].mountpoint, i))
            self.assertTrue(ret, ("Failed to create directory %s/subdir%s on"
                                  "volume from client %s"
                                  % (self.mounts[0].mountpoint, i,
                                     self.mounts[0].client_system)))
        g.log.info("Successfully created directories on mount point")

        # unmount volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Volumes Unmount failed")
        g.log.info("Volumes Unmounted successfully")

        # Set authentication on the subdirectory subdir25 to access by
        # client1 and subdir15 to access by 2 clients
        g.log.info('Setting authentication on subdir25 and subdir15'
                   'for client %s and %s', self.clients[0], self.clients[1])
        ret = set_auth_allow(self.volname, self.mnode,
                             {'/subdir25': [self.mounts[0].client_system],
                              '/subdir15': [self.mounts[1].client_system]})
        self.assertTrue(ret,
                        'Failed to set Authentication on volume %s'
                        % self.volume)

        # Creating mount list for mounting selected subdirs on authorized
        # clients
        self.subdir_mounts = [copy.deepcopy(self.mounts[0]),
                              copy.deepcopy(self.mounts[1])]
        self.subdir_mounts[0].volname = "%s/subdir25" % self.volname
        self.subdir_mounts[1].volname = "%s/subdir15" % self.volname

        # Mount Subdirectory subdir25 on client 1 and subdir15 on client 2
        for mount_obj in self.subdir_mounts:
            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount  %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully mounted sub directories on"
                   "authenticated clients")

        # Start IO on all the subdir mounts.
        all_mounts_procs = []
        count = 1
        for mount_obj in self.subdir_mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.subdir_mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # Log Volume Info and Status before replacing brick from the volume.
        g.log.info("Logging volume info and Status before replacing brick "
                   "from the volume %s", self.volname)
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Replace brick from a sub-volume
        g.log.info("Replace a brick from the volume")
        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        self.servers, self.all_servers_info)
        self.assertTrue(ret, "Failed to replace  brick from the volume")
        g.log.info("Successfully replaced brick from the volume")

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("All volume %s processes failed to come up "
                              "online", self.volname))
        g.log.info("All volume %s processes came up "
                   "online successfully", self.volname)

        # Log Volume Info and Status after replacing the brick
        g.log.info("Logging volume info and Status after replacing brick "
                   "from the volume %s", self.volname)
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed Logging volume info and status on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Wait for self-heal to complete
        g.log.info("Wait for self-heal to complete")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')
        g.log.info("self-heal is successful after replace-brick operation")

        # Again validate if subdirectories are still mounted post replace-brick
        for mount_obj in self.subdir_mounts:
            ret = mount_obj.is_mounted()
            self.assertTrue(ret, ("Subdirectory %s is not mounted on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Subdirectory %s is mounted on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully validated that subdirectories are mounted"
                   "on client1 and clients 2 post replace-brick operation")

        # Validate IO
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.subdir_mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

    def tearDown(self):
        """
        Unmount subdirectories and cleanup the volume
        """
        # Unmount the sub-directories
        # Test needs to continue if  unmount fail.Not asserting here.
        ret = self.unmount_volume(self.subdir_mounts)
        if ret:
            g.log.info("Successfully unmounted all the subdirectories")
        else:
            g.log.error("Failed to unmount sub-directories")

        # cleanup-volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Cleanup volume %s Completed Successfully", self.volname)
