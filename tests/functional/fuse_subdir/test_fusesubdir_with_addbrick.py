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
import copy
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.auth_ops import set_auth_allow
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status, expand_volume,
    wait_for_volume_process_to_be_online)


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class SubdirWithAddBrick(GlusterBaseClass):
    """
    Test case validates fuse subdir functionality when add-brick
    and rebalance is performed
    """
    @classmethod
    def setUpClass(cls):
        """
        setup volume and mount volume
        calling GlusterBaseClass setUpClass
        """
        GlusterBaseClass.setUpClass.im_func(cls)

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

    def test_subdir_with_addbrick(self):

        # pylint: disable=too-many-statements
        """
        Mount the volume
        Create 2 subdir on mount point, subdir1 and subdir2
        Auth allow - Client1(subdir1,subdir2),Client2(subdir1,subdir2)
        Mount the subdir1 on client 1 and subdir2 on client2
        Start IO's on both subdirs
        Perform add-brick and rebalance
        """

        # Create  directories subdir1 and subdir2 on mount point
        ret = mkdir(self.mounts[0].client_system, "%s/subdir1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'subdir1' on"
                              "volume %s from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))
        ret = mkdir(self.mounts[0].client_system, "%s/subdir2"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'subdir2' on"
                              "volume %s from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))
        # unmount volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Volumes Unmount failed")
        g.log.info("Volumes Unmounted successfully")

        # Set authentication on the subdirectory subdir1
        # and subdir2 to access by 2 clients
        g.log.info('Setting authentication on subdir1 and subdir2'
                   'for client %s and %s', self.clients[0], self.clients[0])
        ret = set_auth_allow(self.volname, self.mnode,
                             {'/subdir1': [self.clients[0], self.clients[1]],
                              '/subdir2': [self.clients[0], self.clients[1]]})
        self.assertTrue(ret,
                        'Failed to set Authentication on volume %s'
                        % self.volume)

        # Creating mount list for subdirectories
        self.subdir_mounts = [copy.deepcopy(self.mounts[0]),
                              copy.deepcopy(self.mounts[1])]
        self.subdir_mounts[0].volname = "%s/subdir1" % self.volname
        self.subdir_mounts[1].volname = "%s/subdir2" % self.volname

        # Mount Subdirectory "subdir1" on client 1 and "subdir2" on client 2
        for mount_obj in self.subdir_mounts:
            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount  %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully mounted subdirectories on client1"
                   "and clients 2")

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

        # Start add-brick (subvolume-increase)
        g.log.info("Start adding bricks to volume when IO in progress")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume when IO in "
                              "progress on volume %s", self.volname))
        g.log.info("Expanding volume when IO in progress is successful on "
                   "volume %s", self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("All process  for volume %s are not"
                              "online", self.volname))
        g.log.info("All volume %s processes are now online",
                   self.volname)

        # Start Rebalance
        g.log.info("Starting Rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname, 600)
        self.assertTrue(ret, "Rebalance did not complete "
                             "despite waiting for 10 minutes")
        g.log.info("Rebalance successfully completed on the volume %s",
                   self.volname)

        # Again validate if subdirectories are still mounted post add-brick

        for mount_obj in self.subdir_mounts:
            ret = mount_obj.is_mounted()
            self.assertTrue(ret, ("Subdirectory %s is not mounted on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Subdirectory %s is mounted on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully validated that subdirectories are mounted"
                   "on client1 and clients 2 post add-brick operation")

    def tearDown(self):
        """
        Clean up the volume and umount volume from client
        """

        # Unmount sub-directories from client
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
