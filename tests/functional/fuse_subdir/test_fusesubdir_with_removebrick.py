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
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.mount_ops import (mount_volume,
                                          umount_volume, is_mounted)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.auth_ops import set_auth_allow
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status, shrink_volume,
    wait_for_volume_process_to_be_online)


@runs_on([['distributed', 'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs']])
class SubdirWithRemoveBrick(GlusterBaseClass):
    """
    This test case validates fuse subdir functionality when remove-brick
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

    def test_subdir_with_removebrick(self):

        # pylint: disable=too-many-statements
        """
        Mount the volume
        Create 2 subdir on client subdir1 and subdir2
        Auth allow - Client1(subdir1,subdir2),Client2(subdir1,subdir2)
        Mount the subdir to their respective clients
        Start IO's on both subdirs
        Perform remove-brick
        Validate on client if subdir's are mounted post remove-brick
        operation is performed
        """
        # Create  directories subdir1 and subdir2 on mount point
        ret = mkdir(self.mounts[0].client_system, "%s/subdir1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'subdir1' in"
                              "volume %s from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))
        ret = mkdir(self.mounts[0].client_system, "%s/subdir2"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'subdir2' in"
                              "volume %s from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))
        # unmount volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Volumes UnMount failed")
        g.log.info("Volumes UnMounted successfully")

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

        self.mpoint = "/mnt/Mount_Point1"

        # Mount Subdir1 mount on client 1
        _, _, _ = mount_volume("%s/subdir1" % self.volname, self.mount_type,
                               self.mpoint, self.mnode, self.clients[0])

        # Checking subdir1 is mounted or not
        ret = is_mounted("%s/subdir1" % self.volname, self.mpoint,
                         self.mnode, self.clients[0], self.mount_type)
        self.assertTrue(ret, "Volume not mounted on mount point: %s"
                        % self.mpoint)
        g.log.info("Volume %s mounted on %s/subdir1", self.volname,
                   self.mpoint)

        # Mount Subdir2 mount on client 2
        _, _, _ = mount_volume("%s/subdir2" % self.volname, self.mount_type,
                               self.mpoint, self.mnode, self.clients[1])

        # Checking subdir2 is mounted or not
        ret = is_mounted("%s/subdir2" % self.volname, self.mpoint,
                         self.mnode, self.clients[1], self.mount_type)
        self.assertTrue(ret, "Volume not mounted on mount point: %s"
                        % self.mpoint)
        g.log.info("Volume %s mounted on %s/subdir2", self.volname,
                   self.mpoint)

        # Start IO on all the subdir mounts.
        self.subdir_mounts = [copy.deepcopy(self.mounts[0]),
                              copy.deepcopy(self.mounts[1])]
        self.subdir_mounts[0].volname = "%s/subdir1" % self.volname
        self.subdir_mounts[1].volname = "%s/subdir2" % self.volname
        all_mounts_procs = []
        count = 1
        for mount_obj in self.subdir_mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       self.mpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       sys.version_info.major, self.script_upload_path, count,
                       self.mpoint))
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

        # Perform remove brick operation when subdir is mounted on client
        g.log.info("Start removing bricks from volume")
        ret = shrink_volume(self.mnode, self.volname, rebalance_timeout=600)
        self.assertTrue(ret, ("Remove brick operation failed on "
                              "%s", self.volname))
        g.log.info("Remove brick operation is successful on "
                   "volume %s", self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("All volume %s processes failed to come up "
                              "online", self.volname))
        g.log.info("All volume %s processes came up "
                   "online successfully", self.volname)

        # Log Volume Info and Status after performing remove brick
        g.log.info("Logging volume info and Status after shrinking volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Again Checking subdir1 is mounted or not on Client 1
        ret = is_mounted("%s/subdir1" % self.volname, self.mpoint,
                         self.mnode, self.clients[0], self.mount_type)
        self.assertTrue(ret, "Volume not mounted on mount point: %s"
                        % self.mpoint)
        g.log.info("Volume %s mounted on %s/subdir1", self.volname,
                   self.mpoint)

        # Again Checking subdir2 is mounted or not on Client 2
        ret = is_mounted("%s/subdir2" % self.volname, self.mpoint,
                         self.mnode, self.clients[1], self.mount_type)
        self.assertTrue(ret, "Volume not mounted on mount point: %s"
                        % self.mpoint)
        g.log.info("Volume %s mounted on %s/subdir2", self.volname,
                   self.mpoint)

    def tearDown(self):
        """
        Clean up the volume and umount volume from client
        """

        # Unmount Volume from client
        g.log.info("Starting to Unmount volume")
        for client in self.clients:
            ret, _, _ = umount_volume(client, self.mpoint,
                                      self.mount_type)
            if ret != 0:
                raise ExecutionError("Unmounting the mount point %s failed"
                                     % self.mpoint)
            g.log.info("Unmount Volume Successful")
            cmd = ("rm -rf %s") % self.mpoint
            ret, _, _ = g.run(client, cmd)
            g.log.info("Mount point %s deleted successfully", self.mpoint)

        # cleanup-volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Cleanup volume %s Completed Successfully", self.volname)
