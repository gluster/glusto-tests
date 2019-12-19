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
                                          is_mounted)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.auth_ops import set_auth_allow
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.glusterfile import move_file


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class SubdirWithRename(GlusterBaseClass):
    """
    This test case validates fuse subdir functionality when earlier
    exported subdir is renamed along with auth allow functionality
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

    def test_subdir_when_renamed(self):

        # pylint: disable=too-many-statements
        """
        Mount the volume
        Create 1 subdir on mountpoint "d1"
        Auth allow - Client1(d1),Client2(full volume)
        Mount the subdir "d1" on client1 and volume on client2
        Start IO's on all the mount points
        Perform rename operation from client2.Rename the subdir
        "d1" to "d1_renamed"
        unmount volume and subdir from clients
        Try mounting "d1" on client 1.This should fail.
        Try mounting "d1_renamed" on client 1.This should fail.
        Again set authentication.Auth allow -
        Client1(d1_renamed),Client2(full volume)
        Mount "d1_renamed" on client1 and volume on client2
        """

        # Create  directory d1 on mount point
        ret = mkdir(self.mounts[0].client_system, "%s/d1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'd1' on"
                              "volume %s from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))
        # unmount volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Volumes Unmount failed")
        g.log.info("Volumes Unmounted successfully")

        # Set authentication on the subdirectoy "d1" to access by client1
        # and volume to access by client2
        g.log.info('Setting authentication on subdirectory d1 to access'
                   'by client %s and on volume to access by client %s',
                   self.clients[0], self.clients[1])
        ret = set_auth_allow(self.volname, self.mnode,
                             {'/d1': [self.clients[0]],
                              '/': [self.clients[1]]})
        self.assertTrue(ret,
                        'Failed to set Authentication on volume %s'
                        % self.volume)

        # Creating mount list for mounting subdir mount and volume
        self.subdir_mounts = [copy.deepcopy(self.mounts[0]),
                              copy.deepcopy(self.mounts[1])]
        self.subdir_mounts[0].volname = "%s/d1" % self.volname
        self.subdir_mounts[0].client_system = self.clients[0]
        self.subdir_mounts[1].client_system = self.clients[1]

        # Mount Subdirectory d1 on client 1 and volume on client 2
        for mount_obj in self.subdir_mounts:
            mountpoint = mount_obj.mountpoint
            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount  %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully mounted sub directory and volume to"
                   "authenticated clients")

        # Start IO on all the mounts.
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

        # Rename the subdir "d1" to "d1_renamed" from client2
        source_fpath = "%s/d1" % mountpoint
        dest_fpath = "%s/d1_renamed" % mountpoint
        ret = move_file(self.clients[1], source_fpath,
                        dest_fpath)
        self.assertTrue(ret, "Rename subdirectory failed")
        g.log.info('Renamed directory %s to %s', source_fpath,
                   dest_fpath)

        # unmount volume and subdir from client
        ret = self.unmount_volume(self.subdir_mounts)
        self.assertTrue(ret, "Volumes UnMount failed")
        g.log.info("Volumes Unmounted successfully")

        # Try mounting subdir "d1" on client1
        _, _, _ = mount_volume("%s/d1" % self.volname, self.mount_type,
                               mountpoint, self.mnode, self.clients[0])

        ret = is_mounted(self.volname, mountpoint,
                         self.mnode, self.clients[0], self.mount_type)
        self.assertEqual(ret, 0, "d1 mount should have failed.But d1 is"
                         "successfully mounted on mount point: %s"
                         % mountpoint)
        g.log.info("subdir %s/d1 is not mounted as expected %s", self.volname,
                   mountpoint)

        # Try mounting subdir "d1_renamed" on client1
        _, _, _ = mount_volume("%s/d1_renamed" % self.volname, self.mount_type,
                               mountpoint, self.mnode, self.clients[0])

        ret = is_mounted("%s/d1_renamed" % self.volname, mountpoint,
                         self.mnode, self.clients[0], self.mount_type)
        self.assertEqual(ret, 0, "d1_renamed mount should have failed.But"
                         "d1_renamed is successfully mounted on : %s"
                         % mountpoint)
        g.log.info("subdir %s/d1_renamed is not mounted as expected %s",
                   self.volname, mountpoint)

        # Set authentication on the subdirectoy "d1_renamed" to access
        # by client1 and volume to access by client2
        g.log.info('Setting authentication on subdirectory d1_renamed to'
                   'access by client %s and on volume to access by client %s',
                   self.clients[0], self.clients[1])
        ret = set_auth_allow(self.volname, self.mnode,
                             {'/d1_renamed': [self.clients[0]],
                              '/': [self.clients[1]]})
        self.assertTrue(ret,
                        'Failed to set Authentication on volume %s'
                        % self.volume)

        # Overwriting the list of subdir mount, directory d1 to d1_renamed
        self.subdir_mounts[0].volname = "%s/d1_renamed" % self.volname

        # Mount Subdirectory d1_renamed on client 1 and volume on client 2
        for mount_obj in self.subdir_mounts:
            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount  %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)

        g.log.info("Successfully mounted sub directory and volume to"
                   "authenticated clients")

        # Get stat of all the files/dirs created from both clients.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.subdir_mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

    def tearDown(self):
        """
        Clean up the volume and umount volume from client
        """

        # Unmount sub-directory and volume
        # Test needs to continue if  unmount fail.Not asserting here.
        ret = self.unmount_volume(self.subdir_mounts)
        if not ret:
            g.log.info(ret, "Failed to unmount sub-directory or volume")
        g.log.info("Successfully unmounted subdirectory and volume")

        # cleanup-volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Cleanup volume %s Completed Successfully", self.volname)
