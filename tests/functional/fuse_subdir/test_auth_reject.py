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
        Test Cases in this module tests the Fuse sub directory feature
"""
import copy
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.auth_ops import set_auth_reject


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class FuseSubDirAuthReject(GlusterBaseClass):
    """
    Tests to verify auth.reject functionality on Fuse subdir feature
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

    def test_auth_reject(self):
        """
        Check sub dir auth.reject functionality

        Steps:
        1. Create two sub directories on mounted volume
        2. Unmount volume from clients
        3. Set auth.reject on sub dir d1 for client1 and d2 for client2
        4. Mount d2 on client1 and d1 on client2. This should pass.
        5. Try to mount d2 on client2 and d1 on client1. This should fail.
        """
        # Creating sub directories on mounted volume
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

        # Setting authentication (auth.reject) for directories
        auth_dict = {'/d1': [self.mounts[0].client_system],
                     '/d2': [self.mounts[1].client_system]}
        ret = set_auth_reject(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set authentication")
        g.log.info("Successfully set authentication on sub directories")

        # Creating mounts list for authenticated client
        self.subdir_mounts = [copy.deepcopy(self.mounts[0]),
                              copy.deepcopy(self.mounts[1])]
        self.subdir_mounts[0].volname = "%s/d2" % self.volname
        self.subdir_mounts[1].volname = "%s/d1" % self.volname

        # Mounting sub directories on authenticated client
        for mount_obj in self.subdir_mounts:

            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount sub directory %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted sub directory %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully mounted sub directories to allowed clients")

        # Creating mounts list for rejected client
        self.unauth_subdir_mounts = [copy.deepcopy(self.mounts[0]),
                                     copy.deepcopy(self.mounts[1])]
        self.unauth_subdir_mounts[0].volname = "%s/d1" % self.volname
        self.unauth_subdir_mounts[1].volname = "%s/d2" % self.volname
        self.unauth_subdir_mounts[0].mountpoint \
            = "%s_unauth" % self.unauth_subdir_mounts[0].mountpoint
        self.unauth_subdir_mounts[1].mountpoint \
            = "%s_unauth" % self.unauth_subdir_mounts[1].mountpoint

        # Trying to mount sub directories on rejected client
        for mount_obj in self.unauth_subdir_mounts:
            if mount_obj.mount():
                g.log.warning("Mount command did not fail as expected. "
                              "sub-dir: %s, client: %s, mount point: %s",
                              mount_obj.volname, mount_obj.client_system,
                              mount_obj.mountpoint)
                ret = mount_obj.is_mounted()
                if ret:
                    self.subdir_mounts.append(mount_obj)
                    self.assertFalse(ret, ("Mount operation did not fail as "
                                           "expected. Mount operation of sub "
                                           "directory %s on client %s passed."
                                           "Mount point: %s"
                                           % (mount_obj.volname,
                                              mount_obj.client_system,
                                              mount_obj.mountpoint)))
                g.log.info("Mount command passed. But sub directory "
                           "is not mounted. This is expected. "
                           "sub-dir: %s, client: %s, mount point: %s",
                           mount_obj.volname, mount_obj.client_system,
                           mount_obj.mountpoint)
            g.log.info("Mount operation of sub directory %s on client %s "
                       "failed as expected.", mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Verified mount operation of sub-dirs on "
                   "rejected client. "
                   "Mount operation failed as expected.")

        # Unmount sub directories
        ret = self.unmount_volume(self.subdir_mounts)
        self.assertTrue(ret, "Failed to unmount one or more sub-directories")
        g.log.info("Successfully unmounted all sub-directories")

    def tearDown(self):
        """
        Unmounting and cleaning up
        """
        g.log.info("Unmounting sub-dir mounts")
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Unmount and cleanup sub-dir "
                                 "mounts")
        g.log.info("Successfully unmounted sub-dir mounts and cleaned")
