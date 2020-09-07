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


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class FuseSubDirMount(GlusterBaseClass):
    """
    Tests to verify Fuse subdir feature
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

    def test_subdir_mount(self):
        """
        Check sub dir mount functionality

        Steps:
        1. Create a sub-directory on the mounted volume.
        2. Unmount volume.
        3. Mount sub-directory on client. This should pass.
        """
        # Create sub directory on mounted volume
        ret = mkdir(self.mounts[0].client_system, "%s/d1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory in volume %s from "
                              "client %s"
                              % (self.volname,
                                 self.mounts[0].client_system)))

        # Unmount volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Failed to unmount one or more volumes")
        g.log.info("Successfully unmounted all volumes")

        # Creating mounts list for mounting sub-directories
        self.subdir_mounts = [copy.deepcopy(self.mounts[0])]
        self.subdir_mounts[0].volname = "%s/d1"\
                                        % self.volname

        # Mount sub-directory on client
        ret = self.subdir_mounts[0].mount()
        self.assertTrue(ret, ("Failed to mount sub directory %s"
                              % self.subdir_mounts[0].volname))
        g.log.info("Successfully mounted sub directory %s on client %s",
                   self.subdir_mounts[0].volname,
                   self.subdir_mounts[0].client_system)

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
