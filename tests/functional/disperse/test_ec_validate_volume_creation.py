#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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
EcValidateVolumeCreate:

    This module tries to create and validate EC volume
    with various combinations of input parameters.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs']])
class EcValidateVolumeCreate(GlusterBaseClass):

    # Method to setup the environment for test case
    def setUp(self):
        # Initialize the input parameters
        self.volume['voltype']['redundancy_count'] = 0
        self.volume['voltype']['disperse_count'] = 0

    # Verify volume creation with insufficient bricks
    def test_insufficient_brick_servers(self):
        # Setup input parameters
        self.volume['voltype']['redundancy_count'] = 2
        self.volume['voltype']['disperse_count'] = 6

        # Restrict the brick servers to data brick count
        default_servers = self.volume['servers']
        data_brick_count = (self.volume['voltype']['disperse_count'] -
                            self.volume['voltype']['redundancy_count'])
        self.volume['servers'] = self.servers[0:data_brick_count]

        # Setup Volume and Mount Volume without force
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        self.assertFalse(ret, ("Volume setup is not failing "
                               "for volume %s", self.volname))
        g.log.info("Successfully verified volume setup with insufficient "
                   "brick servers witout force option")

        # Setup Volume and Mount Volume with force
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=True)
        self.assertTrue(ret, ("Volume Setup and Mount failed for "
                              "volume %s", self.volname))
        g.log.info("Successfully verified volume setup with insufficient "
                   "brick servers with force option")

        # Stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        self.assertTrue(ret, ("Failed to Unmount Volume and Cleanup Volume"))
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Restore the default server list
        self.volume['servers'] = default_servers

    # Test cases to verify valid input combinations.
    def test_valid_usecase_one(self):
        # Setup input parameters
        self.volume['voltype']['redundancy_count'] = 2
        self.volume['voltype']['disperse_count'] = 6

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        self.assertTrue(ret, ("Volume Setup and Mount failed for volume %s",
                              self.volname))

        # Stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        self.assertTrue(ret, ("Failed to Unmount Volume and "
                              "Cleanup Volume"))
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

    # Test cases to verify invalid input combinations.
    def test_invalid_usecase_one(self):
        # Setup input parameters
        self.volume['voltype']['redundancy_count'] = 0
        self.volume['voltype']['disperse_count'] = 6

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        self.assertFalse(ret, ("Volume Setup and Mount succeeded"
                               " for volume %s", self.volname))
        g.log.info("Successfully verified invalid input parameters")

    def test_invalid_usecase_two(self):
        # Setup input parameters
        self.volume['voltype']['redundancy_count'] = 3
        self.volume['voltype']['disperse_count'] = 6

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        self.assertFalse(ret, ("Volume Setup and Mount succeeded for volume "
                               "%s", self.volname))
        g.log.info("Successfully verified invalid input parameters")

    def test_invalid_usecase_three(self):
        # Setup input parameters
        self.volume['voltype']['redundancy_count'] = 4
        self.volume['voltype']['disperse_count'] = 6

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        self.assertFalse(ret, ("Volume Setup and Mount succeeded for volume "
                               "%s", self.volname))
        g.log.info("Successfully verified invalid input parameters")

    def test_invalid_usecase_four(self):
        # Setup input parameters
        self.volume['voltype']['redundancy_count'] = 6
        self.volume['voltype']['disperse_count'] = 6

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        self.assertFalse(ret, ("Volume Setup and Mount succeeded for volume "
                               "%s", self.volname))
        g.log.info("Successfully verified invalid input parameters")
