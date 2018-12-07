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
Test Description:
    Tests bricks EC version on a EC vol
    Don't send final version update if non data fop succeeded validation
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import (get_all_bricks)
from glustolibs.gluster.lib_utils import (get_extended_attributes_info)


@runs_on([['dispersed'],
          ['glusterfs']])
class TestEcVersion(GlusterBaseClass):

    def setUp(self):
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    def test_ec_version_nonfop(self):
        # pylint: disable=too-many-statements,too-many-branches,too-many-locals

        # get the bricks from the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # Creating dir1 on the mountpoint
        cmd = ('mkdir %s/dir1' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create directory dir1")
        g.log.info("Directory dir1 on %s created successfully",
                   self.mounts[0])

        ec_version_before_nonfops = []
        ec_version_after_nonfops = []
        # Getting the EC version of the directory
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            target_file = brick_path + "/dir1"
            dir_attribute = get_extended_attributes_info(brick_node,
                                                         [target_file])
            ec_version_before_nonfops.append(dir_attribute
                                             [target_file]
                                             ['trusted.ec.version'])

        # chmod of dir1 once
        cmd = ('chmod 777 %s/dir1'
               % (self.mounts[0].mountpoint))
        g.run(self.mounts[0].client_system, cmd)

        # chmod of dir1 twice
        cmd = ('chmod 777 %s/dir1'
               % (self.mounts[0].mountpoint))
        g.run(self.mounts[0].client_system, cmd)

        # Getting the EC version of the directory
        # After changing mode of the directory
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            target_file = brick_path + "/dir1"
            dir_attribute = get_extended_attributes_info(brick_node,
                                                         [target_file])
            ec_version_after_nonfops.append(dir_attribute
                                            [target_file]
                                            ['trusted.ec.version'])

        # Comparing the EC version before and after non data FOP
        self.assertEqual(ec_version_before_nonfops, ec_version_after_nonfops,
                         "EC version updated for non data FOP")
        g.log.info("EC version is same for before and after non data FOP"
                   "%s", self.volname)
