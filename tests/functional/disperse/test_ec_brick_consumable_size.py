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
EcBrickConsumableSize:

    This test verifies that the size of the volume will be
    'number of data bricks * least of brick size'.

"""
from glusto.core import Glusto as g
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (get_volume_info)
from glustolibs.gluster.lib_utils import get_size_of_mountpoint
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs']])
class EcBrickConsumableSize(GlusterBaseClass):

    # Method to setup the environment for test case
    def setUp(self):
        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    # Test Case
    def test_disperse_vol_size(self):
        # pylint: disable=too-many-locals
        mnode = self.mnode
        volname = self.volname
        client = self.mounts[0].client_system
        mountpoint = self.mounts[0].mountpoint

        # Obtain the volume size
        vol_size = get_size_of_mountpoint(client, mountpoint)
        self.assertIsNotNone(vol_size, ("Unable to get the volsize \
                    of %s.", volname))

        # Retrieve the minimum brick size
        min_brick_size = -1
        bricks_list = get_all_bricks(mnode, volname)
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            brick_size = get_size_of_mountpoint(brick_node, brick_path)
            if ((brick_size is not None) and (min_brick_size == -1) or
                    (int(min_brick_size) > int(brick_size))):
                min_brick_size = brick_size

        # Calculate the consumable size
        vol_info = get_volume_info(mnode, volname)
        self.assertIsNotNone(vol_info, ("Unable to get the volinfo \
                    of %s.", volname))

        disp_data_bricks = (int(vol_info[volname]['disperseCount']) -
                            int(vol_info[volname]['redundancyCount']))
        dist_count = (int(vol_info[volname]['brickCount']) /
                      int(vol_info[volname]['disperseCount']))
        consumable_size = ((int(min_brick_size) * int(disp_data_bricks)) *
                           int(dist_count))

        # Verify the volume size is in allowable range
        # Volume size should be above 98% of consumable size.
        delta = (100 - ((float(vol_size)/float(consumable_size)) * 100))
        self.assertTrue(delta < 2, ("Volume size is not in allowable range"))

        g.log.info("Volume size is in allowable range")

        # Write to the available size
        block_size = 1024
        write_size = ((int(vol_size) * (0.95) * int(block_size)) /
                      (int(dist_count)))
        for i in range(1, int(dist_count)):
            ret, _, _ = g.run(client, "fallocate -l {} {}/testfile{} \
                       ".format(int(write_size), mountpoint, i))
            self.assertTrue(ret == 0, ("Writing file of available size failed \
                    on volume %s", volname))
        g.log.info("Successfully verified volume size")

        # Try writing more than the available size
        write_size = ((int(vol_size) * int(block_size)) * 1.2)
        ret, _, _ = g.run(client, "fallocate -l {} {}/testfile1 \
                    ".format(int(write_size), mountpoint))
        self.assertTrue(ret != 0, ("Writing file of more than available \
                                   size passed on volume %s", volname))

        g.log.info("Successfully verified brick consumable size")

    # Method to cleanup test setup
    def tearDown(self):
        # Stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")
