#  Copyright (C) 2018-2020 Red Hat, Inc. <http://www.redhat.com>
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
from unittest import skip
from glusto.core import Glusto as g
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline)
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

    def _get_min_brick(self):
        # Returns the brick with min size
        bricks_list = get_all_bricks(self.mnode, self.volname)
        min_brick_size = -1
        min_size_brick = None
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            brick_size = get_size_of_mountpoint(brick_node, brick_path)
            if ((brick_size is not None) and (min_brick_size == -1) or
                    (int(min_brick_size) > int(brick_size))):
                min_brick_size = brick_size
                min_size_brick = brick
        return min_size_brick, min_brick_size

    def _get_consumable_vol_size(self, min_brick_size):
        # Calculates the consumable size of the volume created
        vol_info = get_volume_info(self.mnode, self.volname)
        self.assertIsNotNone(vol_info, ("Unable to get the volinfo \
                    of %s.", self.volname))
        disp_data_bricks = (int(vol_info[self.volname]['disperseCount']) -
                            int(vol_info[self.volname]['redundancyCount']))
        dist_count = (int(vol_info[self.volname]['brickCount']) /
                      int(vol_info[self.volname]['disperseCount']))
        consumable_size = ((int(min_brick_size) * int(disp_data_bricks)) *
                           int(dist_count))
        return consumable_size, dist_count

    @skip('Skipping this test due to Bug 1883429')
    def test_disperse_vol_size(self):
        # pylint: disable=too-many-locals
        client = self.mounts[0].client_system
        mount_point = self.mounts[0].mountpoint

        # Obtain the volume size
        vol_size = get_size_of_mountpoint(client, mount_point)
        self.assertIsNotNone(vol_size, ("Unable to get the volsize "
                                        "of %s.", self.volname))

        # Retrieve the minimum brick size
        min_size_brick, min_brick_size = self._get_min_brick()

        # Calculate the consumable size
        consumable_size, dist_count = (
            self._get_consumable_vol_size(min_brick_size))

        # Verify the volume size is in allowable range
        # Volume size should be above 98% of consumable size.
        delta = (100 - ((float(vol_size)/float(consumable_size)) * 100))
        self.assertTrue(delta < 2, "Volume size is not in allowable range")
        g.log.info("Volume size is in allowable range")

        # Write to the available size
        block_size = 1024
        write_size = ((int(vol_size) * 0.95 * int(block_size)) /
                      (int(dist_count)))
        for i in range(1, int(dist_count)):
            ret, _, _ = g.run(client, "fallocate -l {} {}/testfile{} "
                              .format(int(write_size), mount_point, i))
            self.assertTrue(ret == 0, ("Writing file of available size "
                                       "failed on volume %s", self.volname))
        g.log.info("Successfully verified volume size")

        # Try writing more than the available size
        write_size = ((int(vol_size) * int(block_size)) * 1.2)
        ret, _, _ = g.run(client, "fallocate -l {} {}/testfile1 "
                          .format(int(write_size), mount_point))
        self.assertTrue(ret != 0, ("Writing file of more than available "
                                   "size passed on volume %s", self.volname))
        g.log.info("Successfully verified brick consumable size")

        # Cleanup the mounts to verify
        cmd = ('rm -rf %s' % mount_point)
        ret, _, _ = g.run(client, cmd)
        if ret:
            g.log.error("Failed to cleanup vol data on %s", mount_point)
        # Bring down the smallest brick
        ret = bring_bricks_offline(self.volname, min_size_brick)
        self.assertTrue(ret, "Failed to bring down the smallest brick")

        # Find the volume size post brick down
        post_vol_size = get_size_of_mountpoint(client, mount_point)
        self.assertIsNotNone(post_vol_size, ("Unable to get the volsize "
                                             "of %s.", self.volname))

        # Vol size after bringing down the brick with smallest size should
        # not be greater than the actual size
        self.assertGreater(vol_size, post_vol_size,
                           ("The volume size after bringing down the volume "
                            "is greater than the initial"))

    # Method to cleanup test setup
    def tearDown(self):
        # Stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")
