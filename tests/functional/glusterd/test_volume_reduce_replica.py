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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import (get_volume_list, get_volume_info)
from glustolibs.gluster.volume_libs import (cleanup_volume, setup_volume)
from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.brick_libs import get_all_bricks


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestVolumeReduceReplicaCount(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Override Volumes
        cls.volume['voltype'] = {
            'type': 'distributed-replicated',
            'dist_count': 2,
            'replica_count': 3,
            'transport': 'tcp'}

    def tearDown(self):

        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume % s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        # Removing brick directories
        for brick in self.brick_list:
            node, brick_path = brick.split(r':')
            cmd = "rm -rf " + brick_path
            ret, _, _ = g.run(node, cmd)
            if ret:
                raise ExecutionError("Failed to delete the brick "
                                     "dir's of deleted volume")

        GlusterBaseClass.tearDown.im_func(self)

    def test_volume_reduce_replica_count(self):
        """
        Test case:
        1) Create a 2x3 replica volume.
        2) Remove bricks in the volume to make it a 2x2 replica volume.
        3) Remove bricks in the volume to make it a distribute volume.
        """

        # Create and start a volume
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")

        # Getting a list of all the bricks.
        g.log.info("Get all the bricks of the volume")
        self.brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(self.brick_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        # Converting 2x3 to 2x2 volume.
        remove_brick_list = [self.brick_list[0], self.brick_list[3]]
        ret, _, _ = remove_brick(self.mnode, self.volname, remove_brick_list,
                                 'force', replica_count=2)
        self.assertEqual(ret, 0, "Failed to start remove brick operation")
        g.log.info("Remove brick operation successfully")

        # Checking if volume is 2x2 or not.
        volume_info = get_volume_info(self.mnode, self.volname)
        brick_count = int(volume_info[self.volname]['brickCount'])
        self.assertEqual(brick_count, 4, "Failed to remove 2 bricks.")
        g.log.info("Successfully removed 2 bricks.")
        type_string = volume_info[self.volname]['typeStr']
        self.assertEqual(type_string, 'Distributed-Replicate',
                         "Convertion to 2x2 failed.")
        g.log.info("Convertion to 2x2 successful.")

        # Converting 2x2 to distribute volume.
        remove_brick_list = [self.brick_list[1], self.brick_list[4]]
        ret, _, _ = remove_brick(self.mnode, self.volname, remove_brick_list,
                                 'force', replica_count=1)
        self.assertEqual(ret, 0, "Failed to start remove brick operation")
        g.log.info("Remove brick operation successfully")

        # Checking if volume is pure distribute or not.
        volume_info = get_volume_info(self.mnode, self.volname)
        brick_count = int(volume_info[self.volname]['brickCount'])
        self.assertEqual(brick_count, 2, "Failed to remove 2 bricks.")
        g.log.info("Successfully removed 2 bricks.")
        type_string = volume_info[self.volname]['typeStr']
        self.assertEqual(type_string, 'Distribute',
                         "Convertion to distributed failed.")
        g.log.info("Convertion to distributed successful.")
