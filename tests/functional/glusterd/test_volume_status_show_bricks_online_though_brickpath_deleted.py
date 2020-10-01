#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
      Volume status when one of the brickpath is not available.
"""

import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (are_bricks_online, get_all_bricks,
                                           bring_bricks_online,
                                           bring_bricks_offline,
                                           are_bricks_offline)
from glustolibs.gluster.volume_ops import (volume_start)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'], ['glusterfs']])
class TestVolumeStatusShowBrickOnlineThoughBrickpathDeleted(GlusterBaseClass):
    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s"
                                 % self.volname)
        g.log.info("Volume created successfully : %s", self.volname)

    def tearDown(self):
        # Stopping the volume and Cleaning up the volume
        if self.check_for_remount:
            ret, _, _ = g.run(self.brick_node, 'mount %s' % self.node_brick)
            if ret:
                raise ExecutionError('Failed to remount brick %s'
                                     % self.node_brick)
            g.log.info('Successfully remounted %s with read-write option',
                       self.node_brick)
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup the volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully: %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_volume_status_show_brick_online_though_brickpath_deleted(self):
        """
        Test Case:
        1) Create a volume and start it.
        2) Fetch the brick list
        3) Bring any one brick down umount the brick
        4) Force start the volume and check that all the bricks are not online
        5) Remount the removed brick and bring back the brick online
        6) Force start the volume and check if all the bricks are online
        """
        # Fetching the brick list
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Failed to get the bricks in"
                             " the volume")

        # Bringing one brick down
        random_brick = random.choice(brick_list)
        ret = bring_bricks_offline(self.volname, random_brick)
        self.assertTrue(ret, "Failed to bring offline")

        # Creating a list of bricks to be removed
        remove_bricks_list = []
        remove_bricks_list.append(random_brick)

        # Checking if the brick is offline or not
        ret = are_bricks_offline(self.mnode, self.volname,
                                 remove_bricks_list)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % random_brick)
        g.log.info('Brick %s is offline as expected', random_brick)

        # umounting the brick which was made offline
        self.brick_node, volume_brick = random_brick.split(':')
        self.node_brick = '/'.join(volume_brick.split('/')[0:3])
        g.log.info('Start umount brick %s...', self.node_brick)
        ret, _, _ = g.run(self.brick_node, 'umount %s' % self.node_brick)
        self.assertFalse(ret, 'Failed to umount brick %s' % self.node_brick)
        g.log.info('Successfully umounted brick %s', self.node_brick)

        self.check_for_remount = True

        # Force starting the volume
        ret, _, _ = volume_start(self.mnode, self.volname, True)
        self.assertEqual(ret, 0, "Faile to force start volume")
        g.log.info("Successfully force start volume")

        # remounting the offline brick
        g.log.info('Start remount brick %s with read-write option...',
                   self.node_brick)
        ret, _, _ = g.run(self.brick_node, 'mount %s' % self.node_brick)
        self.assertFalse(ret, 'Failed to remount brick %s' % self.node_brick)
        g.log.info('Successfully remounted %s with read-write option',
                   self.node_brick)

        self.check_for_remount = False

        # Checking that all the bricks shouldn't be online
        ret = are_bricks_online(self.mnode, self.volname, brick_list)
        self.assertFalse(ret, "Unexpected: All the bricks are online")
        g.log.info("Expected: All the bricks are not online")

        # Bringing back the offline brick online
        ret = bring_bricks_online(self.mnode, self.volname, remove_bricks_list)
        self.assertTrue(ret, "Failed to bring bricks online")
        g.log.info("Successfully brought bricks online")

        # Force starting the volume
        ret, _, _ = volume_start(self.mnode, self.volname, True)
        self.assertEqual(ret, 0, "Faile to force start volume")
        g.log.info("Successfully force start volume")

        # Checking if all the bricks are online or not
        ret = are_bricks_online(self.mnode, self.volname, brick_list)
        self.assertTrue(ret, "Unexpected: All the bricks are not online")
        g.log.info("Expected: All the bricks are online")
