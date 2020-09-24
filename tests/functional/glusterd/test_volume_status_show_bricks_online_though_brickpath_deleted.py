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
from glustolibs.gluster.brick_libs import (are_bricks_online, get_all_bricks)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'], ['glusterfs']])
class TestVolumeStatusShowBrickOnlineThoughBrickpathDeleted(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Creating and starting Volume
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s"
                                 % self.volname)
        g.log.info("Volume created successfully : %s", self.volname)

    def tearDown(self):
        # Stopping the volume and Cleaning up the volume
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
        3) Remove any brickpath
        4) Check number of bricks online is equal to number of bricks in volume
        """
        # Fetching the brick list
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Failed to get the bricks in"
                             " the volume")

        # Command for removing brick directory
        random_brick = random.choice(brick_list)
        node, brick_path = random_brick.split(r':')
        cmd = 'rm -rf ' + brick_path

        # Removing brick directory of one node
        ret, _, _ = g.run(node, cmd)
        self.assertEqual(ret, 0, "Failed to remove brick dir")
        g.log.info("Brick directory removed successfully")

        # Checking if all the bricks are online or not
        ret = are_bricks_online(self.mnode, self.volname, brick_list)
        self.assertTrue(ret, "Unexpected: All the bricks are not online")
        g.log.info("Expected: All the bricks are online")
