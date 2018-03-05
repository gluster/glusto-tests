#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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

import random
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import setup_volume, cleanup_volume
from glustolibs.gluster.volume_ops import (get_volume_list)
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.rebalance_ops import rebalance_start


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestVolumeCreate(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # check whether peers are in connected state
        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

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

        GlusterBaseClass.tearDown.im_func(self)

    def test_add_brick_functionality(self):

        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume %s"
                        % self.volname)
        g.log.info("Volume created and started successfully")

        # form bricks list to test add brick functionality

        replica_count_of_volume = self.volume['voltype']['replica_count']
        num_of_bricks = 4 * replica_count_of_volume
        bricks_list = form_bricks_list(self.mnode, self.volname,
                                       num_of_bricks, self.servers,
                                       self.all_servers_info)
        self.assertIsNotNone(bricks_list, "Bricks list is None")

        # Try to add a single brick to volume, which should fail as it is a
        # replicated volume, we should pass multiple of replica count number
        # of bricks

        bricks_list_to_add = [bricks_list[0]]
        ret, _, _ = add_brick(self.mnode, self.volname, bricks_list_to_add)
        self.assertNotEqual(ret, 0, "Expected: It should fail to add a single"
                            "brick to a replicated volume. Actual: "
                            "Successfully added single brick to volume")
        g.log.info("failed to add a single brick to replicated volume")

        # add brick replica count number of bricks in which one is
        # non existing brick
        kwargs = {}
        kwargs['replica_count'] = replica_count_of_volume

        bricks_list_to_add = bricks_list[1:replica_count_of_volume + 1]

        num_of_bricks = len(bricks_list_to_add)
        index_of_non_existing_brick = random.randint(0, num_of_bricks - 1)
        complete_brick = bricks_list_to_add[index_of_non_existing_brick]
        non_existing_brick = complete_brick + "/non_existing_brick"
        bricks_list_to_add[index_of_non_existing_brick] = non_existing_brick

        ret, _, _ = add_brick(self.mnode, self.volname,
                              bricks_list_to_add, False, **kwargs)
        self.assertNotEqual(ret, 0, "Expected: It should fail to add non"
                            "existing brick to a volume. Actual: "
                            "Successfully added non existing brick to volume")
        g.log.info("failed to add a non existing brick to volume")

        # adding brick from node which is not part of cluster
        bricks_list_to_add = bricks_list[replica_count_of_volume + 1:
                                         (2 * replica_count_of_volume) + 1]

        num_of_bricks = len(bricks_list_to_add)
        index_of_node = random.randint(0, num_of_bricks - 1)
        complete_brick = bricks_list_to_add[index_of_node].split(":")
        complete_brick[0] = "abc.def.ghi.jkl"
        bricks_list_to_add[index_of_node] = ":".join(complete_brick)
        ret, _, _ = add_brick(self.mnode, self.volname,
                              bricks_list_to_add, False, **kwargs)
        self.assertNotEqual(ret, 0, "Expected: It should fail to add brick "
                            "from a node which is not part of a cluster."
                            "Actual:Successfully added bricks from node which"
                            " is not a part of cluster to volume")

        g.log.info("Failed to add bricks form node which is not a part of "
                   "cluster to volume")

        # add correct number of valid bricks, it should succeed

        bricks_list_to_add = bricks_list[(2 * replica_count_of_volume) + 1:
                                         (3 * replica_count_of_volume) + 1]
        ret, _, _ = add_brick(self.mnode, self.volname,
                              bricks_list_to_add, False, **kwargs)
        self.assertEqual(ret, 0, "Failed to add the bricks to the volume")
        g.log.info("Successfully added bricks to volume")

        # Perform rebalance start operation
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Rebalance start is success")
