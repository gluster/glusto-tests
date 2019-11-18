#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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

from time import sleep
from random import randint
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_init import stop_glusterd, start_glusterd
from glustolibs.gluster.peer_ops import peer_status, is_peer_connected
from glustolibs.gluster.volume_ops import volume_list, volume_info
from glustolibs.gluster.volume_libs import (cleanup_volume, setup_volume)


@runs_on([['replicated'], ['glusterfs']])
class TestOpsWhenOneNodeIsDown(GlusterBaseClass):

    def setUp(self):

        GlusterBaseClass.setUp.im_func(self)

        # Create and start a volume.
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        if ret:
            ExecutionError("Failed to create and start volume")

    def tearDown(self):

        # Starting glusterd on node where stopped.
        ret = start_glusterd(self.servers[self.random_server])
        if ret:
            ExecutionError("Failed to start glusterd.")
        g.log.info("Successfully started glusterd.")

        # Checking if peer is connected.
        counter = 0
        while counter < 30:
            ret = is_peer_connected(self.mnode, self.servers)
            counter += 1
            if ret:
                break
            sleep(3)
        if not ret:
            ExecutionError("Peer is not in connected state.")
        g.log.info("Peers is in connected state.")

        # Stopping and deleting volume.
        ret = cleanup_volume(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Unable to delete volume % s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        GlusterBaseClass.tearDown.im_func(self)

    def test_ops_when_one_node_is_down(self):

        # pylint: disable=too-many-statements
        """
        Test Case:
        1) Create a N node gluster cluster.
        2) Stop gluster on one node.
        3) Execute gluster peer status on other node.
        4) Execute gluster v list on other node.
        5) Execute gluster v info on other node.
        """

        # Fetching a random server from list.
        self.random_server = randint(1, len(self.servers)-1)

        # Stopping glusterd on one node.
        ret = stop_glusterd(self.servers[self.random_server])
        self.assertTrue(ret, "Failed to stop glusterd on one node.")
        g.log.info("Successfully stopped glusterd on one node.")

        # Running peer status on another node.
        ret, _, err = peer_status(self.mnode)
        self.assertEqual(ret, 0, ("Failed to get peer status from %s with "
                                  "error message %s" % (self.mnode, err)))
        g.log.info("Successfully got peer status from %s.", self.mnode)

        # Running volume list on another node.
        ret, _, _ = volume_list(self.mnode)
        self.assertEqual(ret, 0, "Failed to get volume list.")
        g.log.info("Successfully got volume list from %s.", self.mnode)

        # Running volume info on another node.
        ret, _, _ = volume_info(self.mnode)
        self.assertEqual(ret, 0, "Failed to get volume info.")
        g.log.info("Successfully got volume info from %s.", self.mnode)
