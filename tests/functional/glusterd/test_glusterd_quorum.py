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

from time import sleep
import pytest
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import wait_for_bricks_to_be_online
from glustolibs.gluster.volume_libs import (setup_volume, volume_exists,
                                            cleanup_volume)
from glustolibs.gluster.gluster_init import (stop_glusterd, start_glusterd,
                                             is_glusterd_running)
from glustolibs.gluster.volume_ops import (set_volume_options, volume_start,
                                           volume_stop, volume_delete,
                                           get_volume_list, volume_reset)
from glustolibs.gluster.peer_ops import (is_peer_connected, peer_probe_servers,
                                         peer_detach_servers, peer_probe)


@runs_on([['distributed-replicated', 'replicated'], ['glusterfs']])
class TestServerQuorum(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()
        ret = volume_exists(cls.mnode, cls.volname)
        if ret:
            ret = cleanup_volume(cls.mnode, cls.volname)
            if not ret:
                raise ExecutionError("Unable to delete volume")
            g.log.info("Successfully deleted volume % s", cls.volname)

        # Check if peer is connected state or not and detach all the nodes
        for server in cls.servers:
            ret = is_peer_connected(server, cls.servers)
            if ret:
                ret = peer_detach_servers(server, cls.servers)
                if not ret:
                    raise ExecutionError(
                        "Detach failed from all the servers from the node.")
                g.log.info("Peer detach SUCCESSFUL.")

        # Before starting the testcase, proceed only it has minimum of 4 nodes
        if len(cls.servers) < 4:
            raise ExecutionError("Minimun four nodes required for this "
                                 " testcase to execute")

    @classmethod
    def tearDownClass(cls):

        # Setting quorum ratio to 51%
        ret = set_volume_options(cls.mnode, 'all',
                                 {'cluster.server-quorum-ratio': '51%'})
        if not ret:
            raise ExecutionError("Failed to set server quorum ratio on %s"
                                 % cls.volname)

        vol_list = get_volume_list(cls.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get volume list")

        for volume in vol_list:
            ret = cleanup_volume(cls.mnode, volume)
            if not ret:
                raise ExecutionError("Failed Cleanup the volume")
            g.log.info("Volume deleted successfully %s", volume)

        # Peer probe servers since we are doing peer detach in setUpClass
        for server in cls.servers:
            ret = is_peer_connected(server, cls.servers)
            if not ret:
                ret = peer_probe_servers(server, cls.servers)
                if not ret:
                    raise ExecutionError(
                        "Peer probe failed to one of the node")
                g.log.info("Peer probe successful")

        cls.get_super_method(cls, 'tearDownClass')()

    @pytest.mark.test_glusterd_quorum_validation
    def test_glusterd_quorum_validation(self):
        """
        -> Creating two volumes and starting them, stop the second volume
        -> set the server quorum and set the ratio to 90
        -> Stop the glusterd in one of the node, so the quorum won't meet
        -> Peer probing a new node should fail
        -> Volume stop will fail
        -> volume delete will fail
        -> volume reset will fail
        -> Start the glusterd on the node where it is stopped
        -> Volume stop, start, delete will succeed once quorum is met
        """
        # pylint: disable=too-many-statements, too-many-branches

        # Peer probe first 3 servers
        servers_info_from_three_nodes = {}
        for server in self.servers[0:3]:
            servers_info_from_three_nodes[
                server] = self.all_servers_info[server]

            # Peer probe the first 3 servers
            ret, _, _ = peer_probe(self.mnode, server)
            self.assertEqual(ret, 0,
                             ("Peer probe failed to one of the server"))
        g.log.info("Peer probe to first 3 nodes succeeded")

        self.volume['servers'] = self.servers[0:3]
        # Create a volume using the first 3 nodes
        ret = setup_volume(self.mnode, servers_info_from_three_nodes,
                           self.volume, force=True)
        self.assertTrue(ret, ("Failed to create and start volume"))
        g.log.info("Volume created and started successfully")

        # Creating another volume and stopping it
        second_volume = "second_volume"
        self.volume['name'] = second_volume
        ret = setup_volume(self.mnode, servers_info_from_three_nodes,
                           self.volume, force=True)
        self.assertTrue(ret, ("Failed to create and start volume"))
        g.log.info("Volume created and started succssfully")

        # stopping the second volume
        g.log.info("Stopping the second volume %s", second_volume)
        ret, _, _ = volume_stop(self.mnode, second_volume)
        self.assertEqual(ret, 0, ("Failed to stop the volume"))
        g.log.info("Successfully stopped second volume %s", second_volume)

        # Setting the server-quorum-type as server
        self.options = {"cluster.server-quorum-type": "server"}
        vol_list = get_volume_list(self.mnode)
        self.assertIsNotNone(vol_list, "Failed to get the volume list")
        g.log.info("Fetched the volume list")
        for volume in vol_list:
            g.log.info("Setting the server-quorum-type as server"
                       " on volume %s", volume)
            ret = set_volume_options(self.mnode, volume, self.options)
            self.assertTrue(ret, ("Failed to set the quorum type as a server"
                                  " on volume %s", volume))
        g.log.info("Server Quorum type is set as a server")

        # Setting the server quorum ratio to 90
        self.quorum_perecent = {'cluster.server-quorum-ratio': '90%'}
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertTrue(ret, ("Failed to set the server quorum ratio "
                              "to 90 on servers"))
        g.log.info("Successfully set server quorum ratio to 90% on servers")

        # Stop glusterd on one of the node
        ret = stop_glusterd(self.servers[2])
        self.assertTrue(ret, ("Failed to stop glusterd on "
                              "node %s", self.servers[2]))
        g.log.info("Glusterd stop on the nodes : %s"
                   " succeeded", self.servers[2])

        # Check glusterd is stopped
        ret = is_glusterd_running(self.servers[2])
        self.assertEqual(ret, 1, "Unexpected: Glusterd is running on node")
        g.log.info("Expected: Glusterd stopped on node %s", self.servers[2])

        # Adding a new peer will fail as quorum not met
        ret, _, _ = peer_probe(self.mnode, self.servers[3])
        self.assertNotEqual(ret, 0, (
            "Unexpected:"
            "Succeeded to peer probe new node %s when quorum "
            "is not met", self.servers[3]))
        g.log.info("Failed to peer probe new node as expected"
                   " when quorum not met")

        # Stopping an already started volume should fail as quorum is not met
        ret, _, _ = volume_start(self.mnode, second_volume)
        self.assertNotEqual(ret, 0, "Unexpected: Successfuly started "
                            "volume even when quorum not met.")
        g.log.info("Volume start %s failed as expected when quorum "
                   "is not met", second_volume)

        # Stopping a volume should fail stop the first volume
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 1, "Unexpected: Successfully stopped"
                         " volume even when quourm is not met")
        g.log.info("volume stop %s failed as expected when quorum "
                   "is not met", self.volname)

        # Stopping a volume with force option should fail
        ret, _, _ = volume_stop(self.mnode, self.volname, force=True)
        self.assertNotEqual(ret, 0, "Unexpected: Successfully "
                            "stopped volume with force. Expected: "
                            "Volume stop should fail when quourm is not met")
        g.log.info("volume stop failed as expected when quorum is not met")

        # Deleting a volume should fail. Deleting the second volume.
        ret = volume_delete(self.mnode, second_volume)
        self.assertFalse(ret, "Unexpected: Volume delete was "
                         "successful even when quourm is not met")
        g.log.info("volume delete failed as expected when quorum is not met")

        # Volume reset should fail when quorum is not met
        ret, _, _ = volume_reset(self.mnode, self.volname)
        self.assertNotEqual(ret, 0, "Unexpected: Volume reset was "
                            "successful even when quorum is not met")
        g.log.info("volume reset failed as expected when quorum is not met")

        # Volume reset should fail even with force when quourum is not met
        ret, _, _ = volume_reset(self.mnode, self.volname, force=True)
        self.assertNotEqual(ret, 0, "Unexpected: Volume reset was "
                            "successful with force even "
                            "when quourm is not met")
        g.log.info("volume reset failed as expected when quorum is not met")

        # Start glusterd on the node where glusterd is stopped
        ret = start_glusterd(self.servers[2])
        self.assertTrue(ret, "Failed to start glusterd on one node")
        g.log.info("Started glusterd on server"
                   " %s successfully", self.servers[2])

        ret = is_glusterd_running(self.servers[2])
        self.assertEqual(ret, 0, ("glusterd is not running on "
                                  "node %s", self.servers[2]))
        g.log.info("glusterd is running on node"
                   " %s ", self.servers[2])

        # Check peer status whether all peer are in connected state none of the
        # nodes should be in peer rejected state
        halt, counter, _rc = 30, 0, False
        g.log.info("Wait for some seconds, right after glusterd start it "
                   "will create two daemon process it need few seconds "
                   "(like 3-5) to initialize the glusterd")
        while counter < halt:
            ret = is_peer_connected(self.mnode, self.servers[0:3])
            if not ret:
                g.log.info("Peers are not connected state,"
                           " Retry after 2 seconds .......")
                sleep(2)
                counter = counter + 2
            else:
                _rc = True
                g.log.info("Peers are in connected state in the cluster")
                break

        self.assertTrue(_rc, ("Peers are not connected state after "
                              "bringing back glusterd online on the "
                              "nodes in which previously glusterd "
                              "had been stopped"))

        # Check all bricks are online or wait for the bricks to be online
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "All bricks are not online")
        g.log.info("All bricks of the volume %s are online", self.volname)

        # Once quorum is met should be able to cleanup the volume
        ret = volume_delete(self.mnode, second_volume)
        self.assertTrue(ret, "Volume delete failed even when quorum is met")
        g.log.info("volume delete succeed without any issues")

        # Volume stop should succeed
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Volume stop failed")
        g.log.info("succeeded stopping the volume as expected")

        # volume reset should succeed
        ret, _, _ = volume_reset(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Volume reset failed ")
        g.log.info("volume reset succeeded as expected when quorum is not met")

        # Peer probe new node should succeed
        ret, _, _ = peer_probe(self.mnode, self.servers[3])
        self.assertEqual(ret, 0, (
            "Failed to peer probe new node even when quorum is met"))
        g.log.info("Succeeded to peer probe new node when quorum met")

        # Check peer status whether all peer are in connected state none of the
        # nodes should be in peer rejected state
        halt, counter, _rc = 30, 0, False
        g.log.info("Wait for some seconds, right after peer probe")
        while counter < halt:
            ret = is_peer_connected(self.mnode, self.servers[0:3])
            if not ret:
                g.log.info("Peers are not connected state,"
                           " Retry after 2 seconds .......")
                sleep(2)
                counter = counter + 2
            else:
                _rc = True
                g.log.info("Peers are in connected state in the cluster")
                break

        self.assertTrue(_rc, ("Peers are not connected state"))
