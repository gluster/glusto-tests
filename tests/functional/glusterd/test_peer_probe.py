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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list, volume_stop,
                                           volume_delete)
from glustolibs.gluster.volume_libs import (cleanup_volume)
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         peer_probe_servers,
                                         is_peer_connected,
                                         nodes_from_pool_list)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed'], ['glusterfs']])
class TestPeerProbe(GlusterBaseClass):

    def setUp(self):

        # Performing peer detach
        for server in self.servers[1:]:
            ret, _, _ = peer_detach(self.mnode, server)
            if ret != 0:
                raise ExecutionError("Peer detach failed")
            g.log.info("Peer detach SUCCESSFUL.")
        GlusterBaseClass.setUp.im_func(self)

    def tearDown(self):
        """
        clean up all volumes and peer probe to form cluster
        """
        vol_list = get_volume_list(self.mnode)
        if vol_list is not None:
            for volume in vol_list:
                ret = cleanup_volume(self.mnode, volume)
                if not ret:
                    raise ExecutionError("Failed to cleanup volume")
                g.log.info("Volume deleted successfully : %s", volume)

        # Peer probe detached servers
        pool = nodes_from_pool_list(self.mnode)
        for node in pool:
            peer_detach(self.mnode, node)
        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to probe detached "
                                 "servers %s" % self.servers)
        g.log.info("Peer probe success for detached "
                   "servers %s", self.servers)
        GlusterBaseClass.tearDown.im_func(self)

    def test_peer_probe(self):
        """
        In this test case:
        1. Create Dist Volume on Node 1
        2. Create Replica Volume on Node 2
        3. Peer Probe N2 from N1(should fail)
        4. Clean All Volumes
        5. Peer Probe N1 to N2(should success)
           Peer Probe N3 to N2(should fail)
        6. Create replica Volume on N1 and N2
        7. Peer probe from N3 to N1(should fail)
        8. Peer probe from N1 to N3(should succeed)
        9. Create replica Volume on N1, N2 and N2
        10.Start Volume
        11. delete volume (should fail)
        12. Stop volume
        13. Clean up all volumes
        """

        # pylint: disable=too-many-statements
        # Create a distributed volume on Node1
        number_of_brick = 1
        servers_info_from_single_node = {}
        servers_info_from_single_node[
            self.servers[0]] = self.all_servers_info[self.servers[0]]
        self.volname = "testvol"
        bricks_list = form_bricks_list(self.servers[0], self.volname,
                                       number_of_brick, self.servers[0],
                                       servers_info_from_single_node)
        ret, _, _ = volume_create(self.servers[0], self.volname,
                                  bricks_list, True)
        self.assertEqual(ret, 0, "Volume create failed")
        g.log.info("Volume %s created successfully", self.volname)

        # Create a replicate volume on Node2 without force
        number_of_brick = 2
        servers_info_from_single_node = {}
        servers_info_from_single_node[
            self.servers[1]] = self.all_servers_info[self.servers[1]]
        kwargs = {'replica_count': 2}
        self.volname = "new-volume"
        bricks_list = form_bricks_list(self.servers[1], self.volname,
                                       number_of_brick, self.servers[1],
                                       servers_info_from_single_node)

        # creation of replicate volume without force should fail
        ret, _, _ = volume_create(self.servers[1], self.volname,
                                  bricks_list, False, **kwargs)
        self.assertNotEqual(ret, 0, ("Unexpected: Successfully created "
                                     "the replicate volume on node2 "
                                     "without force"))
        g.log.info("Failed to create the replicate volume %s as "
                   " expected without force", self.volname)

        # Create a replica volume on Node2 with force
        number_of_brick = 3
        servers_info_from_single_node = {}
        servers_info_from_single_node[
            self.servers[1]] = self.all_servers_info[self.servers[1]]
        kwargs = {'replica_count': 3}
        self.volname = "new-volume"
        bricks_list = form_bricks_list(self.servers[1], self.volname,
                                       number_of_brick, self.servers[1],
                                       servers_info_from_single_node)

        # creation of replicate volume with force should succeed
        ret, _, _ = volume_create(self.servers[1], self.volname,
                                  bricks_list, True, **kwargs)
        self.assertEqual(ret, 0, "Volume create failed")
        g.log.info("Volume %s created", self.volname)

        # Perform peer probe from N1 to N2
        ret, _, _ = peer_probe(self.servers[0], self.servers[1])
        self.assertNotEqual(ret, 0, (
            "peer probe is success from %s to %s even if %s "
            " is a part of another cluster or having volumes "
            " configured", self.servers[0], self.servers[1], self.servers[1]))
        g.log.info("peer probe failed from %s to "
                   "%s as expected", self.servers[0], self.servers[1])

        # clean up all volumes
        for server in self.servers[0:2]:
            # Listing all the volumes
            vol_list = get_volume_list(server)
            self.assertIsNotNone(vol_list, "Unable to get volumes list")
            g.log.info("Getting the volume list from %s", self.mnode)
            for vol in vol_list:
                g.log.info("deleting volume : %s", vol)
                ret = cleanup_volume(server, vol)
                self.assertTrue(ret, ("Failed to Cleanup the Volume %s", vol))
                g.log.info("Volume deleted successfully : %s", vol)

        # Perform peer probe from N1 to N2 should success
        ret, _, _ = peer_probe(self.servers[0], self.servers[1])
        self.assertEqual(
            ret, 0, ("peer probe from %s to %s is "
                     "failed", self.servers[0], self.servers[1]))
        g.log.info("peer probe is success from %s to "
                   "%s", self.servers[0], self.servers[1])

        # Checking if peer is connected
        counter = 0
        while counter < 30:
            ret = is_peer_connected(self.servers[0], self.servers[1])
            counter += 1
            if ret:
                break
            sleep(3)
        self.assertTrue(ret, "Peer is not in connected state.")
        g.log.info("Peers is in connected state.")

        # Perform peer probe from N3 to N2 should fail
        ret, _, _ = peer_probe(self.servers[2], self.servers[1])
        self.assertNotEqual(ret, 0, (
            "peer probe is success from %s to %s even if %s "
            "is a part of another cluster or having volumes "
            "configured", self.servers[2], self.servers[1], self.servers[1]))
        g.log.info("peer probe failed from %s to "
                   "%s as expected", self.servers[2], self.servers[1])

        # Create a replica volume on N1 and N2 with force
        number_of_brick = 2
        servers_info_from_two_node = {}
        for server in self.servers[0:2]:
            servers_info_from_two_node[server] = self.all_servers_info[server]
        kwargs = {'replica_count': 2}
        self.volname = "new-volume"
        bricks_list = form_bricks_list(self.servers[0], self.volname,
                                       number_of_brick, self.servers[0:2],
                                       servers_info_from_two_node)
        ret, _, _ = volume_create(self.servers[1], self.volname,
                                  bricks_list, True, **kwargs)
        self.assertEqual(ret, 0, "Volume create failed")
        g.log.info("Volume %s created succssfully", self.volname)

        # Perform peer probe from N3 to N1 should fail
        ret, _, _ = peer_probe(self.servers[2], self.servers[0])
        self.assertNotEqual(ret, 0, (
            "peer probe is success from %s to %s even if %s "
            "a part of another cluster or having volumes "
            "configured", self.servers[2], self.servers[0], self.servers[0]))
        g.log.info("peer probe is failed from %s to "
                   "%s as expected", self.servers[2], self.servers[0])

        # Perform peer probe from N1 to N3 should succed
        ret, _, _ = peer_probe(self.servers[0], self.servers[2])
        self.assertEqual(
            ret, 0, ("peer probe from %s to %s is "
                     "failed", self.servers[0], self.servers[2]))
        g.log.info("peer probe is success from %s to "
                   "%s", self.servers[0], self.servers[2])

        # Checking if peer is connected
        counter = 0
        while counter < 30:
            ret = is_peer_connected(self.servers[0], self.servers[:3])
            counter += 1
            if ret:
                break
            sleep(3)
        self.assertTrue(ret, "Peer is not in connected state.")
        g.log.info("Peers is in connected state.")

        # Create a replica volume on N1, N2 and N3 with force
        number_of_brick = 3
        server_info_from_three_node = {}
        for server in self.servers[0:3]:
            server_info_from_three_node[server] = self.all_servers_info[server]
        kwargs = {'replica_count': 3}
        self.volname = "new-replica-volume"
        bricks_list = form_bricks_list(self.servers[2], self.volname,
                                       number_of_brick, self.servers[0:3],
                                       server_info_from_three_node)
        ret, _, _ = volume_create(self.servers[1], self.volname,
                                  bricks_list, True, **kwargs)
        self.assertEqual(ret, 0, "Volume create failed")
        g.log.info("creation of replica volume should succeed")

        ret, _, _ = volume_start(self.servers[2], self.volname, True)
        self.assertEqual(ret, 0, ("Failed to start the "
                                  "volume %s", self.volname))
        g.log.info("Volume %s start with force is success", self.volname)

        # Volume delete should fail without stopping volume
        self.assertTrue(
            volume_delete(self.servers[2], self.volname, xfail=True),
            "Unexpected Error: Volume deleted "
            "successfully without stopping volume"
        )
        g.log.info("Expected: volume delete should fail without "
                   "stopping volume: %s", self.volname)

        # Volume stop with force
        ret, _, _ = volume_stop(self.mnode, self.volname, True)
        self.assertEqual(ret, 0, ("Failed to stop the volume "
                                  "%s", self.volname))
        g.log.info("Volume stop with force is success")
