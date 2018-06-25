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

import socket
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import runs_on, GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import setup_volume, cleanup_volume
from glustolibs.gluster.volume_ops import get_volume_info, get_volume_list
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.peer_ops import (peer_probe, peer_status, peer_detach,
                                         peer_probe_servers,
                                         peer_detach_servers,
                                         nodes_from_pool_list)


@runs_on([['distributed'], ['glusterfs']])
class TestPeerStatus(GlusterBaseClass):

    def setUp(self):

        GlusterBaseClass.setUp.im_func(self)

        # Performing peer detach
        ret = peer_detach_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to detach servers %s"
                                 % self.servers)
        g.log.info("Peer detach SUCCESSFUL.")

    def tearDown(self):

        # Clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is not None:
            for volume in vol_list:
                ret = cleanup_volume(self.mnode, volume)
                if not ret:
                    raise ExecutionError("Failed to cleanup volume")
                g.log.info("Volume deleted successfully : %s", volume)

        # detached servers from cluster
        pool = nodes_from_pool_list(self.mnode)
        for node in pool:
            peer_detach(self.mnode, node)

        # form a cluster
        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to probe peer "
                                 "servers %s" % self.servers)
        g.log.info("Peer probe success for detached "
                   "servers %s", self.servers)
        GlusterBaseClass.tearDown.im_func(self)

    def test_peer_probe_status(self):

        # get FQDN of node1 and node2
        node1 = socket.getfqdn(self.mnode)
        node2 = socket.getfqdn(self.servers[1])

        # peer probe to a new node, N2 from N1
        ret, _, err = peer_probe(node1, node2)
        self.assertEqual(ret, 0, ("Peer probe failed to %s from %s with "
                                  "error message %s" % (self.servers[1],
                                                        self.mnode, err)))
        g.log.info("Peer probe from %s to %s is success", self.mnode,
                   self.servers[1])

        # check peer status in both the nodes, it should have FQDN
        # from node1
        ret, out, err = peer_status(self.mnode)
        self.assertEqual(ret, 0, ("Failed to get peer status from %s with "
                                  "error message %s" % (self.mnode, err)))
        g.log.info("Successfully got peer status from %s", self.mnode)

        self.assertIn(node2, out, ("FQDN of %s is not present in the "
                                   "output of peer status from %s"
                                   % (self.servers[1], self.mnode)))
        g.log.info("FQDN of %s is present in peer status of %s",
                   self.servers[1], self.mnode)

        # from node2
        ret, out, err = peer_status(self.servers[1])
        self.assertEqual(ret, 0, ("Failed to get peer status from %s with "
                                  "error message %s" % (self.servers[1], err)))
        g.log.info("Successfully got peer status from %s", self.servers[1])

        self.assertIn(node1, out, ("FQDN of %s is not present in the "
                                   "output of peer status from %s"
                                   % (self.mnode, self.servers[1])))
        g.log.info("FQDN of %s is present in peer status of %s",
                   self.mnode, self.servers[1])

        # create a distributed volume with 2 bricks
        servers_info_from_two_node_cluster = {}
        for server in self.servers[0:2]:
            servers_info_from_two_node_cluster[
                server] = self.all_servers_info[server]

        self.volume['servers'] = self.servers[0:2]
        self.volume['voltype']['dist_count'] = 2
        ret = setup_volume(self.mnode, servers_info_from_two_node_cluster,
                           self.volume)
        self.assertTrue(ret, ("Failed to create "
                              "and start volume %s" % self.volname))
        g.log.info("Successfully created and started the volume %s",
                   self.volname)

        # peer probe to a new node, N3
        ret, _, err = peer_probe(self.mnode, self.servers[2])
        self.assertEqual(ret, 0, ("Peer probe failed to %s from %s with "
                                  "error message %s" % (self.servers[2],
                                                        self.mnode, err)))
        g.log.info("Peer probe from %s to %s is success", self.mnode,
                   self.servers[2])

        # add a brick from N3 to the volume
        num_bricks_to_add = 1
        server_info = {}
        server_info[self.servers[2]] = self.all_servers_info[self.servers[2]]
        brick = form_bricks_list(self.mnode, self.volname, num_bricks_to_add,
                                 self.servers[2], server_info)
        ret, _, _ = add_brick(self.mnode, self.volname, brick)
        self.assertEqual(ret, 0, ("Failed to add brick to volume %s"
                                  % self.volname))
        g.log.info("add brick to the volume %s is success", self.volname)

        # get volume info, it should have correct brick information
        ret = get_volume_info(self.mnode, self.volname)
        self.assertIsNotNone(ret, ("Failed to get volume info from %s"
                                   % self.mnode))
        g.log.info("volume info from %s is success", self.mnode)

        brick3 = ret[self.volname]['bricks']['brick'][2]['name']
        self.assertEqual(brick3, str(brick[0]), ("Volume info has incorrect "
                                                 "information"))
        g.log.info("Volume info has correct information")
