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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import setup_volume
from glustolibs.gluster.volume_ops import get_volume_info
from glustolibs.gluster.peer_ops import (peer_probe_servers, peer_detach,
                                         peer_detach_servers,
                                         nodes_from_pool_list)


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestUUID(GlusterBaseClass):

    def setUp(self):

        GlusterBaseClass.setUp.im_func(self)

        # check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

        # detach all the nodes
        ret = peer_detach_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Peer detach failed to all the servers from "
                                 "the node %s." % self.mnode)
        g.log.info("Peer detach SUCCESSFUL.")

    def tearDown(self):

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup the Volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        pool = nodes_from_pool_list(self.mnode)
        for node in pool:
            peer_detach(self.mnode, node)

        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to probe detached "
                                 "servers %s" % self.servers)
        GlusterBaseClass.tearDown.im_func(self)

    def test_uuid_in_volume_info_xml(self):

        # create a two node cluster
        ret = peer_probe_servers(self.servers[0], self.servers[1])
        self.assertTrue(ret, "Peer probe failed to %s from %s"
                        % (self.mnode, self.servers[1]))

        # create a 2x2 volume
        servers_info_from_two_node_cluster = {}
        for server in self.servers[0:2]:
            servers_info_from_two_node_cluster[
                server] = self.all_servers_info[server]

        self.volume['servers'] = self.servers[0:2]
        self.volume['voltype']['replica_count'] = 2
        self.volume['voltype']['dist_count'] = 2
        ret = setup_volume(self.mnode, servers_info_from_two_node_cluster,
                           self.volume)
        self.assertTrue(ret, ("Failed to create"
                              "and start volume %s" % self.volname))

        # probe a new node from cluster
        ret = peer_probe_servers(self.mnode, self.servers[2])
        self.assertTrue(ret, "Peer probe failed to %s from %s"
                        % (self.mnode, self.servers[2]))

        # check gluster vol info --xml from newly probed node
        xml_output = get_volume_info(self.servers[2], self.volname)
        self.assertIsNotNone(xml_output, ("Failed to get volume info --xml for"
                                          "volume %s from newly probed node %s"
                                          % (self.volname, self.servers[2])))

        # volume info --xml should have non zero UUID for host and brick
        uuid_with_zeros = '00000000-0000-0000-0000-000000000000'
        len_of_uuid = len(uuid_with_zeros)
        number_of_bricks = int(xml_output[self.volname]['brickCount'])
        for i in range(number_of_bricks):
            uuid = xml_output[self.volname]['bricks']['brick'][i]['hostUuid']
            self.assertEqual(len(uuid), len_of_uuid, "Invalid uuid length")
            self.assertNotEqual(uuid, uuid_with_zeros, ("Invalid uuid %s"
                                                        % uuid))
