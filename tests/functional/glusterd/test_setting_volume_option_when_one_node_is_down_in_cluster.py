#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable= too-many-statements

import socket
import random
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import (
    set_volume_options, get_volume_info)
from glustolibs.gluster.gluster_init import (
    start_glusterd, wait_for_glusterd_to_start)
from glustolibs.gluster.volume_libs import setup_volume
from glustolibs.gluster.peer_ops import (peer_probe_servers,
                                         peer_detach_servers,
                                         nodes_from_pool_list)


@runs_on([['distributed-replicated'], ['glusterfs']])
class VolumeInfoSync(GlusterBaseClass):

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

        g.log.info("Peers are in connected state")

        # detach a node from cluster, assume last node
        last_node = self.servers[len(self.servers) - 1]
        ret = peer_detach_servers(self.mnode, last_node)
        if not ret:
            raise ExecutionError("Peer detach failed to the last node "
                                 "%s from %s" % (last_node, self.mnode))
        g.log.info("Peer detach SUCCESSFUL.")

    def tearDown(self):

        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to probe detached "
                                 "servers %s" % self.servers)

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup the Volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_sync_functinality(self):

        # create a 2x3 volume
        num_of_servers = len(self.servers)
        servers_info_from_cluster = {}
        for server in self.servers[0:num_of_servers-1]:
            servers_info_from_cluster[server] = self.all_servers_info[server]

        self.volume['servers'] = self.servers[0:num_of_servers-1]
        self.volume['voltype']['replica_count'] = 3
        self.volume['voltype']['dist_count'] = 2
        ret = setup_volume(self.mnode, servers_info_from_cluster, self.volume)
        self.assertTrue(ret, ("Failed to create "
                              "and start volume %s" % self.volname))
        g.log.info("Successfully created and started the volume %s",
                   self.volname)

        # stop glusterd on a random node of the cluster
        random_server_index = random.randint(1, num_of_servers - 2)
        random_server = self.servers[random_server_index]
        cmd = "systemctl stop glusterd"
        ret = g.run_async(random_server, cmd)
        g.log.info("Stopping glusterd on %s", random_server)

        # set a option on volume, stat-prefetch on
        self.options = {"stat-prefetch": "on"}
        ret = set_volume_options(self.mnode, self.volname, self.options)
        self.assertTrue(ret, ("Failed to set option stat-prefetch to on"
                              "for the volume %s" % self.volname))
        g.log.info("Succeeded in setting stat-prefetch option to on"
                   "for the volume %s", self.volname)

        # start glusterd on the node where glusterd is stopped
        ret = start_glusterd(random_server)
        self.assertTrue(ret, "Failed to start glusterd on %s" % random_server)

        ret = wait_for_glusterd_to_start(random_server)
        self.assertTrue(ret, "glusterd is not running on %s"
                        % random_server)
        g.log.info("glusterd is started and running on %s", random_server)

        # volume info should be synced across the cluster
        out1 = get_volume_info(self.mnode, self.volname)
        self.assertIsNotNone(out1, "Failed to get the volume info from %s"
                             % self.mnode)
        g.log.info("Getting volume info from %s is success", self.mnode)

        count = 0
        while count < 60:
            out2 = get_volume_info(random_server, self.volname)
            self.assertIsNotNone(out2, "Failed to get the volume info from %s"
                                 % random_server)
            if out1 == out2:
                break
            sleep(2)
            count += 1

        g.log.info("Getting volume info from %s is success", random_server)
        self.assertDictEqual(out1, out2, "volume info is not synced")

        # stop glusterd on a random server from cluster
        random_server_index = random.randint(1, num_of_servers - 2)
        random_server = self.servers[random_server_index]
        cmd = "systemctl stop glusterd"
        ret = g.run_async(random_server, cmd)
        g.log.info("Stopping glusterd on node %s", random_server)

        # peer probe a new node
        ret = peer_probe_servers(self.mnode, self.servers[num_of_servers-1])
        self.assertTrue(ret, "Failed to peer probe %s from %s"
                        % (self.servers[num_of_servers-1], self.mnode))
        g.log.info("Peer probe from %s to %s is success", self.mnode,
                   self.servers[num_of_servers-1])

        # start glusterd on the node where glusterd is stopped
        ret = start_glusterd(random_server)
        self.assertTrue(ret, "Failed to start glusterd on %s" % random_server)

        ret = wait_for_glusterd_to_start(random_server)
        self.assertTrue(ret, "glusterd is not running on %s"
                        % random_server)
        g.log.info("glusterd is started and running on %s", random_server)

        # peer status should be synced across the cluster
        list1 = nodes_from_pool_list(self.mnode)
        self.assertIsNotNone(list1, "Failed to get nodes list in the cluster"
                             "from %s" % self.mnode)
        g.log.info("Successfully got the nodes list in the cluster from %s",
                   self.mnode)

        # replacing ip with FQDN
        i = 0
        for node in list1:
            list1[i] = socket.getfqdn(node)
            i += 1
        list1 = sorted(list1)

        count = 0
        while count < 60:
            list2 = nodes_from_pool_list(random_server)
            self.assertIsNotNone(list2, "Failed to get nodes list in the "
                                 "cluster from %s" % random_server)
            # replacing ip with FQDN
            i = 0
            for node in list2:
                list2[i] = socket.getfqdn(node)
                i += 1

            list2 = sorted(list2)
            if list2 == list1:
                break
            sleep(2)
            count += 1

        g.log.info("Successfully got the nodes list in the cluster from %s",
                   random_server)

        self.assertListEqual(list1, list2, "Peer status is "
                             "not synced across the cluster")
        g.log.info("Peer status is synced across the cluster")
