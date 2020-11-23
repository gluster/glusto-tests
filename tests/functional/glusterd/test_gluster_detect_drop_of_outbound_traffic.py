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
      Gluster should detect drop of outbound traffic as network failure
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.peer_ops import nodes_from_pool_list, get_peer_status
from glustolibs.gluster.volume_ops import volume_status


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'], ['glusterfs']])
class TestGlusterDetectDropOfOutboundTrafficAsNetworkFailure(GlusterBaseClass):
    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s"
                                 % self.volname)
        g.log.info("Volume created successfully : %s", self.volname)

    def tearDown(self):
        # Removing the status_err file and the iptable rule,if set previously
        if self.iptablerule_set:
            cmd = "iptables -D OUTPUT -p tcp -m tcp --dport 24007 -j DROP"
            ret, _, _ = g.run(self.servers[1], cmd)
            if ret:
                raise ExecutionError("Failed to remove the iptable rule"
                                     " for glusterd")

        # Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup the volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully: %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_gluster_detect_drop_of_out_traffic_as_network_failure(self):
        """
        Test Case:
        1) Create a volume and start it.
        2) Add an iptable rule to drop outbound glusterd traffic
        3) Check if the rule is added in iptables list
        4) Execute few Gluster CLI commands like volume status, peer status
        5) Gluster CLI commands should fail with suitable error message
        """
        # Set iptablerule_set as false initially
        self.iptablerule_set = False

        # Set iptable rule on one node to drop outbound glusterd traffic
        cmd = "iptables -I OUTPUT -p tcp --dport 24007 -j DROP"
        ret, _, _ = g.run(self.servers[1], cmd)
        self.assertEqual(ret, 0, "Failed to set iptable rule on the node: %s"
                         % self.servers[1])
        g.log.info("Successfully added the rule to iptable")

        # Update iptablerule_set to true
        self.iptablerule_set = True

        # Confirm if the iptable rule was added successfully
        iptable_rule = "'OUTPUT -p tcp -m tcp --dport 24007 -j DROP'"
        cmd = "iptables -S OUTPUT | grep %s" % iptable_rule
        ret, _, _ = g.run(self.servers[1], cmd)
        self.assertEqual(ret, 0, "Failed to get the rule from iptable")

        # Fetch number of nodes in the pool, except localhost
        pool_list = nodes_from_pool_list(self.mnode)
        peers_count = len(pool_list) - 1

        # Gluster CLI commands should fail
        # Check volume status command
        ret, _, err = volume_status(self.servers[1])
        self.assertEqual(ret, 2, "Unexpected: gluster volume status command"
                         " did not return any error")

        status_err_count = err.count("Staging failed on")
        self.assertEqual(status_err_count, peers_count, "Unexpected: No. of"
                         " nodes on which vol status cmd failed is not equal"
                         " to peers_count value")
        g.log.info("Volume status command failed with expected error message")

        # Check peer status command and all peers are in 'Disconnected' state
        peer_list = get_peer_status(self.servers[1])

        for peer in peer_list:
            self.assertEqual(int(peer["connected"]), 0, "Unexpected: All"
                             "  the peers are not in 'Disconnected' state")
            self.assertEqual(peer["stateStr"], "Peer in Cluster", "Unexpected:"
                             " All the peers not in 'Peer in Cluster' state")

        g.log.info("Peer status command listed all the peers in the"
                   "expected state")
