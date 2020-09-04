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

from socket import gethostbyname, getfqdn
from random import choice
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         peer_probe_servers,
                                         peer_detach_servers,
                                         nodes_from_pool_list)
from glustolibs.gluster.exceptions import ExecutionError


# pylint: disable=unsubscriptable-object
class TestPeerProbeScenarios(GlusterBaseClass):

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # Performing peer detach
        if not peer_detach_servers(self.mnode, self.servers):
            raise ExecutionError("Failed to detach servers %s"
                                 % self.servers)
        g.log.info("Peer detach SUCCESSFUL.")
        self.peers_in_pool = []
        self.by_type = ""
        self.node = None

    def tearDown(self):
        """Detach servers from cluster"""
        pool = nodes_from_pool_list(self.mnode)
        self.assertIsNotNone(pool, "Failed to get pool list")
        for node in pool:
            if not peer_detach(self.mnode, node):
                raise ExecutionError("Failed to detach %s from %s"
                                     % (node, self.mnode))
        # Create a cluster
        if not peer_probe_servers(self.mnode, self.servers):
            raise ExecutionError("Failed to probe peer "
                                 "servers %s" % self.servers)
        g.log.info("Peer probe success for detached "
                   "servers %s", self.servers)

        self.get_super_method(self, 'tearDown')()

    def _get_node_identifiers(self):
        """ Returns node address dict with ip, fqdn, hostname as keys """
        node = {}
        node['ip'] = gethostbyname(self.node)
        node['fqdn'] = getfqdn(self.node)
        node['hostname'] = g.run(self.node, "hostname")[1].strip()
        return node

    def _perform_peer_probe(self, peer):
        """ Perfroms peer probe to a given node """
        ret, _, err = peer_probe(self.mnode, peer)
        self.assertEqual(ret, 0, "Failed to peer probe %s from %s. Error : %s"
                         % (peer, self.mnode, err))

    def _get_new_nodes_to_peer_probe(self):
        """ Selects a node randomly from the existing set of nodes """
        self.node = None
        while self.node is None:
            self.node = (gethostbyname(choice(self.servers[1:]))
                         if gethostbyname(choice(self.servers)) not in
                         self.peers_in_pool else None)
            self.peers_in_pool.append(self.node)

        return self._get_node_identifiers()

    def _verify_pool_list(self, node):
        """ Verifies given nodes are there in the gluster pool list"""
        pool_list = nodes_from_pool_list(self.mnode)
        status = next((n for n in pool_list if n in node.values()), None)
        self.assertIsNotNone(status, ("Node %s is not the pool list :"
                                      " %s" %
                                      (node[self.by_type], pool_list)))
        g.log.info("The given node is there in the gluster pool list")

    def _verify_cmd_history(self, node):
        """Verifies cmd_history for successful entry of peer probe of nodes"""

        # Extract the test specific cmds from cmd_hostory
        start_msg = "Starting Test : %s : %s" % (self.id(),
                                                 self.glustotest_run_id)
        end_msg = "Ending Test: %s : %s" % (self.id(), self.glustotest_run_id)
        cmd_history_log = "/var/log/glusterfs/cmd_history.log"
        cmd = "awk '/{}/ {{p=1}}; p; /{}/ {{p=0}}' {}".format(start_msg,
                                                              end_msg,
                                                              cmd_history_log)
        ret, test_specific_cmd_history, err = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to extract cmd_history specific to "
                                 "the current test case. Error : %s" % err)
        # Verify the cmd is found from the extracted cmd log
        peer_probe_cmd = "peer probe {} : SUCCESS".format(node)
        self.assertNotEqual(test_specific_cmd_history.count(peer_probe_cmd),
                            0, "Peer probe success entry not found"
                               " in cmd history")
        g.log.info("The command history contains a successful entry "
                   "of peer probe to %s ", node)

    def test_validate_peer_probe(self):
        """
        1. Add one of the node(HOST1-IP) to the other node(HOST2-IP) and
           form the cluster
           # gluster peer probe <HOST-IP>
        2. Check the return value of the 'peer probe' command
        3. Confirm that the cluster is formed successfully by 'peer status'
           command
           # gluster peer status
        4. Execute 'pool list' command to get the status of the cluster
           including the local node itself
           # gluster pool list
        5. Check the cmd_history' for the status message related to
          'peer probe' command
        6. Repeat 1-5 for FQDN and hostnames
        """

        for self.by_type in ('ip', 'fqdn', 'hostname'):
            # Get a node to peer probe to
            host_node = self._get_new_nodes_to_peer_probe()

            # Perform peer probe and verify the status
            self._perform_peer_probe(host_node[self.by_type])

            # Verify Peer pool list and check whether the node exists or not
            self._verify_pool_list(host_node)

            # Verify command history for successful peer probe status
            self._verify_cmd_history(host_node[self.by_type])

            g.log.info("Peer probe scenario validated using %s", self.by_type)
