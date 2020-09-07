""" This Module demostrates how to use functions available in peer_ops module
"""

import random
import re
import socket

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (
    GlusterBaseClass,
    runs_on,
)
from glustolibs.gluster.peer_ops import (
    get_peer_status,
    get_pool_list,
    is_peer_connected,
    nodes_from_pool_list,
    peer_detach,
    peer_detach_servers,
    peer_probe,
    peer_probe_servers,
    peer_status,
    pool_list,
)


@runs_on([['distributed-replicated', 'replicated'],
          ['glusterfs', 'nfs']])
class DemoPeerOpsClass(GlusterBaseClass):
    """Demonstrating all the functions available in peer_ops module
    """
    @classmethod
    def setUpClass(cls):
        # Read all the cluster config from the g.config and assign it to
        # class variables
        cls.get_super_method(cls, 'setUpClass')()

        # Detach all the servers if it's already attached to the cluster
        nodes_in_pool_list = nodes_from_pool_list(cls.mnode)
        if nodes_in_pool_list is None:
            g.log.error("Unable to get nodes from gluster pool list "
                        "from node %s", cls.mnode)
        else:
            g.log.info("Nodes in pool: %s", nodes_in_pool_list)

        if nodes_in_pool_list:
            if cls.mnode in nodes_in_pool_list:
                nodes_in_pool_list.remove(cls.mnode)
            g.log.info("Detaching servers '%s' from the cluster from node %s",
                       nodes_in_pool_list, cls.mnode)
            ret = peer_detach_servers(cls.mnode, nodes_in_pool_list)
            if not ret:
                raise ExecutionError("Failed to detach some or all "
                                     "servers %s from the cluster "
                                     "from node %s", nodes_in_pool_list,
                                     cls.mnode)
            g.log.info("Successfully detached all servers '%s' "
                       "from the cluster from node %s",
                       nodes_in_pool_list, cls.mnode)

        # Get pool list from mnode
        g.log.info("Pool list on node %s", cls.mnode)
        ret, out, err = pool_list(cls.mnode)
        if ret != 0:
            raise ExecutionError("Failed to get pool list on node %s: %s",
                                 cls.mnode, err)
        g.log.info("Successfully got pool list on node %s:\n%s", cls.mnode,
                   out)

        # Get peer status output from all servers
        for server in cls.servers:
            g.log.info("Peer status on node %s", server)
            ret, out, err = peer_status(server)
            if ret != 0:
                raise ExecutionError("Failed to get peer status on node %s: "
                                     "%s", server, err)
            g.log.info("Successfully got peer status on node %s:\n%s",
                       server, out)

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # Peer probe servers
        g.log.info("Peer Probe servers '%s'", self.servers)
        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to peer probe some or all servers %s "
                                 "into the cluster", self.servers)
        g.log.info("Successfully peer probed all servers '%s' to the cluster",
                   self.servers)

        # Validate if peers are connected from each server
        g.log.info("Validating if servers %s are connected from other servers "
                   "in the cluster", self.servers)
        for server in self.servers:
            ret = is_peer_connected(server, self.servers)
            if not ret:
                raise ExecutionError("Some or all servers %s are not "
                                     "in connected state from node %s",
                                     self.servers, self.mnode)
            g.log.info("Successfully validated servers %s are all "
                       "in connected state from node %s",
                       self.servers, self.mnode)
        g.log.info("Successfully validated all servers %s are in connected "
                   "state from other servers in the cluster", self.servers)

    def test_pool_list(self):
        """Testing pool list command
        """
        # peer status from mnode
        g.log.info("Get Pool List from node %s", self.mnode)
        ret, out, err = pool_list(self.mnode)
        self.assertEqual(ret, 0, ("Failed to get pool list from node "
                                  "%s: %s", self.mnode, err))
        g.log.info("Successfully got pool list from node %s:\n%s",
                   self.mnode, out)

        # Get pool list randomly from some node
        random_server = random.choice(self.servers)
        g.log.info("Get Pool List from node %s", random_server)
        ret, out, err = pool_list(random_server)
        self.assertEqual(ret, 0, ("Failed to get pool list from node "
                                  "%s: %s", random_server, err))
        g.log.info("Successfully got pool list from node %s:\n%s",
                   random_server, out)

        # Get pool list from all the servers
        for server in self.servers:
            g.log.info("Get Pool List from node %s", server)
            ret, out, err = pool_list(server)
            self.assertEqual(ret, 0, ("Failed to get pool list from node "
                                      "%s: %s", server, err))
            g.log.info("Successfully got pool list from node %s:\n%s",
                       server, out)

    def test_peer_status(self):
        """Testing peer status command
        """
        # peer status from mnode
        g.log.info("Get peer status from node %s", self.mnode)
        ret, out, err = peer_status(self.mnode)
        self.assertEqual(ret, 0, ("Failed to get peer status from node "
                                  "%s: %s", self.mnode, err))
        g.log.info("Successfully got peer status from node %s:\n%s",
                   self.mnode, out)

        # Get peer status randomly from some node
        random_server = random.choice(self.servers)
        g.log.info("Get peer status from node %s", random_server)
        ret, out, err = pool_list(random_server)
        self.assertEqual(ret, 0, ("Failed to get peer status from node "
                                  "%s: %s", random_server, err))
        g.log.info("Successfully got peer status from node %s:\n%s",
                   random_server, out)

        # Get peer status output from all servers
        for server in self.servers:
            g.log.info("Peer status on node %s", server)
            ret, out, err = peer_status(server)
            self.assertEqual(ret, 0, ("Failed to get peer status from node "
                                      "%s: %s", server, err))
            g.log.info("Successfully got peer status from node %s:\n%s",
                       server, out)

    def test_is_peer_connected(self):
        """Check if peer is connected with is_peer_connected function
        """
        # Executing if all the peers are in connected state from mnode
        # This will validate all nodes in self.servers are in 'Connected'
        # State from self.mnode
        g.log.info("Validating servers %s are in connected state from node %s",
                   self.servers, self.mnode)
        ret = is_peer_connected(self.mnode, self.servers)
        self.assertTrue(ret, ("Some or all servers %s are not in connected "
                              "state from node %s", self.servers, self.mnode))
        g.log.info("Successfully validated servers %s are all in connected "
                   "state from node %s", self.servers, self.mnode)

        # Validate if peers are connected from each server
        g.log.info("Validating if servers %s are connected from other servers "
                   "in the cluster", self.servers)
        for server in self.servers:
            ret = is_peer_connected(server, self.servers)
            self.assertTrue(ret, ("Some or all servers %s are not "
                                  "in connected state from node %s",
                                  self.servers, self.mnode))
            g.log.info("Successfully validated servers %s are all "
                       "in connected state from node %s",
                       self.servers, self.mnode)
        g.log.info("Successfully validated all servers %s are in connected "
                   "state from other servers in the cluster", self.servers)

    def test_nodes_from_pool_list(self):
        """Testing nodes from pool list and peer probe by hostname or IP
        """
        # Get list of nodes from 'gluster pool list'
        nodes_in_pool_list = nodes_from_pool_list(self.mnode)
        if nodes_in_pool_list is None:
            g.log.error("Unable to get nodes from gluster pool list "
                        "from node %s", self.mnode)
        else:
            g.log.info("Nodes in pool: %s", nodes_in_pool_list)

        # Peer probe by hostname if node in nodes_in_pool_list is IP or
        # Peer probe by IP if node in nodes_in_pool_list is hostname
        for node in nodes_in_pool_list:
            if socket.gethostbyname(node) == node:
                node = socket.gethostbyaddr(node)[0]
            else:
                node = socket.gethostbyname(node)
            if node:
                g.log.info("Peer probe node %s from %s", node, self.mnode)
                ret, out, err = peer_probe(self.mnode, node)
                self.assertFalse((ret != 0 or
                                  re.search(r'^peer\sprobe\:\ssuccess(.*)',
                                            out) is None),
                                 ("Failed to peer probe %s from node %s",
                                  node, self.mnode))
                g.log.info("Successfully peer probed %s from node %s",
                           node, self.mnode)

    def test_get_pool_list(self):
        # Get pool list
        """ Example output of pool list

        [{'uuid': 'a2b88b10-eba2-4f97-add2-8dc37df08b27',
        'hostname': 'abc.lab.eng.xyz.com',
        'state': '3',
        'connected': '1',
        'stateStr': 'Peer in Cluster'},

        {'uuid': 'b15b8337-9f8e-4ec3-8bdb-200d6a67ae12',
        'hostname': 'def.lab.eng.xyz.com',
        'state': '3',
        'hostnames': ['def.lab.eng.xyz.com'],
        'connected': '1',
        'stateStr': 'Peer in Cluster'}
        ]
        """
        g.log.info("Get pool list --xml output as python dict from node %s",
                   self.mnode)
        pool_list_data = get_pool_list(self.mnode)
        self.assertIsNotNone(pool_list_data, ("Failed to get pool list --xml "
                                              "output as python dict on "
                                              "node %s", self.mnode))
        g.log.info("Successful in getting Pool list --xml output from node "
                   "%s as python dict:\n %s", self.mnode, pool_list_data)

        # Log connected state of the peer
        for item in pool_list_data:
            node = item['hostname']
            if node == self.mnode:
                continue
            connected_status = item['connected']
            state_str = item['stateStr']
            g.log.info("Node %s status: \n%s", node,
                       ("Connected: %s\nStateStr:%s\n" %
                        (connected_status, state_str)
                        ))

    def test_get_peer_status(self):
        # Get peer status
        """ Example output of peer status

        [{'uuid': '77dc299a-32f7-43d8-9977-7345a344c398',
        'hostname': 'ijk.lab.eng.xyz.com',
        'state': '3',
        'hostnames' : ['ijk.lab.eng.xyz.com'],
        'connected': '1',
        'stateStr': 'Peer in Cluster'},

        {'uuid': 'b15b8337-9f8e-4ec3-8bdb-200d6a67ae12',
        'hostname': 'def.lab.eng.xyz.com',
        'state': '3',
        'hostnames': ['def.lab.eng.xyz.com'],
        'connected': '1',
        'stateStr': 'Peer in Cluster'}
        ]
        """
        g.log.info("Get peer status --xml output as python dict from node %s",
                   self.mnode)
        peer_status_list = get_peer_status(self.mnode)
        self.assertIsNotNone(peer_status_list,
                             ("Failed to get peer status --xml "
                              "output as python dict from "
                              "node %s", self.mnode))
        g.log.info("Successful in getting Peer status --xml output from "
                   "node %s as python dict:\n %s", self.mnode,
                   peer_status_list)

        # Validating UUID of the peer with get_peer_status
        server_ips = []
        for server in self.servers:
            server_ips.append(socket.gethostbyname(server))

        for peer_stat in peer_status_list:
            if socket.gethostbyname(peer_stat['hostname']) in server_ips:
                self.assertIsNotNone(
                    re.match(r'([0-9a-f]{8})(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',
                             peer_stat['uuid'], re.I),
                    ("Invalid UUID for the node '%s'", peer_stat['hostname']))
                g.log.info("Valid UUID '%s' for the node %s",
                           peer_stat['uuid'], peer_stat['hostname'])

    def tearDown(self):
        """peer teardown
        """
        # Detach all the servers if it's already attached to the cluster
        nodes_in_pool_list = nodes_from_pool_list(self.mnode)
        if nodes_in_pool_list is None:
            g.log.error("Unable to get nodes from gluster pool list "
                        "from node %s", self.mnode)
        else:
            g.log.info("Nodes in pool: %s", nodes_in_pool_list)

        if nodes_in_pool_list:
            if self.mnode in nodes_in_pool_list:
                nodes_in_pool_list.remove(self.mnode)
            g.log.info("Detaching servers %s from node %s",
                       nodes_in_pool_list, self.mnode)
            for server in nodes_in_pool_list:
                ret, out, err = peer_detach(self.mnode, server)
                self.assertFalse(
                    (ret != 0 or
                     re.search(r'^peer\sdetach\:\ssuccess(.*)', out) is None),
                    ("Failed to detach server %s from node %s: %s", server,
                     self.mnode, err))
                g.log.info("Successfully detached server %s from node %s: %s",
                           server, self.mnode, out)
            g.log.info("Successfully detached servers %s from node %s",
                       nodes_in_pool_list, self.mnode)

        self.get_super_method(self, 'tearDown')()

    @classmethod
    def tearDownClass(cls):
        """Define it when you need to execute something else than super's
           tearDownClass method.
        """
        cls.get_super_method(cls, 'tearDownClass')()
