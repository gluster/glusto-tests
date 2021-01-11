#  Copyright (C) 2020-2021 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.peer_ops import peer_probe
from glustolibs.gluster.lib_utils import is_core_file_created
from glustolibs.gluster.peer_ops import peer_detach, is_peer_connected
from glustolibs.gluster.gluster_init import stop_glusterd, start_glusterd
from glustolibs.misc.misc_libs import bring_down_network_interface


class PeerProbeWhenGlusterdDown(GlusterBaseClass):

    def test_peer_probe_when_glusterd_down(self):
        # pylint: disable=too-many-statements
        '''
        Test script to verify the behavior when we try to peer
        probe a valid node whose glusterd is down
        Also post validate to make sure no core files are created
        under "/", /var/log/core and /tmp  directory

        Ref: BZ#1257394 Provide meaningful error on peer probe and peer detach
        Test Steps:
        1 check the current peer status
        2 detach one of the valid nodes which is already part of cluster
        3 stop glusterd on that node
        4 try to attach above node to cluster, which must fail with
          Transport End point error
        5 Recheck the test using hostname, expected to see same result
        6 start glusterd on that node
        7 halt/reboot the node
        8 try to peer probe the halted node, which must fail again.
        9 The only error accepted is
          "peer probe: failed: Probe returned with Transport endpoint is not
          connected"
        10 Check peer status and make sure no other nodes in peer reject state
        '''

        ret, test_timestamp, _ = g.run_local('date +%s')
        test_timestamp = test_timestamp.strip()

        # Detach one of the nodes which is part of the cluster
        g.log.info("detaching server %s ", self.servers[1])
        ret, _, err = peer_detach(self.mnode, self.servers[1])
        msg = 'peer detach: failed: %s is not part of cluster\n' \
              % self.servers[1]
        if ret:
            self.assertEqual(err, msg, "Failed to detach %s "
                             % (self.servers[1]))

        # Bring down glusterd of the server which has been detached
        g.log.info("Stopping glusterd on %s ", self.servers[1])
        ret = stop_glusterd(self.servers[1])
        self.assertTrue(ret, "Fail to stop glusterd on %s " % self.servers[1])

        # Trying to peer probe the node whose glusterd was stopped using IP
        g.log.info("Peer probing %s when glusterd down ", self.servers[1])
        ret, _, err = peer_probe(self.mnode, self.servers[1])
        self.assertNotEqual(ret, 0, "Peer probe should not pass when "
                                    "glusterd is down")
        self.assertEqual(err, "peer probe: failed: Probe returned with "
                              "Transport endpoint is not connected\n")

        # Trying to peer probe the same node with hostname
        g.log.info("Peer probing node %s using hostname with glusterd down ",
                   self.servers[1])
        hostname = g.run(self.servers[1], "hostname")
        ret, _, err = peer_probe(self.mnode, hostname[1].strip())
        self.assertNotEqual(ret, 0, "Peer probe should not pass when "
                                    "glusterd is down")
        self.assertEqual(err, "peer probe: failed: Probe returned with"
                              " Transport endpoint is not connected\n")

        # Start glusterd again for the next set of test steps
        g.log.info("starting glusterd on %s ", self.servers[1])
        ret = start_glusterd(self.servers[1])
        self.assertTrue(ret, "glusterd couldn't start successfully on %s"
                        % self.servers[1])

        # Bring down the network for sometime
        network_status = bring_down_network_interface(self.servers[1], 150)

        # Peer probing the node using IP when it is still not online
        g.log.info("Peer probing node %s when network is down",
                   self.servers[1])
        ret, _, err = peer_probe(self.mnode, self.servers[1])
        self.assertNotEqual(ret, 0, "Peer probe passed when it was expected to"
                                    " fail")
        self.assertEqual(err.split("\n")[0], "peer probe: failed: Probe "
                                             "returned with Transport endpoint"
                                             " is not connected")

        # Peer probing the node using hostname when it is still not online
        g.log.info("Peer probing node %s using hostname which is still "
                   "not online ",
                   self.servers[1])
        ret, _, err = peer_probe(self.mnode, hostname[1].strip())
        self.assertNotEqual(ret, 0, "Peer probe should not pass when node "
                                    "has not come online")
        self.assertEqual(err.split("\n")[0], "peer probe: failed: Probe "
                                             "returned with Transport endpoint"
                                             " is not connected")

        ret, _, _ = network_status.async_communicate()
        if ret != 0:
            g.log.error("Failed to perform network interface ops")

        # Peer probe the node must pass
        g.log.info("peer probing node %s", self.servers[1])
        ret, _, err = peer_probe(self.mnode, self.servers[1])
        self.assertEqual(ret, 0, "Peer probe has failed unexpectedly with "
                                 "%s " % err)

        # Checking if core file created in "/", "/tmp" and "/var/log/core"
        ret = is_core_file_created(self.servers, test_timestamp)
        self.assertTrue(ret, "core file found")

    def tearDown(self):
        g.log.info("Peering any nodes which are not part of cluster as "
                   "part of cleanup")
        for server in self.servers:
            if not is_peer_connected(self.mnode, server):
                ret, _, err = peer_probe(self.mnode, server)
                if ret:
                    raise ExecutionError("Peer probe failed with %s " % err)
