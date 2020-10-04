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

from random import choice
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         peer_probe_servers,
                                         nodes_from_pool_list)
from glustolibs.gluster.lib_utils import is_core_file_created
from glustolibs.gluster.exceptions import ExecutionError


class TestPeerProbeWithFirewallNotOpened(GlusterBaseClass):

    def setUp(self):
        # Performing peer detach
        for server in self.servers[1:]:
            ret, _, _ = peer_detach(self.mnode, server)
            if ret:
                raise ExecutionError("Peer detach failed")
            g.log.info("Peer detach SUCCESSFUL.")
        self.get_super_method(self, 'setUp')()
        self.node_to_probe = choice(self.servers[1:])

    def tearDown(self):
        # Add the removed services in firewall
        for service in ('glusterfs', 'rpc-bind'):
            for option in ("", " --permanent"):
                cmd = ("firewall-cmd --zone=public --add-service={}{}"
                       .format(service, option))
                ret, _, _ = g.run(self.node_to_probe, cmd)
                if ret:
                    raise ExecutionError("Failed to add firewall service %s "
                                         "on %s" % (service,
                                                    self.node_to_probe))

        # Detach servers from cluster
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

    def _remove_firewall_service(self):
        """ Remove glusterfs and rpc-bind services from firewall"""
        for service in ['glusterfs', 'rpc-bind']:
            for option in ("", " --permanent"):
                cmd = ("firewall-cmd --zone=public --remove-service={}{}"
                       .format(service, option))
                ret, _, _ = g.run(self.node_to_probe, cmd)
                self.assertEqual(ret, 0, ("Failed to bring down service {} on"
                                          " node {}"
                                          .format(service,
                                                  self.node_to_probe)))
        g.log.info("Successfully removed glusterfs and rpc-bind services")

    def _get_test_specific_glusterd_log(self, node):
        """Gets the test specific glusterd log"""
        # Extract the test specific cmds from cmd_hostory
        start_msg = "Starting Test : %s : %s" % (self.id(),
                                                 self.glustotest_run_id)
        end_msg = "Ending Test: %s : %s" % (self.id(),
                                            self.glustotest_run_id)
        glusterd_log = "/var/log/glusterfs/glusterd.log"
        cmd = ("awk '/{}/ {{p=1}}; p; /{}/ {{p=0}}' {}"
               .format(start_msg, end_msg, glusterd_log))
        ret, test_specific_glusterd_log, err = g.run(node, cmd)
        self.assertEqual(ret, 0, "Failed to extract glusterd log specific"
                                 " to the current test case. "
                                 "Error : %s" % err)
        return test_specific_glusterd_log

    def test_verify_peer_probe_with_firewall_ports_not_opened(self):
        """
        Test Steps:
        1. Open glusterd port only in  Node1 using firewall-cmd command
        2. Perform peer probe to Node2 from Node 1
        3. Verify glusterd.log for Errors
        4. Check for core files created
        """

        ret, test_timestamp, _ = g.run_local('date +%s')
        test_timestamp = test_timestamp.strip()

        # Remove firewall service on the node to probe to
        self._remove_firewall_service()

        # Try peer probe from mnode to node
        ret, _, err = peer_probe(self.mnode, self.node_to_probe)
        self.assertEqual(ret, 1, ("Unexpected behavior: Peer probe should"
                                  " fail when the firewall services are "
                                  "down but returned success"))

        expected_err = ('peer probe: failed: Probe returned with '
                        'Transport endpoint is not connected\n')
        self.assertEqual(err, expected_err,
                         "Expected error {}, but returned {}"
                         .format(expected_err, err))
        msg = ("Peer probe of {} from {} failed as expected "
               .format(self.mnode, self.node_to_probe))
        g.log.info(msg)

        # Verify there are no glusterd crashes
        status = True
        glusterd_logs = (self._get_test_specific_glusterd_log(self.mnode)
                         .split("\n"))
        for line in glusterd_logs:
            if ' E ' in line:
                status = False
                g.log.info("Error found: ' %s '", line)

        self.assertTrue(status, "Glusterd crash found")

        # Verify no core files are created
        ret = is_core_file_created(self.servers, test_timestamp)
        self.assertTrue(ret, "Unexpected crash found.")
        g.log.info("No core file found as expected")
