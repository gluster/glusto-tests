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

""" Description:
        Test Cases in this module tests the self heal daemon process.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import wait_for_volume_process_to_be_online
from glustolibs.gluster.heal_libs import (get_self_heal_daemon_pid,
                                          is_shd_daemonized)
from glustolibs.gluster.volume_ops import (volume_stop, volume_start)
from glustolibs.gluster.volume_libs import setup_volume, cleanup_volume
from glustolibs.gluster.peer_ops import (
    peer_probe_servers, peer_detach_servers, peer_detach, nodes_from_pool_list,
    peer_probe)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class SelfHealDaemonProcessTests(GlusterBaseClass):
    """
    SelfHealDaemonProcessTests contains tests which verifies the
    self-heal daemon process of the nodes
    """
    def setUp(self):
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        self.extra_servers = self.servers[-2:]
        self.servers = self.servers[:-2]
        # Performing peer detach
        for server in self.extra_servers:
            # Peer detach
            ret, _, _ = peer_detach(self.mnode, server)
            if ret:
                raise ExecutionError("Peer detach failed")
            g.log.info("Peer detach successful.")

        # Create volume using first four nodes
        servers_info_from_four_nodes = {}
        for server in self.servers:
            servers_info_from_four_nodes[
                server] = self.all_servers_info[server]

        self.volume['servers'] = self.servers
        ret = setup_volume(self.mnode, servers_info_from_four_nodes,
                           self.volume, force=False)
        if not ret:
            raise ExecutionError("Volume create failed on four nodes")
        g.log.info("Distributed replicated volume created successfully")

        # Verfiy glustershd process releases its parent process
        ret = is_shd_daemonized(self.servers)
        if not ret:
            raise ExecutionError("Self Heal Daemon process was still"
                                 " holding parent process.")
        g.log.info("Self Heal Daemon processes are online")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup volume
        g.log.info("Starting to Cleanup Volume")
        ret = cleanup_volume(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Failed to cleanup Volume")
        g.log.info("Successful in Cleanup")

        # Peer probe detached servers
        pool = nodes_from_pool_list(self.mnode)
        for node in self.extra_servers:
            if node not in pool:
                ret = peer_probe(self.mnode, node)
                if not ret:
                    raise ExecutionError("Failed to probe detached server %s"
                                         % node)
        g.log.info("Peer probe success for detached servers %s", self.servers)

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    def test_glustershd_on_newly_probed_server(self):
        """
        Test script to verify glustershd process on newly probed server

        * check glustershd process - only 1 glustershd process should
          be running
        * Add new node to cluster
        * check glustershd process - only 1 glustershd process should
          be running on all servers inclusing newly probed server
        * stop the volume
        * add another node to cluster
        * check glustershd process - glustershd process shouldn't be running
          on servers including newly probed server
        * start the volume
        * check glustershd process - only 1 glustershd process should
          be running on all servers inclusing newly probed server

        """
        # pylint: disable=too-many-statements

        nodes = self.volume['servers'][:-2]

        # check the self-heal daemon process
        g.log.info("Starting to get self heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either no self heal daemon process found or "
                              "more than one self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in getting single self heal daemon process"
                   " on all nodes %s", nodes)

        # Add new node to the cluster
        g.log.info("Peer probe for %s", self.extra_servers[0])
        ret = peer_probe_servers(self.mnode, self.extra_servers[0])
        self.assertTrue(ret, "Failed to peer probe server : %s"
                        % self.extra_servers[0])
        g.log.info("Peer probe success for %s and all peers are in "
                   "connected state", self.extra_servers[0])
        nodes.append(self.extra_servers[0])

        # check the self-heal daemon process and it should be running on
        # newly probed servers
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either no self heal daemon process found or "
                              "more than one self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in getting single self heal daemon process"
                   " on all nodes %s", nodes)

        # stop the volume
        g.log.info("Stopping the volume %s", self.volname)
        ret = volume_stop(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to stop volume %s" % self.volname))
        g.log.info("Successfully stopped volume %s", self.volname)

        # Add another new node to the cluster
        g.log.info("peer probe for %s", self.extra_servers[1])
        ret = peer_probe_servers(self.mnode, self.extra_servers[1])
        self.assertTrue(ret, "Failed to peer probe server : %s"
                        % self.extra_servers[1])
        g.log.info("Peer probe success for %s and all peers are in "
                   "connected state", self.extra_servers[1])
        nodes.append(self.extra_servers[1])

        # check the self-heal daemon process after stopping volume and
        # no self heal daemon should be running including newly probed node
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertFalse(ret, ("Self Heal Daemon process is running even "
                               "after stopping volume %s" % self.volname))
        for node in pids:
            self.assertEquals(pids[node][0], -1, ("Self Heal Daemon is still "
                                                  "running on node %s even "
                                                  "after stopping all "
                                                  "volumes" % node))
        g.log.info("Expected : No self heal daemon process is running "
                   "after stopping all volumes")

        # start the volume
        g.log.info("Starting volume %s", self.volname)
        ret = volume_start(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to start volume  %s" % self.volname))
        g.log.info("Volume %s started successfully", self.volname)

        # Verify volume's all process are online for 60 sec
        g.log.info("Verifying volume's all process are online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname,
                                                   60)
        self.assertTrue(ret, ("Volume %s : All process are not "
                              "online", self.volname))
        g.log.info("Successfully Verified volume %s processes are online",
                   self.volname)

        # Verfiy glustershd process releases its parent process
        g.log.info("verifying self heal daemon process is daemonized")
        ret = is_shd_daemonized(nodes)
        self.assertTrue(ret, ("Either no self heal daemon process found or "
                              "more than one self heal daemon process "
                              "found : %s" % pids))

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either no self heal daemon process found or "
                              "more than one self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in getting single self heal daemon process"
                   " on all nodes %s", nodes)

        # detach extra servers from the cluster
        g.log.info("peer detaching extra servers %s from cluster",
                   self.extra_servers)
        ret = peer_detach_servers(self.mnode, self.extra_servers)
        self.assertTrue(ret, "Failed to peer detach extra servers : %s"
                        % self.extra_servers)
        g.log.info("Peer detach success for %s ", self.extra_servers)
