#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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
      Test to check gluster operations after removing firewall
"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list)
from glustolibs.gluster.volume_libs import (cleanup_volume)
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         peer_probe_servers,
                                         nodes_from_pool_list,
                                         wait_for_peers_to_connect,
                                         is_peer_connected)
from glustolibs.gluster.lib_utils import (form_bricks_list,
                                          add_services_to_firewall,
                                          remove_service_from_firewall)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.mount_ops import (mount_volume, is_mounted,
                                          umount_volume)


class TestGlusterOperationsAfterRemovingFirewall(GlusterBaseClass):

    def setUp(self):

        # Adding services list here, so that it can be
        # used in teardown if setup fails
        self.services_lst = ['glusterfs', 'nfs', 'rpc-bind']

        # Performing peer detach
        for server in self.servers[1:]:
            ret, _, _ = peer_detach(self.mnode, server)
            if ret != 0:
                raise ExecutionError("Peer detach failed")
        self.get_super_method(self, 'setUp')()

    def tearDown(self):
        # Reset firewall services to the zone
        if not self.firewall_added:
            ret = self._add_firewall_services(self.servers[:2])
            if not ret:
                raise ExecutionError("Failed to add firewall services")

        # Reload firewall services
        ret = self._reload_firewall_service(self.servers[:2])
        if not ret:
            raise ExecutionError("Failed to reload firewall services")

        # Cleanup the volumes and unmount it, if mounted
        vol_list = get_volume_list(self.mnode)
        if vol_list is not None:
            for volume in vol_list:
                ret = is_mounted(volume, mpoint="/mnt/distribute-vol",
                                 mserver=self.mnode, mclient=self.servers[1],
                                 mtype="glusterfs")
                if ret:
                    ret, _, _ = umount_volume(mclient=self.servers[1],
                                              mpoint="/mnt/distribute-vol")
                    if ret:
                        raise ExecutionError("Failed to unmount volume")

                ret = cleanup_volume(self.mnode, volume)
                if not ret:
                    raise ExecutionError("Failed to cleanup volume")
                g.log.info("Volume cleaned up successfully : %s", volume)

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
        self.get_super_method(self, 'tearDown')()

    def _add_firewall_services(self, servers):
        """ Add services to firewall """
        ret = add_services_to_firewall(servers, self.services_lst, True)
        return ret

    def _remove_firewall_services(self, servers):
        """ Remove services from firewall """
        ret = remove_service_from_firewall(servers, self.services_lst, True)
        self.assertTrue(ret, "Failed to remove services from firewall")

    def _reload_firewall_service(self, nodes):
        """ Reload the firewall service on the nodes """
        cmd = "firewall-cmd --reload"

        self.ret_value = g.run_parallel(nodes, cmd)
        # Check for return status
        for host in self.ret_value:
            ret, _, _ = self.ret_value[host]
            if ret != 0:
                return False
        return True

    def _probe_peer(self, node, should_fail=False):
        """ Peer probe node """
        ret, _, _ = peer_probe(self.mnode, node)
        if should_fail:
            self.assertNotEqual(ret, 0, "Unexpected: Successfully peer probe"
                                " node: %s" % node)
        else:
            self.assertEqual(ret, 0, "Failed to peer probe node: %s" % node)

    def _create_distribute_volume(self, volume_name):
        """ Create 2 brick distribute volume """
        number_of_brick = 2
        servers_info_from_two_node = {}
        for server in self.servers[:2]:
            servers_info_from_two_node[server] = self.all_servers_info[server]
        self.volname = volume_name
        bricks_list = form_bricks_list(self.mnode, self.volname,
                                       number_of_brick, self.servers[:2],
                                       servers_info_from_two_node)
        ret, _, _ = volume_create(self.mnode, self.volname,
                                  bricks_list)
        self.assertEqual(ret, 0, "Volume creation failed")
        g.log.info("Volume %s created succssfully", self.volname)

    def _start_the_volume(self, volume):
        """ Start a volume """
        ret, _, _ = volume_start(self.mnode, volume)
        self.assertEqual(ret, 0, "Failed to start the "
                         "volume %s" % volume)
        g.log.info("Volume %s started successfully", volume)

    def _try_mounting_volume(self):
        """ Mount a volume """
        ret, _, _ = mount_volume(self.volname, mtype="glusterfs",
                                 mpoint="/mnt/distribute-vol",
                                 mserver=self.mnode,
                                 mclient=self.servers[1])
        self.assertNotEqual(ret, 0, "Unexpected: Volume %s is mounted"
                            % self.volname)

    def test_gluster_operation_after_removing_firewall(self):
        """
        Test steps:
        1. Add firewall services to the zones on 2 nodes
        2. Create a cluster using the 2 nodes
        3. Check peer status on both the nodes
        4. Remove firewall services from both the nodes
        5. Check peer status on both the nodes
        6. Create a distribute volume using both the node bricks and start it
        7. Mount the volume on different node, it should fail
        8. Cleanup the volume, Detach the node and try to probe again
        9. Check peer status
        10. Remove firewall services permanently and reload firewall
        11. Check peer status
        12. Create a distribute volume using both the node bricks and start it
        13. Mount the volume on different node, it should fail
        """
        # pylint: disable=too-many-statements
        # Add firewall services on first 2 nodes
        ret = self._add_firewall_services(self.servers[:2])
        self.assertTrue(ret, "Failed to add services to firewall")

        self.firewall_added = True

        # Peer probe second node
        self._probe_peer(self.servers[1])

        # Check peer status on both the nodes
        ret = wait_for_peers_to_connect(self.mnode, self.servers[:2])
        self.assertTrue(ret, "Peer is not connected")

        # Remove firewall services
        self._remove_firewall_services(self.servers[:2])

        self.firewall_added = False

        # Create a volume
        self._create_distribute_volume("distribute_volume")

        # Start the volume
        self._start_the_volume(self.volname)

        # Mount the volume on a different node, it should fail
        self._try_mounting_volume()

        # Cleanup volume before peer detach
        ret = cleanup_volume(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to cleanup volume")

        # Detach the probed node
        ret, _, _ = peer_detach(self.mnode, self.servers[1])
        self.assertEqual(ret, 0, "Failed to detach node: %s"
                         % self.servers[1])

        # Peer probe the node should fail
        self._probe_peer(self.servers[1], True)

        # Add firewall services permanently
        ret = self._add_firewall_services(self.servers[:2])
        self.assertTrue(ret, "Failed to add services to firewall")

        self.firewall_added = True

        # Reload firewall
        ret = self._reload_firewall_service(self.servers[:2])
        self.assertTrue(ret, "Failed to reload firewall service")

        # Peer probe again
        self._probe_peer(self.servers[1])

        # Check peer status the probed node
        ret = wait_for_peers_to_connect(self.mnode, self.servers[1])
        self.assertTrue(ret, "Peer is not connected")

        # Remove firewall services permanently
        self._remove_firewall_services(self.servers[:2])

        self.firewall_added = False

        # Reload firewall
        ret = self._reload_firewall_service(self.servers[:2])
        self.assertTrue(ret, "Failed to reload firewall service")

        # Check peer status
        ret = is_peer_connected(self.mnode, self.servers[1])
        self.assertTrue(ret, "Peer is not connected")

        # Create a volume
        self._create_distribute_volume("distribute_volume_2")

        # Start the volume
        self._start_the_volume(self.volname)

        # Mount the volume on a different node, it should fail
        self._try_mounting_volume()
