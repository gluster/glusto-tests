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
      TC to validate optimized handshake by glusterd with 2000 volumes
"""

from random import choice
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.volume_ops import get_volume_list
from glustolibs.gluster.gluster_init import (stop_glusterd, restart_glusterd,
                                             wait_for_glusterd_to_start)
from glustolibs.gluster.volume_libs import (cleanup_volume)
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           get_volume_options,
                                           volume_create,
                                           volume_start)
from glustolibs.gluster.peer_ops import (is_peer_connected,
                                         peer_probe_servers,
                                         peer_detach,
                                         nodes_from_pool_list)
from glustolibs.gluster.lib_utils import get_servers_bricks_dict


class TestValidateOptimizedGlusterdHandshake(GlusterBaseClass):

    def setUp(self):
        """ Detach peers, and leave it as a 3 node cluster """
        for server in self.servers[3:]:
            ret, _, _ = peer_detach(self.mnode, server)
            if ret:
                raise ExecutionError("Peer detach failed")

        self.get_super_method(self, 'setUp')()

    def tearDown(self):
        """ Cleanup the volumes """
        if self.glusterd_is_stopped:
            ret = restart_glusterd(self.servers[1])
            if not ret:
                raise ExecutionError("Failed to start glusterd on node: %s"
                                     % self.servers[1])

            ret = wait_for_glusterd_to_start(self.servers[1])
            if not ret:
                raise ExecutionError("Glusterd is not yet started on node: %s"
                                     % self.servers[1])

        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume %s" % volume)

        # Disable multiplex
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.brick-multiplex': 'disable'})
        if not ret:
            raise ExecutionError("Failed to disable brick mux in cluster")

        # Peer probe detached servers
        pool = nodes_from_pool_list(self.mnode)
        for node in pool:
            peer_detach(self.mnode, node)
        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to probe detached "
                                 "servers %s" % self.servers)

        # Calling baseclass tearDown method
        self.get_super_method(self, 'tearDown')()

    def test_validate_optimized_glusterd_handshake(self):
        """
        Test Case:
        1) Create a 3 node cluster
        2) Enable brick-multiplex in the cluster
        3) Create and start 2000 volumes
        4) Stop one of the node in the cluster
        5) Set an option for around 850 volumes in the cluster
        6) Restart glusterd on the previous node
        7) Check the value of the option set earlier, in the restarted node
        """
        # pylint: disable=too-many-locals
        # Enable brick-multiplex
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.brick-multiplex': 'enable'})
        self.assertTrue(ret, "Failed to enable brick mux on cluster")

        server_info_frm_three_node = {}
        for server in self.servers[:3]:
            server_info_frm_three_node[server] = self.all_servers_info[server]

        # Fetch the available bricks dict
        bricks_dict = get_servers_bricks_dict(self.servers[:3],
                                              server_info_frm_three_node)
        self.assertIsNotNone(bricks_dict, "Failed to get the bricks dict")

        # Using, custome method because method bulk_volume_creation creates
        # a huge logging and does unwanted calls, which will slow down the
        # test case and use more memory
        # Create and start 2000 volumes
        for i in range(2000):
            self.volname = "volume-%d" % i
            bricks_list = []
            j = 0
            for key, value in bricks_dict.items():
                j += 1
                brick = choice(value)
                brick = "{}:{}/{}_brick-{}".format(key, brick,
                                                   self.volname, j)
                bricks_list.append(brick)

            kwargs = {'replica_count': 3}

            ret, _, _ = volume_create(self.mnode, self.volname,
                                      bricks_list, False, **kwargs)
            self.assertEqual(ret, 0, "Failed to create volume: %s"
                             % self.volname)

            ret, _, _ = volume_start(self.mnode, self.volname)
            self.assertEqual(ret, 0, "Failed to start volume: %s"
                             % self.volname)

        g.log.info("Successfully created and started all the volumes")

        # Stop glusterd on one node
        ret = stop_glusterd(self.servers[1])
        self.assertTrue(ret, "Failed to stop glusterd on node :%s"
                        % self.servers[1])

        self.glusterd_is_stopped = True

        # Set a volume option for 800 volumes
        option_value = {'network.ping-timeout': 45}
        for i in range(850):
            vol_name = "volume-" + str(i)
            ret = set_volume_options(self.mnode, vol_name, option_value)
            self.assertTrue(ret, "Failed to set volume option")

        # Start glusterd on the previous node
        ret = restart_glusterd(self.servers[1])
        self.assertTrue(ret, "Failed to start glusterd on node: %s"
                        % self.servers[1])

        ret = wait_for_glusterd_to_start(self.servers[1])
        self.assertTrue(ret, "Glusterd is not yet started on the node :%s"
                        % self.servers[1])

        # It might take some time, to get the peers to connected state,
        # because of huge number of volumes to sync
        while True:
            ret = is_peer_connected(self.mnode, self.servers[1:3])
            if ret:
                break
            sleep(1)

        self.assertTrue(ret, "Peers are not in connected state")

        self.glusterd_is_stopped = False

        # Check the volume option set earlier is synced on restarted node
        for i in range(850):
            vol_name = "volume-" + str(i)
            # Doing, a while True loop because there might be race condition
            # and it might take time for the node to sync the data initially
            while True:
                ret = get_volume_options(self.servers[1], vol_name,
                                         'network.ping-timeout')
                self.assertTrue(ret, "Failed to get volume option")
                g.log.info("Ret: %s", ret['network.ping-timeout'])
                if ret['network.ping-timeout'] == '45':
                    break
            self.assertEqual(ret['network.ping-timeout'], '45',
                             "Option value not updated in the restarted node")
