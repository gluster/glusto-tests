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

import time
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.peer_ops import is_peer_connected
from glustolibs.gluster.volume_libs import (cleanup_volume,
                                            setup_volume)
from glustolibs.gluster.volume_ops import (get_volume_list,
                                           volume_reset,
                                           set_volume_options)
from glustolibs.gluster.gluster_init import (is_glusterd_running,
                                             start_glusterd, stop_glusterd)


@runs_on([['distributed-replicated'], ['glusterfs']])
class GlusterdSplitBrainQuorumValidation(GlusterBaseClass):

    def setUp(self):
        GlusterBaseClass.setUp.im_func(self)

        # Overriding the volume type to specifically test the volume type
        if self.volume_type == "distributed-replicated":
            self.volume['voltype'] = {
                'type': 'distributed-replicated',
                'replica_count': 2,
                'dist_count': 4,
                'transport': 'tcp'}

        # Create a distributed-replicated volume with replica count 2
        # using first four nodes
        servers_info_from_four_nodes = {}
        for server in self.servers[0:4]:
            servers_info_from_four_nodes[
                server] = self.all_servers_info[server]

        self.volume['servers'] = self.servers[0:4]
        ret = setup_volume(self.mnode, servers_info_from_four_nodes,
                           self.volume, force=False)
        if not ret:
            raise ExecutionError("Volume create failed on four nodes")
        g.log.info("Distributed replicated volume created successfully")

    def tearDown(self):
        # stopping the volume and Cleaning up the volume
        GlusterBaseClass.tearDown.im_func(self)
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Failed Cleanup the Volume")
        g.log.info("Volume deleted successfully")

    def test_glusterd_split_brain_with_quorum(self):
        """
        - On a 6 node cluster
        - Create a volume using first four nodes
        - Set the volumes options
        - Stop two gluster nodes
        - Perform gluster vol reset
        - Start the glusterd on the nodes where it stopped
        - Check the peer status, all the nodes should be in connected state

        """
        # Before starting the testcase, proceed only it has minimum of 6 nodes
        self.assertGreaterEqual(len(self.servers), 6,
                                "Not enough servers to run this test")

        # Volume options to set on the volume
        volume_options = {
            'nfs.disable': 'off',
            'auth.allow': '1.1.1.1',
            'nfs.rpc-auth-allow': '1.1.1.1',
            'nfs.addr-namelookup': 'on',
            'cluster.server-quorum-type': 'server',
            'network.ping-timeout': '20',
            'nfs.port': '2049',
            'performance.nfs.write-behind': 'on',
            }

        # Set the volume options
        ret = set_volume_options(self.mnode, self.volname, volume_options)
        self.assertTrue(ret, "Unable to set the volume options")
        g.log.info("All the volume_options set succeeded")

        # Stop glusterd on two gluster nodes where bricks aren't present
        ret = stop_glusterd(self.servers[-2:])
        self.assertTrue(ret, "Failed to stop glusterd on one of the node")
        g.log.info("Glusterd stop on the nodes : %s "
                   "succeeded", self.servers[-2:])

        # Check glusterd is stopped
        ret = is_glusterd_running(self.servers[-2:])
        self.assertEqual(ret, 1, "Glusterd is running on nodes")
        g.log.info("Expected: Glusterd stopped on nodes %s", self.servers[-2:])

        # Performing volume reset on the volume to remove all the volume
        # options set earlier
        ret, _, err = volume_reset(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Volume reset failed with below error "
                         "%s" % err)
        g.log.info("Volume reset on the volume %s succeeded", self.volname)

        # Bring back glusterd online on the nodes where it stopped earlier
        ret = start_glusterd(self.servers[-2:])
        self.assertTrue(ret, "Failed to start glusterd on the nodes")
        g.log.info("Glusterd start on the nodes : %s "
                   "succeeded", self.servers[-2:])

        # Check peer status whether all peer are in connected state none of the
        # nodes should be in peer rejected state
        halt = 20
        counter = 0
        _rc = False
        g.log.info("Wait for some seconds, right after glusterd start it "
                   "will create two daemon process it need few seconds "
                   "(like 3-5) to initialize the glusterd")
        while counter < halt:
            ret = is_peer_connected(self.mnode, self.servers)
            if not ret:
                g.log.info("Peers are not connected state,"
                           " Retry after 2 seconds .......")
                time.sleep(2)
                counter = counter + 2
            else:
                _rc = True
                g.log.info("Peers are in connected state in the cluster")
                break
        if not _rc:
            raise ExecutionError("Peers are not connected state after "
                                 "bringing back glusterd online on the "
                                 "nodes in which previously glusterd "
                                 "had been stopped")
