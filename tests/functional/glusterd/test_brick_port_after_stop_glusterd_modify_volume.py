#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list, get_volume_status,
                                           set_volume_options)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.volume_libs import (cleanup_volume)
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         is_peer_connected,
                                         peer_probe_servers,
                                         peer_detach_servers,
                                         nodes_from_pool_list)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_init import (start_glusterd, stop_glusterd,
                                             is_glusterd_running)


@runs_on([['distributed'], ['glusterfs']])
class TestBrickPortAfterModifyVolume(GlusterBaseClass):

    def setUp(self):

        # Performing peer detach
        ret = peer_detach_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to probe detached "
                                 "servers %s" % self.servers)
        g.log.info("Peer detach SUCCESSFUL.")
        self.get_super_method(self, 'setUp')()

    def tearDown(self):
        """
        clean up all volumes and peer probe to form cluster
        """
        vol_list = get_volume_list(self.mnode)
        if vol_list is not None:
            for volume in vol_list:
                ret = cleanup_volume(self.mnode, volume)
                if not ret:
                    raise ExecutionError("Failed to cleanup volume")
                g.log.info("Volume deleted successfully : %s", volume)

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

    def test_brick_port(self):
        # pylint: disable=too-many-statements, too-many-branches
        """
        In this test case:
        1. Trusted storage Pool of 2 nodes
        2. Create a distributed volumes with 2 bricks
        3. Start the volume
        4. Stop glusterd on one node 2
        5. Modify any of the volume option on node 1
        6. Start glusterd on node 2
        7. Check volume status, brick should get port
        """
        my_server_info = {
            self.servers[0]: self.all_servers_info[self.servers[0]]
        }
        my_servers = self.servers[0:2]
        index = 1
        ret, _, _ = peer_probe(self.servers[0], self.servers[index])
        self.assertEqual(ret, 0, ("peer probe from %s to %s is failed",
                                  self.servers[0], self.servers[index]))
        g.log.info("peer probe is success from %s to "
                   "%s", self.servers[0], self.servers[index])
        key = self.servers[index]
        my_server_info[key] = self.all_servers_info[key]

        self.volname = "testvol"
        bricks_list = form_bricks_list(self.mnode, self.volname, 2,
                                       my_servers,
                                       my_server_info)
        g.log.info("Creating a volume %s ", self.volname)
        ret = volume_create(self.mnode, self.volname,
                            bricks_list, force=False)
        self.assertEqual(ret[0], 0, ("Unable"
                                     "to create volume %s" % self.volname))
        g.log.info("Volume created successfully %s", self.volname)

        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start the "
                                  "volume %s", self.volname))
        g.log.info("Get all the bricks of the volume")
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the brick list")

        g.log.info("Successfully got the list of bricks of volume")

        vol_status = get_volume_status(self.mnode, self.volname)
        self.assertIsNotNone(vol_status, "Failed to get volume "
                             "status for %s" % self.volname)
        totport = 0
        for _, value in vol_status.items():
            for _, val in value.items():
                for _, value1 in val.items():
                    if int(value1["port"]) > 0:
                        totport += 1

        self.assertEqual(totport, 2, ("Volume %s is not started successfully"
                                      "because no. of brick port is not equal"
                                      " to 2", self.volname))

        ret = stop_glusterd(self.servers[1])
        self.assertTrue(ret, "Failed to stop glusterd on one of the node")
        count = 0
        while count < 60:
            ret = is_glusterd_running(self.servers[1])
            if ret:
                break
            sleep(2)
            count += 1
        self.assertEqual(ret, 1, "glusterd is still running on %s"
                         % self.servers[1])
        g.log.info("Glusterd stop on the nodes : %s "
                   "succeeded", self.servers[1])

        option = {'performance.readdir-ahead': 'on'}
        ret = set_volume_options(self.servers[0], self.volname, option)
        self.assertTrue(ret, "gluster volume set %s performance.readdir-ahead"
                             "on is failed on server %s"
                        % (self.volname, self.servers[0]))
        g.log.info("gluster volume set %s performance.readdir-ahead on"
                   "successfully on :%s", self.volname, self.servers[0])

        ret = start_glusterd(self.servers[1])
        self.assertTrue(ret, "Failed to start glusterd on one of the node")
        g.log.info("Glusterd start on the nodes : %s "
                   "succeeded", self.servers[1])
        count = 0
        while count < 60:
            ret = is_glusterd_running(self.servers[1])
            if not ret:
                break
            sleep(2)
            count += 1

        self.assertEqual(ret, 0, "glusterd is not running on %s"
                         % self.servers[1])
        g.log.info("Glusterd start on the nodes : %s "
                   "succeeded", self.servers[1])

        count = 0
        while count < 60:
            ret = is_peer_connected(self.servers[0], self.servers[1])
            if ret:
                break
            sleep(2)
            count += 1
        self.assertEqual(ret, 1, "glusterd is not connected %s with peer %s"
                         % (self.servers[0], self.servers[1]))

        vol_status = get_volume_status(self.mnode, self.volname)
        self.assertIsNotNone(vol_status, "Failed to get volume "
                             "status for %s" % self.volname)
        totport = 0
        for _, value in vol_status.items():
            for _, val in value.items():
                for _, value1 in val.items():
                    if int(value1["port"]) > 0:
                        totport += 1

        self.assertEqual(totport, 2, ("Volume %s is not started successfully"
                                      "because no. of brick port is not equal"
                                      " to 2", self.volname))
