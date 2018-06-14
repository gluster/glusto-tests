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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import runs_on, GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import setup_volume
from glustolibs.gluster.volume_ops import set_volume_options, get_volume_status
from glustolibs.gluster.gluster_init import (stop_glusterd, start_glusterd,
                                             is_glusterd_running)
from glustolibs.gluster.brick_libs import get_all_bricks, are_bricks_offline
from glustolibs.gluster.brick_ops import remove_brick


@runs_on([['distributed'], ['glusterfs']])
class TestRemoveBrickWhenQuorumNotMet(GlusterBaseClass):

    def setUp(self):

        GlusterBaseClass.setUp.im_func(self)

        # check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

    def tearDown(self):

        ret = is_glusterd_running(self.servers)
        if ret:
            ret = start_glusterd(self.servers)
            if not ret:
                raise ExecutionError("Failed to start glusterd on servers")

        # checking for peer status from every node
        count = 0
        while count < 80:
            ret = self.validate_peers_are_connected()
            if ret:
                break
            sleep(2)
            count += 1

        if not ret:
            raise ExecutionError("Servers are not in connected state")

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup the Volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        GlusterBaseClass.tearDown.im_func(self)

    def test_remove_brick_when_quorum_not_met(self):

        # create and start a volume
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, ("Failed to create "
                              "and start volume %s" % self.volname))

        # set cluster.server-quorum-type as server
        ret = set_volume_options(self.mnode, self.volname,
                                 {'cluster.server-quorum-type': 'server'})
        self.assertTrue(ret, ("Failed to set the quorum type as a server"
                              " on volume %s", self.volname))

        # Setting quorum ratio to 95%
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.server-quorum-ratio': '95%'})
        self.assertTrue(ret, "Failed to set server quorum ratio on %s"
                        % self.volname)

        # bring down glusterd of half nodes
        num_of_servers = len(self.servers)
        num_of_nodes_to_bring_down = num_of_servers/2

        for node in range(num_of_nodes_to_bring_down, num_of_servers):
            ret = stop_glusterd(self.servers[node])
            self.assertTrue(ret, ("Failed to stop glusterd on %s"
                                  % self.servers[node]))

        for node in range(num_of_nodes_to_bring_down, num_of_servers):
            count = 0
            while count < 80:
                ret = is_glusterd_running(self.servers[node])
                if ret:
                    break
                sleep(2)
                count += 1
            self.assertNotEqual(ret, 0, "glusterd is still running on %s"
                                % self.servers[node])

        # Verifying node count in volume status after glusterd stopped
        # on half of the servers, Its not possible to check the brick status
        # immediately in volume status after glusterd stop
        count = 0
        while count < 100:
            vol_status = get_volume_status(self.mnode, self.volname)
            servers_count = len(vol_status[self.volname].keys())
            if servers_count == num_of_servers - num_of_nodes_to_bring_down:
                break
            sleep(2)
            count += 1

        # confirm that quorum is not met, brick process should be down
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the brick list")
        bricks_to_check = bricks_list[0:num_of_nodes_to_bring_down]
        ret = are_bricks_offline(self.mnode, self.volname, bricks_to_check)
        self.assertTrue(ret, "Server quorum is not met, Bricks are up")

        # try remove brick operation, which should fail
        brick1 = bricks_list[0]
        ret, _, _ = remove_brick(self.mnode, self.volname, [brick1], "start")
        self.assertNotEqual(ret, 0, ("remove brick is success, when quorum is"
                                     " not met"))

        # confirm that, removed brick is still part of volume
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the brick list")
        if brick1 not in bricks_list:
            ret = False
            self.assertTrue(ret, ("remove brick is success, when quorum is"
                                  " not met"))
