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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import (volume_create, volume_start)
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.peer_ops import is_peer_connected
from glustolibs.gluster.lib_utils import form_bricks_list


@runs_on([['distributed'], ['glusterfs']])
class TestVolumeCreateWithGlusterdRestarts(GlusterBaseClass):

    def tearDown(self):

        # wait till peers are in connected state
        count = 0
        while count < 60:
            ret = is_peer_connected(self.mnode, self.servers)
            if ret:
                break
            sleep(3)

        # clean up volumes
        ret = cleanup_volume(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Unable to delete volume % s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        GlusterBaseClass.tearDown.im_func(self)

    def test_volume_create_with_glusterd_restarts(self):
        # pylint: disable=too-many-statements
        """
        Test case:
        1) Create a cluster.
        2) Create volume using the first three nodes say N1, N2 and N3.
        3) While the create is happening restart the fourth node N4.
        4) Check if glusterd has crashed on any node.
        5) While the volume start is happening restart N4.
        6) Check if glusterd has crashed on any node.
        """

        # Fetching all the parameters for volume_create
        list_of_three_servers = []
        server_info_for_three_nodes = {}

        for server in self.servers[0:3]:
            list_of_three_servers.append(server)
            server_info_for_three_nodes[server] = self.all_servers_info[server]

        bricks_list = form_bricks_list(self.mnode, self.volname,
                                       3, list_of_three_servers,
                                       server_info_for_three_nodes)
        # Restarting glusterd in a loop
        restart_cmd = ("for i in `seq 1 5`; do "
                       "service glusterd restart; sleep 3; "
                       "done")
        proc1 = g.run_async(self.servers[3], restart_cmd)

        # Creating volumes using 3 servers
        ret, _, _ = volume_create(self.mnode, self.volname,
                                  bricks_list)
        self.assertEqual(ret, 0, "Volume creation failed")
        g.log.info("Volume %s created successfully", self.volname)

        ret, _, _ = proc1.async_communicate()
        self.assertEqual(ret, 0, "Glusterd restart not working.")

        # Checking if peers are connected or not.
        count = 0
        while count < 60:
            ret = is_peer_connected(self.mnode, self.servers)
            if ret:
                break
            sleep(3)
        self.assertTrue(ret, "Peers are not in connected state.")
        g.log.info("Peers are in connected state.")

        # Restarting glusterd in a loop
        restart_cmd = ("for i in `seq 1 5`; do "
                       "service glusterd restart; sleep 3; "
                       "done")
        proc1 = g.run_async(self.servers[3], restart_cmd)

        # Start the volume created.
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Volume start failed")
        g.log.info("Volume %s started successfully", self.volname)

        ret, _, _ = proc1.async_communicate()
        self.assertEqual(ret, 0, "Glusterd restart not working.")

        # Checking if peers are connected or not.
        count = 0
        while count < 60:
            ret = is_peer_connected(self.mnode, self.servers)
            if ret:
                break
            sleep(3)
        self.assertTrue(ret, "Peers are not in connected state.")
        g.log.info("Peers are in connected state.")
