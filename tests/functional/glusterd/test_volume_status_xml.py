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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import (volume_create, volume_status,
                                           get_volume_status, volume_start)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.peer_ops import (peer_probe_servers, peer_detach,
                                         peer_detach_servers,
                                         nodes_from_pool_list)


@runs_on([['distributed'], ['glusterfs']])
class TestVolumeStatusxml(GlusterBaseClass):

    def setUp(self):

        GlusterBaseClass.setUp.im_func(self)

        # check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

        # detach all the nodes
        ret = peer_detach_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Peer detach failed to all the servers from "
                                 "the node %s." % self.mnode)
        g.log.info("Peer detach SUCCESSFUL.")

    def tearDown(self):

        # stopping and cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup the Volume %s"
                                 % self.volname)

        pool = nodes_from_pool_list(self.mnode)
        for node in pool:
            peer_detach(self.mnode, node)

        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to probe detached "
                                 "servers %s" % self.servers)
        GlusterBaseClass.tearDown.im_func(self)

    def test_volume_status_xml(self):

        # create a two node cluster
        ret = peer_probe_servers(self.servers[0], self.servers[1])
        self.assertTrue(ret, "Peer probe failed to %s from %s"
                        % (self.mnode, self.servers[1]))

        # create a distributed volume with single node
        number_of_bricks = 1
        servers_info_from_single_node = {}
        servers_info_from_single_node[
            self.servers[0]] = self.all_servers_info[self.servers[0]]

        bricks_list = form_bricks_list(self.mnode, self.volname,
                                       number_of_bricks, self.servers[0],
                                       servers_info_from_single_node)
        ret, _, _ = volume_create(self.servers[0], self.volname, bricks_list)
        self.assertEqual(ret, 0, "Volume creation failed")
        g.log.info("Volume %s created successfully", self.volname)

        # Get volume status
        ret, _, err = volume_status(self.servers[1], self.volname)
        self.assertNotEqual(ret, 0, ("Unexpected: volume status is success for"
                                     " %s, even though volume is not started "
                                     "yet" % self.volname))
        self.assertIn("is not started", err, ("volume status exited with"
                                              " incorrect error message"))

        # Get volume status with --xml
        vol_status = get_volume_status(self.servers[1], self.volname)
        self.assertIsNone(vol_status, ("Unexpected: volume status --xml for %s"
                                       " is success even though the volume is"
                                       " not stared yet" % self.volname))

        # start the volume
        ret, _, _ = volume_start(self.servers[1], self.volname)
        self.assertEqual(ret, 0, "Failed to start volume %s" % self.volname)

        # Get volume status
        ret, _, _ = volume_status(self.servers[1], self.volname)
        self.assertEqual(ret, 0, ("Failed to get volume status for %s"
                                  % self.volname))

        # Get volume status with --xml
        vol_status = get_volume_status(self.servers[1], self.volname)
        self.assertIsNotNone(vol_status, ("Failed to get volume "
                                          "status --xml for %s"
                                          % self.volname))
