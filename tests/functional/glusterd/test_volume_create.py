#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list)
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline,
                                           wait_for_bricks_to_be_online)
from glustolibs.gluster.volume_libs import setup_volume, cleanup_volume
from glustolibs.gluster.peer_ops import (peer_detach_servers, peer_probe,
                                         peer_probe_servers, is_peer_connected,
                                         peer_detach)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.gluster_init import start_glusterd, stop_glusterd
import random


@runs_on([['distributed'], ['glusterfs']])
class TestVolumeCreate(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

    def tearDown(self):

        # start glusterd on all servers
        ret = start_glusterd(self.servers)
        if not ret:
            raise ExecutionError("Failed to start glusterd on all servers")

        for server in self.servers:
            ret = is_peer_connected(server, self.servers)
            if not ret:
                ret = peer_probe_servers(server, self.servers)
                if not ret:
                    raise ExecutionError("Failed to peer probe all "
                                         "the servers")

        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume % s" % volume)
            g.log.info("Volume deleted successfully : %s" % volume)

        GlusterBaseClass.tearDown.im_func(self)

    def test_volume_create(self):

        # create and start a volume
        self.volume['name'] = "first_volume"
        self.volname = "first_volume"
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")

        # bring a brick down and volume start force should bring it to online

        g.log.info("Get all the bricks of the volume")
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        ret = bring_bricks_offline(self.volname, bricks_list[0:2])
        self.assertTrue(ret, "Failed to bring down the bricks")
        g.log.info("Successfully brought the bricks down")

        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Failed to start the volume")
        g.log.info("Volume start with force is success")

        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to bring the bricks online")
        g.log.info("Volume start with force successfully brought all the "
                   "bricks online")

        # create volume with previously used bricks and different volume name
        self.volname = "second_volume"
        ret, _, _ = volume_create(self.mnode, self.volname, bricks_list)
        self.assertNotEqual(ret, 0, "Expected: It should fail to create a "
                            "volume with previously used bricks. Actual:"
                            "Successfully created the volume with previously"
                            " used bricks")
        g.log.info("Failed to create the volume with previously used bricks")

        # create a volume with already existing volume name
        self.volume['name'] = "first_volume"
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Expected: It should fail to create a volume"
                        " with already existing volume name. Actual: "
                        "Successfully created the volume with "
                        "already existing volname")
        g.log.info("Failed to create the volume with already existing volname")

        # creating a volume with non existing brick path should fail

        self.volname = "second_volume"
        bricks_list = form_bricks_list(self.mnode, self.volname,
                                       len(self.servers), self.servers,
                                       self.all_servers_info)
        nonexisting_brick_index = random.randint(0, len(bricks_list) - 1)
        non_existing_brick = bricks_list[nonexisting_brick_index].split(":")[0]
        non_existing_path = ":/brick/non_existing_path"
        non_existing_brick = non_existing_brick + non_existing_path
        bricks_list[nonexisting_brick_index] = non_existing_brick

        ret, _, _ = volume_create(self.mnode, self.volname, bricks_list)
        self.assertNotEqual(ret, 0, "Expected: Creating a volume with non "
                            "existing brick path should fail. Actual: "
                            "Successfully created the volume with "
                            "non existing brick path")
        g.log.info("Failed to create the volume with non existing brick path")

        # cleanup the volume and peer detach all servers. form two clusters,try
        # to create a volume with bricks whose nodes are in different clusters

        # cleanup volumes
        vol_list = get_volume_list(self.mnode)
        self.assertIsNotNone(vol_list, "Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            self.assertTrue(ret, "Unable to delete volume % s" % volume)

        # peer detach all servers
        ret = peer_detach_servers(self.mnode, self.servers)
        self.assertTrue(ret, "Peer detach to all servers is failed")
        g.log.info("Peer detach to all the servers is success")

        # form cluster 1
        ret, _, _ = peer_probe(self.servers[0], self.servers[1])
        self.assertEqual(ret, 0, "Peer probe from %s to %s is failed"
                         % (self.servers[0], self.servers[1]))
        g.log.info("Peer probe is success from %s to %s"
                   % (self.servers[0], self.servers[1]))

        # form cluster 2
        ret, _, _ = peer_probe(self.servers[2], self.servers[3])
        self.assertEqual(ret, 0, "Peer probe from %s to %s is failed"
                         % (self.servers[2], self.servers[3]))
        g.log.info("Peer probe is success from %s to %s"
                   % (self.servers[2], self.servers[3]))

        # Creating a volume with bricks which are part of another
        # cluster should fail
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertFalse(ret, "Expected: Creating a volume with bricks"
                         " which are part of another cluster should fail."
                         " Actual: Successfully created the volume with "
                         "bricks which are part of another cluster")
        g.log.info("Failed to create the volume with bricks which are "
                   "part of another cluster")

        # form a cluster, bring a node down. try to create a volume when one of
        # the brick node is down
        ret, _, _ = peer_detach(self.servers[2], self.servers[3])
        self.assertEqual(ret, 0, "Peer detach is failed")
        g.log.info("Peer detach is success")

        ret = peer_probe_servers(self.mnode, self.servers)
        self.assertTrue(ret, "Peer probe is failed")
        g.log.info("Peer probe to all the servers is success")

        random_server = self.servers[random.randint(1, len(self.servers) - 1)]
        ret = stop_glusterd(random_server)
        self.assertTrue(ret, "Glusterd is stopped successfully")

        self.volume['name'] = "third_volume"
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertFalse(ret, "Expected: It should fail to create a volume "
                         "when one of the node is down. Actual: Successfully "
                         "created the volume with bbrick whose node is down")

        g.log.info("Failed to create the volume with brick whose node is down")
