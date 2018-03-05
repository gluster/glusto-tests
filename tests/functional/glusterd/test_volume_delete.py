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

import re
import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (cleanup_volume, get_volume_list,
                                            setup_volume)
from glustolibs.gluster.volume_ops import (volume_stop)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.gluster_init import stop_glusterd, start_glusterd
from glustolibs.gluster.peer_ops import peer_probe_servers, is_peer_connected


@runs_on([['distributed', 'replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'], ['glusterfs']])
class TestVolumeDelete(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # check whether peers are in connected state
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
            g.log.info("Volume deleted successfully : %s", volume)

        GlusterBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDownClass.im_func(cls)

    def test_vol_delete_when_one_of_nodes_is_down(self):

        # create a volume and start it
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start the volume")
        g.log.info("Successfully created and started the volume")

        # get the bricks list
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the bricks list")

        # get a random node other than self.mnode
        if len(bricks_list) >= len(self.servers):
            random_index = random.randint(1, len(self.servers) - 1)
        else:
            random_index = random.randint(1, len(bricks_list) - 1)

        # stop glusterd on the random node

        node_to_stop_glusterd = self.servers[random_index]
        ret = stop_glusterd(node_to_stop_glusterd)
        self.assertTrue(ret, "Failed to stop glusterd")

        # stop the volume, it should succeed
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Volume stop failed")

        # try to delete the volume, it should fail
        ret, _, err = g.run(self.mnode, "gluster volume delete %s "
                            "--mode=script" % self.volname)
        self.assertNotEqual(ret, 0, "Volume delete succeeded when one of the"
                            " brick node is down")
        if re.search(r'Some of the peers are down', err):
            g.log.info("Volume delete failed with expected error message")
        else:
            g.log.info("Volume delete failed with unexpected error message")
