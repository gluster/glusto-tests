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
import time
import string
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list, get_volume_status)
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline)
from glustolibs.gluster.volume_libs import (cleanup_volume)
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         peer_probe_servers,
                                         nodes_from_pool_list)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed'], ['glusterfs']])
class TestAddIdenticalBrick(GlusterBaseClass):

    def setUp(self):

        # Performing peer detach
        for server in self.servers[1:]:
            ret, _, _ = peer_detach(self.mnode, server)
            if ret != 0:
                raise ExecutionError("Peer detach failed")
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

    def test_add_identical_brick(self):
        """
        In this test case:
        1. Create Dist Volume on Node 1
        2. Down brick on Node 1
        3. Peer Probe N2 from N1
        4. Add identical brick on newly added node
        5. Check volume status
        """

        # pylint: disable=too-many-statements
        # Create a distributed volume on Node1
        number_of_brick = 1
        servers_info_from_single_node = {
            self.servers[0]: self.all_servers_info[self.servers[0]]
        }
        self.volname = "testvol"
        bricks_list = form_bricks_list(self.servers[0], self.volname,
                                       number_of_brick, self.servers[0],
                                       servers_info_from_single_node)
        ret, _, _ = volume_create(self.servers[0], self.volname,
                                  bricks_list, force=False)
        self.assertEqual(ret, 0, "Volume create failed")
        g.log.info("Volume %s created successfully", self.volname)

        ret, _, _ = volume_start(self.servers[0], self.volname, True)
        self.assertEqual(ret, 0, ("Failed to start the "
                                  "volume %s", self.volname))
        g.log.info("Get all the bricks of the volume")
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        ret = bring_bricks_offline(self.volname, bricks_list[0])
        self.assertTrue(ret, "Failed to bring down the bricks")
        g.log.info("Successfully brought the bricks down")

        ret, _, _ = peer_probe(self.servers[0], self.servers[1])
        self.assertEqual(ret, 0, ("peer probe from %s to %s is failed",
                                  self.servers[0], self.servers[1]))
        g.log.info("peer probe is success from %s to "
                   "%s", self.servers[0], self.servers[1])

        # wait for some time before add-brick
        time.sleep(2)

        # Replace just host IP to create identical brick
        add_bricks = []
        try:
            add_bricks.append(string.replace(bricks_list[0],
                                             self.servers[0],
                                             self.servers[1]))
        except AttributeError:
            add_bricks.append(str.replace(bricks_list[0],
                                          self.servers[0],
                                          self.servers[1]))
        ret, _, _ = add_brick(self.mnode, self.volname, add_bricks)
        self.assertEqual(ret, 0, "Failed to add the bricks to the volume")
        g.log.info("Successfully added bricks to volume %s", add_bricks[0])

        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Volume start with force failed")

        vol_status = get_volume_status(self.mnode, self.volname)
        self.assertIsNotNone(vol_status, "Failed to get volume "
                             "status for %s" % self.volname)
