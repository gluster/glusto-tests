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
                                           get_volume_list)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.volume_libs import (cleanup_volume)
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         peer_probe_servers,
                                         nodes_from_pool_list,
                                         is_peer_connected)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_fix_layout_to_complete)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.gluster.glusterfile import get_fattr


@runs_on([['distributed'], ['glusterfs']])
class TestSpuriousRebalance(GlusterBaseClass):

    def setUp(self):

        # Performing peer detach
        for server in self.servers[1:]:
            ret, _, _ = peer_detach(self.mnode, server)
            if ret != 0:
                raise ExecutionError("Peer detach failed")
            g.log.info("Peer detach SUCCESSFUL.")
        self.get_super_method(self, 'setUp')()

    def tearDown(self):

        # UnMount Volume
        g.log.info("Starting to Unmount Volume %s", self.volname)
        ret = umount_volume(self.mounts[0].client_system,
                            self.mounts[0].mountpoint, mtype=self.mount_type)
        self.assertTrue(ret, ("Failed to Unmount Volume %s" % self.volname))
        g.log.info("Successfully Unmounted Volume %s", self.volname)

        # Clean up all volumes and peer probe to form cluster
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

    def test_spurious_rebalance(self):
        """
        In this test case:
        1. Trusted storage Pool of 3 nodes
        2. Create a distributed volumes with 3 bricks
        3. Start the volume
        4. Fuse mount the gluster volume on out of trusted nodes
        5. Remove a brick from the volume
        6. Check remove-brick status
        7. Stop the remove brick process
        8. Perform fix-layoyt on the volume
        9. Get the rebalance fix-layout status
       10. Create a directory from mount point
       11. Check trusted.glusterfs.dht extended attribue for newly
           created directory on the remove brick
        """

        # pylint: disable=too-many-statements
        my_servers = self.servers[0:3]
        my_server_info = {}
        for server in self.servers[0:3]:
            my_server_info[server] = self.all_servers_info[server]
        for index in range(1, 3):
            ret, _, _ = peer_probe(self.servers[0], self.servers[index])
            self.assertEqual(ret, 0, ("peer probe from %s to %s is failed",
                                      self.servers[0], self.servers[index]))
            g.log.info("peer probe is success from %s to "
                       "%s", self.servers[0], self.servers[index])
        # Checking if peer is connected
        counter = 0
        while counter < 30:
            ret = is_peer_connected(self.mnode, self.servers[:3])
            counter += 1
            if ret:
                break
            sleep(3)
        self.assertTrue(ret, "Peer is not in connected state.")
        g.log.info("Peers is in connected state.")

        self.volname = "testvol"
        bricks_list = form_bricks_list(self.mnode, self.volname, 3,
                                       my_servers,
                                       my_server_info)
        g.log.info("Creating a volume %s ", self.volname)
        ret, _, _ = volume_create(self.mnode, self.volname,
                                  bricks_list, force=False)
        self.assertEqual(ret, 0, ("Unable"
                                  "to create volume %s" % self.volname))
        g.log.info("Volume created successfully %s", self.volname)

        ret, _, _ = volume_start(self.mnode, self.volname, False)
        self.assertEqual(ret, 0, ("Failed to start the "
                                  "volume %s", self.volname))
        g.log.info("Get all the bricks of the volume")
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        # Mounting a volume
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume %s is not mounted") % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)
        remove_brick_list = []
        remove_brick_list.append(bricks_list[2])
        ret, _, _ = remove_brick(self.mnode, self.volname, remove_brick_list,
                                 'start')
        self.assertEqual(ret, 0, "Failed to start remove brick operation")
        g.log.info("Remove bricks operation started successfully")

        ret, _, _ = remove_brick(self.mnode, self.volname, remove_brick_list,
                                 'stop')
        self.assertEqual(ret, 0, "Failed to stop remove brick operation")
        g.log.info("Remove bricks operation stopped successfully")

        g.log.info("Starting Fix-layoyt on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname, True)
        self.assertEqual(ret, 0, ("Failed to start rebalance for fix-layout"
                                  "on the volume %s", self.volname))
        g.log.info("Successfully started fix-layout on the volume %s",
                   self.volname)

        # Wait for fix-layout to complete
        g.log.info("Waiting for fix-layout to complete")
        ret = wait_for_fix_layout_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Fix-layout is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Fix-layout is successfully complete on the volume %s",
                   self.volname)
        ret = mkdir(self.mounts[0].client_system, "%s/dir1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory dir1"))
        g.log.info("directory dir1 is created successfully")

        brick_server, brick_dir = remove_brick_list[0].split(':')
        folder_name = brick_dir+"/dir1"
        g.log.info("Check trusted.glusterfs.dht on host  %s for directory %s",
                   brick_server, folder_name)

        ret = get_fattr(brick_server, folder_name, 'trusted.glusterfs.dht')
        self.assertTrue(ret, ("Failed to get trusted.glusterfs.dht for %s"
                              % folder_name))
        g.log.info("get trusted.glusterfs.dht xattr for %s successfully",
                   folder_name)
