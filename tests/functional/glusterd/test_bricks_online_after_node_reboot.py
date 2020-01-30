#  Copyright (C) 2019-2020  Red Hat, Inc. <http://www.redhat.com>
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

"""
Test Cases in this module related to gluster bricks are online
after node reboot or not
"""
from random import choice
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_start, volume_stop,
                                           volume_create, set_volume_options,
                                           get_volume_list)
from glustolibs.gluster.gluster_init import is_glusterd_running
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.brick_libs import wait_for_bricks_to_be_online
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.misc.misc_libs import reboot_nodes_and_wait_to_come_online


@runs_on([['distributed-dispersed'], ['glusterfs']])
class BricksOnlineAfterNodeReboot(GlusterBaseClass):
    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume()
        if ret:
            g.log.info("Volme created successfully : %s", self.volname)
        else:
            raise ExecutionError("Volume creation failed: %s" % self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """
        # Cleaning up the volume
        volume_list = get_volume_list(choice(self.servers))
        for volume in volume_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Failed Cleanup the Volume %s" % volume)
        g.log.info("Successfully cleaned up all the volumes")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def check_bricks_online(self, all_volumes):
        for volume in all_volumes:
            self.assertTrue(wait_for_bricks_to_be_online(
                self.mnode, volume), "Unexpected: Few bricks are offline")
            g.log.info("All bricks are online in the volume %s ", volume)

    def check_node_after_reboot(self, server):
        count = 0
        while count < 80:
            ret = is_glusterd_running(server)
            if not ret:
                ret = self.validate_peers_are_connected()
                if ret:
                    g.log.info("glusterd is running and all peers are in"
                               "connected state")
                    break
            count += 1
            sleep(10)
        self.assertNotEqual(count, 60, "Either glusterd is not runnig or peers"
                                       " are not in connected state")

    def test_bricks_online_after_node_reboot(self):
        '''
        Create all types of volumes
        Start the volume and check the bricks are online
        Reboot a node at random
        After the node is up check the bricks are online
        Set brick-mux to on
        stop and start the volume to get the brick-mux into effect
        Check all bricks are online
        Now perform node reboot
        After node reboot all bricks should be online
        '''

        # Creating all types of volumes disperse, replicate, arbiter
        all_volumes = ['disperse', 'replicate', 'arbiter']
        for volume in all_volumes:
            bricks_list = form_bricks_list(self.mnode, volume,
                                           6 if volume == "disperse" else 3,
                                           self.servers,
                                           self.all_servers_info)
            if volume == "disperse":
                ret, _, _ = volume_create(self.mnode, volume, bricks_list,
                                          disperse_count=6,
                                          redundancy_count=2)
            elif volume == "replicate":
                ret, _, _ = volume_create(self.mnode, volume, bricks_list,
                                          replica_count=3)
            else:
                ret, _, _ = volume_create(self.mnode, volume, bricks_list,
                                          replica_count=3, arbiter_count=1)
            self.assertEqual(ret, 0, "Unexpected: Volume create '%s' failed"
                             % volume)
            g.log.info("volume create %s succeeded", volume)
        # All volumes start
        for volume in all_volumes:
            ret, _, _ = volume_start(self.mnode, volume)
            self.assertEqual(ret, 0, "Unexpected: Volume start succeded %s"
                             % volume)
            g.log.info("Volume started succesfully %s", volume)

        # Adding self.volname to the all_volumes list
        all_volumes.append(self.volname)

        # Validate whether all volume bricks are online or not
        self.check_bricks_online(all_volumes)
        # Perform node reboot
        random_server = choice(self.servers)
        ret, _ = reboot_nodes_and_wait_to_come_online(random_server)
        self.assertTrue(ret, "Reboot Failed on node %s" % random_server)
        g.log.info("Node: %s rebooted successfully", random_server)

        # Wait till glusterd is started on the node rebooted
        self.check_node_after_reboot(random_server)

        # After reboot check bricks are online
        self.check_bricks_online(all_volumes)

        # Enable brick-mux on and stop and start the volumes
        ret = set_volume_options(self.mnode, 'all',
                                 {"cluster.brick-multiplex": "enable"})
        self.assertTrue(ret, "Unable to set the volume option")
        g.log.info("Brick-mux option enabled successfully")
        self.addCleanup(set_volume_options, self.mnode, 'all',
                        {"cluster.brick-multiplex": "disable"})

        # Stop all the volumes in the cluster
        for vol in all_volumes:
            ret, _, _ = volume_stop(self.mnode, vol)
            self.assertEqual(ret, 0, "volume stop failed on %s" % vol)
            g.log.info("volume: %s stopped successfully", vol)

        # Starting the volume to get brick-mux into effect
        for vol in all_volumes:
            ret, _, _ = volume_start(self.mnode, vol)
            self.assertEqual(ret, 0, "volume start failed on %s" % vol)
            g.log.info("volume: %s started successfully", vol)

        # Checking all bricks are online or not
        self.check_bricks_online(all_volumes)

        # Perform node reboot
        ret, _ = reboot_nodes_and_wait_to_come_online(random_server)
        self.assertTrue(ret, "Reboot Failed on node %s" % random_server)
        g.log.info("Node: %s rebooted successfully", random_server)

        # Wait till glusterd is started on the node rebooted
        self.check_node_after_reboot(random_server)

        # Validating bricks are online after node reboot
        self.check_bricks_online(all_volumes)
