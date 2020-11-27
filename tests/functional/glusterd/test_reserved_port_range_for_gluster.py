#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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

""" Description:
      Setting reserved port range for gluster
"""

from random import choice
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list)
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.lib_utils import get_servers_bricks_dict
from glustolibs.gluster.gluster_init import restart_glusterd
from glustolibs.gluster.peer_ops import wait_for_peers_to_connect


class TestReservedPortRangeForGluster(GlusterBaseClass):
    def tearDown(self):
        # Reset port range if some test fails
        if self.port_range_changed:
            cmd = "sed -i 's/49200/60999/' /etc/glusterfs/glusterd.vol"
            ret, _, _ = g.run(self.mnode, cmd)
            self.assertEqual(ret, 0, "Failed to set the max-port back to"
                             " 60999 in glusterd.vol file")

        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume %s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        # Calling baseclass tearDown method
        self.get_super_method(self, 'tearDown')()

    def test_reserved_port_range_for_gluster(self):
        """
        Test Case:
        1) Set the max-port option in glusterd.vol file to 49200
        2) Restart glusterd on one of the node
        3) Create 50 volumes in a loop
        4) Try to start the 50 volumes in a loop
        5) Confirm that the 50th volume failed to start
        6) Confirm the error message, due to which volume failed to start
        7) Set the max-port option in glusterd.vol file back to default value
        8) Restart glusterd on the same node
        9) Starting the 50th volume should succeed now
        """
        # Set max port number as 49200 in glusterd.vol file
        cmd = "sed -i 's/60999/49200/' /etc/glusterfs/glusterd.vol"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to set the max-port to 49200 in"
                         " glusterd.vol file")

        self.port_range_changed = True

        # Restart glusterd
        ret = restart_glusterd(self.mnode)
        self.assertTrue(ret, "Failed to restart glusterd")
        g.log.info("Successfully restarted glusterd on node: %s", self.mnode)

        # Check node on which glusterd was restarted is back to 'Connected'
        # state from any other peer
        ret = wait_for_peers_to_connect(self.servers[1], self.servers)
        self.assertTrue(ret, "All the peers are not in connected state")

        # Fetch the available bricks dict
        bricks_dict = get_servers_bricks_dict(self.servers,
                                              self.all_servers_info)
        self.assertIsNotNone(bricks_dict, "Failed to get the bricks dict")

        # Create 50 volumes in a loop
        for i in range(1, 51):
            self.volname = "volume-%d" % i
            bricks_list = []
            j = 0
            for key, value in bricks_dict.items():
                j += 1
                brick = choice(value)
                brick = "{}:{}/{}_brick-{}".format(key, brick,
                                                   self.volname, j)
                bricks_list.append(brick)

            ret, _, _ = volume_create(self.mnode, self.volname, bricks_list)
            self.assertEqual(ret, 0, "Failed to create volume: %s"
                             % self.volname)
            g.log.info("Successfully created volume: %s", self.volname)

        # Try to start 50 volumes in loop
        for i in range(1, 51):
            self.volname = "volume-%d" % i
            ret, _, err = volume_start(self.mnode, self.volname)
            if ret:
                break
        g.log.info("Successfully started all the volumes until volume: %s",
                   self.volname)

        # Confirm if the 50th volume failed to start
        self.assertEqual(i, 50, "Failed to start the volumes volume-1 to"
                         " volume-49 in a loop")

        # Confirm the error message on volume start fail
        err_msg = ("volume start: volume-50: failed: Commit failed on"
                   " localhost. Please check log file for details.")
        self.assertEqual(err.strip(), err_msg, "Volume start failed with"
                         " a different error message")

        # Confirm the error message from the log file
        cmd = ("cat /var/log/glusterfs/glusterd.log | %s"
               % "grep -i 'All the ports in the range are exhausted' | wc -l")
        ret, out, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to 'grep' the glusterd.log file")
        self.assertNotEqual(out, "0", "Volume start didn't fail with expected"
                            " error message")

        # Set max port number back to default value in glusterd.vol file
        cmd = "sed -i 's/49200/60999/' /etc/glusterfs/glusterd.vol"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to set the max-port back to 60999 in"
                         " glusterd.vol file")

        self.port_range_changed = False

        # Restart glusterd on the same node
        ret = restart_glusterd(self.mnode)
        self.assertTrue(ret, "Failed to restart glusterd")
        g.log.info("Successfully restarted glusterd on node: %s", self.mnode)

        # Starting the 50th volume should succeed now
        self.volname = "volume-%d" % i
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start volume: %s" % self.volname)
