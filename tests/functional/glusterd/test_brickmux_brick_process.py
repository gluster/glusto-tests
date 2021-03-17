#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.volume_libs import bulk_volume_creation, cleanup_volume
from glustolibs.gluster.volume_ops import (set_volume_options, get_volume_list,
                                           volume_start)
from glustolibs.misc.misc_libs import are_nodes_online


class TestBrickMuxProcessWhileNodeReboot(GlusterBaseClass):
    """ Description:
        [Brick-mux] Observing multiple brick processes on node reboot with
        volume start
    """
    def test_brickmux_brick_process(self):
        """
        1. Create a 3 node cluster.
        2. Set cluster.brick-multiplex to enable.
        3. Create 15 volumes of type replica 1x3.
        4. Start all the volumes one by one.
        5. While the volumes are starting reboot one node.
        6. check for pifof glusterfsd single process should be visible
        """
        volume_config = {
            'name': 'test',
            'servers': self.all_servers[:3],
            'voltype': {'type': 'replicated',
                        'replica_count': 3,
                        'transport': 'tcp'}}

        servers = self.all_servers[:3]
        # Volume Creation
        ret = bulk_volume_creation(
            self.mnode, 14, self.all_servers_info, volume_config,
            is_create_only=True)
        self.assertTrue(ret, "Volume creation Failed")
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.brick-multiplex': 'enable'})
        self.assertTrue(ret)
        vol_list = get_volume_list(self.mnode)
        for volname in vol_list:
            if vol_list.index(volname) == 2:
                g.run(servers[2], "reboot")
            ret, out, _ = volume_start(self.mnode, volname)
            self.assertFalse(
                ret, "Failed to start volume '{}'".format(volname))

        for _ in range(10):
            sleep(1)
            _, node_result = are_nodes_online(servers[2])
            self.assertTrue(node_result, "Node is not Online")

        for server in servers:
            ret, out, _ = g.run(server, "pgrep glusterfsd")
            out = out.split()
            self.assertFalse(ret, "Failed to get 'glusterfsd' pid")
            self.assertEqual(
                len(out), 1, "More then 1 brick process  seen in glusterfsd")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        for vol in get_volume_list(self.mnode):
            ret = cleanup_volume(self.mnode, vol)
            if not ret:
                raise ExecutionError("Failed to  Cleanup Volume")
            ret = set_volume_options(self.mnode, 'all',
                                     {'cluster.brick-multiplex': 'disable'})
            if not ret:
                raise ExecutionError("Failed to set volume option")
            # Calling GlusterBaseClass teardown
            self.get_super_method(self, 'tearDown')()
