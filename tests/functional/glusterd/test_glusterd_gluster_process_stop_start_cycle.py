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
# GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Description:
    Checking gluster processes stop and start cycle.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (
    cleanup_volume,
    wait_for_volume_process_to_be_online,
    setup_volume)
from glustolibs.gluster.gluster_init import (
    start_glusterd,
    wait_for_glusterd_to_start)
from glustolibs.gluster.peer_ops import wait_for_peers_to_connect


@runs_on([['distributed', 'replicated', 'arbiter', 'dispersed',
           'distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed'], ['glusterfs']])
class TestGlusterdStartStopCycle(GlusterBaseClass):
    """ Testing Glusterd stop and start cycle """

    def _wait_for_gluster_process_online_state(self):
        """
        Function which waits for the glusterfs processes to come up
        """
        # Wait for glusterd to be online and validate it's running.
        self.assertTrue(wait_for_glusterd_to_start(self.servers),
                        "glusterd not up on the desired nodes.")
        g.log.info("Glusterd is up and running on desired nodes.")

        # Wait for peers to connect
        ret = wait_for_peers_to_connect(self.mnode, self.servers, 50)
        self.assertTrue(ret, "Peers not in connected state.")
        g.log.info("Peers in connected state.")

        # Wait for all volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode,
                                                   self.volname,
                                                   timeout=600)
        self.assertTrue(ret, ("All volume processes not up."))
        g.log.info("All volume processes are up.")

    def test_glusterd_start_stop_cycle(self):
        """
        Test Glusterd stop-start cycle of gluster processes.
        1. Create a gluster volume.
        2. Kill all gluster related processes.
        3. Start glusterd service.
        4. Verify that all gluster processes are up.
        5. Repeat the above steps 5 times.
        """
        # Create and start a volume
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")

        for _ in range(5):
            killed_gluster_process_count = []
            # Kill gluster processes in all servers
            for server in self.servers:
                cmd = ('pkill --signal 9 -c -e "(glusterd|glusterfsd|glusterfs'
                       ')"|tail -1')
                ret, out, err = g.run(server, cmd)
                self.assertEqual(ret, 0, err)
                killed_gluster_process_count.append(int(out))

            # Start glusterd on all servers.
            ret = start_glusterd(self.servers)
            self.assertTrue(ret, ("Failed to restart glusterd on desired"
                                  " nodes."))
            g.log.info("Glusterd started on desired nodes.")

            # Wait for gluster processes to come up.
            self._wait_for_gluster_process_online_state()

            spawned_gluster_process_count = []
            # Get number of  gluster processes spawned in all server
            for server in self.servers:
                cmd = ('pgrep -c "(glusterd|glusterfsd|glusterfs)"')
                ret, out, err = g.run(server, cmd)
                self.assertEqual(ret, 0, err)
                spawned_gluster_process_count.append(int(out))

            # Compare process count in each server.
            for index, server in enumerate(self.servers):
                self.assertEqual(killed_gluster_process_count[index],
                                 spawned_gluster_process_count[index],
                                 ("All processes not up and running on %s",
                                  server))

    def tearDown(self):
        """ tear Down Callback """
        # Wait for peers to connect
        ret = wait_for_peers_to_connect(self.mnode, self.servers, 50)
        if not ret:
            raise ExecutionError("Peers are not in connected state.")

        # Cleanup the volume
        ret = cleanup_volume(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Successfully cleaned up the volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
