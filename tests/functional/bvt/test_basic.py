#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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

""" Description: BVT-Basic Tests """

import pytest
import time
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_init import (
    is_glusterd_running, restart_glusterd, start_glusterd, stop_glusterd)
from glustolibs.gluster.peer_ops import is_peer_connected, peer_status


class TestGlusterdSanity(GlusterBaseClass):
    """GLusterd Sanity check
    """
    def are_peers_in_connected_state(self):
        """Validate if all the peers are in connected state from all servers.
        """
        _rc = True
        # Validate if peer is connected from all the servers
        for server in self.servers:
            ret = is_peer_connected(server, self.servers)
            if not ret:
                _rc = False

        # Peer Status from mnode
        peer_status(self.mnode)

        return _rc

    def setUp(self):
        """setUp required for tests
        """
        GlusterBaseClass.setUp.im_func(self)
        self.test_method_complete = False

    @pytest.mark.bvt_basic
    def test_glusterd_restart_stop_start(self):
        """Tests glusterd stop, start, restart and validate if all
        peers are in connected state after glusterd restarts.
        """
        # restart glusterd on all servers
        g.log.info("Restart glusterd on all servers")
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, "Failed to restart glusterd on all servers")
        g.log.info("Successfully restarted glusterd on all servers")

        # Check if glusterd is running on all servers(expected: active)
        g.log.info("Check if glusterd is running on all servers"
                   "(expected: active)")
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, "Glusterd is not running on all servers")
        g.log.info("Glusterd is running on all the servers")

        # Stop glusterd on all servers
        g.log.info("Stop glusterd on all servers")
        ret = stop_glusterd(self.servers)
        self.assertTrue(ret, "Failed to stop glusterd on all servers")
        g.log.info("Successfully stopped glusterd on all servers")

        # Check if glusterd is running on all servers(expected: not running)
        g.log.info("Check if glusterd is running on all servers"
                   "(expected: not running)")
        ret = is_glusterd_running(self.servers)
        self.assertNotEqual(ret, 0, "Glusterd is still running on some "
                            "servers")
        g.log.info("Glusterd not running on any servers as expected.")

        # Start glusterd on all servers
        g.log.info("Start glusterd on all servers")
        ret = start_glusterd(self.servers)
        self.assertTrue(ret, "Failed to start glusterd on all servers")
        g.log.info("Successfully started glusterd on all servers")

        # Check if glusterd is running on all servers(expected: active)
        g.log.info("Check if glusterd is running on all servers"
                   "(expected: active)")
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, "Glusterd is not running on all servers")
        g.log.info("Glusterd is running on all the servers")

        # Wait for all the glusterd's to establish communication.
        time.sleep(30)

        # Validate all the peers are in connected state
        g.log.info("Validating all the peers are in Cluster and Connected")
        ret = self.are_peers_in_connected_state()
        self.assertTrue(ret, "Validating Peers to be in Cluster Failed")
        g.log.info("All peers are in connected state")

        self.test_method_complete = True

    def tearDown(self):
        """In case of any failure restart glusterd on all servers
        """
        if not self.test_method_complete:
            # restart glusterd on all servers
            g.log.info("Restart glusterd on all servers")
            ret = restart_glusterd(self.servers)
            self.assertTrue(ret, "Failed to restart glusterd on all servers")
            g.log.info("Successfully restarted glusterd on all servers")

            # Wait for all the glusterd's to establish communication.
            time.sleep(30)

            # Validate all the peers are in connected state
            g.log.info("Validating all the peers are in Cluster and Connected")
            ret = self.are_peers_in_connected_state()
            self.assertTrue(ret, "Validating Peers to be in Cluster Failed")
            g.log.info("All peers are in connected state")

        GlusterBaseClass.tearDown.im_func(self)
