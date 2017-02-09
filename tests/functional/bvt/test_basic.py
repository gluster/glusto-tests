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
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_init import (
    is_glusterd_running, restart_glusterd, start_glusterd, stop_glusterd)


class TestGlusterdSanity(GlusterBaseClass):
    @pytest.mark.bvt_basic
    def test_glusterd_restart_stop_start(self):
        """Tests glusterd stop, start, restart
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

    def tearDown(self):
        """In case of any failure restart glusterd on all servers
        """
        # restart glusterd on all servers
        g.log.info("Restart glusterd on all servers")
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, "Failed to restart glusterd on all servers")
        g.log.info("Successfully restarted glusterd on all servers")
        GlusterBaseClass.tearDown.im_func(self)
