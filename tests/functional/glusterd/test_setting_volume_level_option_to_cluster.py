#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.peer_ops import is_peer_connected
from glustolibs.gluster.gluster_init import is_glusterd_running
from glustolibs.gluster.volume_ops import set_volume_options


class TestSettingVolumeLevelOptionToCluster(GlusterBaseClass):

    def test_setting_volume_level_option_to_cluster(self):
        # pylint: disable=too-many-statements
        """
        Test Case:
        1) Create a cluster.
        2) Try to set volume level options to cluster level.
           (These should fail!)
        eg: gluster v set all transport.listen-backlog 128
            gluster v set all performance.parallel-readdir on
        3) Check if glusterd has crashed or not.(Should not crash!)
        """

        # Set transport.listen-backlog to 128 for all volumes.(Should fail!)
        ret = set_volume_options(self.mnode, 'all',
                                 {'transport.listen-backlog': '128'})
        self.assertFalse(ret, "Error: Able to set transport.listen-backlog "
                         "to 128 for all volumes.")
        g.log.info("EXPECTED: Failed to set transport.listen-backlog to 128"
                   " for all volumes.")

        # Checking if glusterd is running on all the nodes.
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, "glusterd has crashed.")
        g.log.info("glusterd is running on all servers.")

        # Checking if all the peers are in connected state or not.
        ret = is_peer_connected(self.mnode, self.servers)
        self.assertTrue(ret, "All peers are not in connected state.")
        g.log.info("All peers are in connected state.")

        # Set performance.parallel-readdir to on for all volumes.(Should fail!)
        ret = set_volume_options(self.mnode, 'all',
                                 {'performance.parallel-readdir': 'on'})
        self.assertFalse(ret, "Error: Able to set performance.parallel"
                         "-readdir to ON for all volumes.")
        g.log.info("EXPECTED: Failed to set parallel-readdir to"
                   " ON for all volumes.")

        # Checking if glusterd is running on all the nodes
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, "glusterd has crashed.")
        g.log.info("glusterd is running on all servers.")

        # Checking if all the peers are in connected state or not.
        ret = is_peer_connected(self.mnode, self.servers)
        self.assertTrue(ret, "All peers are not in connected state.")
        g.log.info("All peers are in connected state.")
