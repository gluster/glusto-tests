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
      Default max bricks per-process should be 250
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (get_volume_options,
                                           reset_volume_option,
                                           set_volume_options)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'], ['glusterfs']])
class TestDefaultMaxBricksPerProcess(GlusterBaseClass):
    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s"
                                 % self.volname)
        g.log.info("Volume created successfully : %s", self.volname)

    def tearDown(self):
        # Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup the volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully: %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_default_max_bricks_per_process(self):
        """
        Test Case:
        1) Create a volume and start it.
        2) Fetch the max bricks per process value
        3) Reset the volume options
        4) Fetch the max bricks per process value
        5) Compare the value fetched in last step with the initial value
        6) Enable brick-multiplexing in the cluster
        7) Fetch the max bricks per process value
        8) Compare the value fetched in last step with the initial value
        """
        # Fetch the max bricks per process value
        ret = get_volume_options(self.mnode, 'all')
        self.assertIsNotNone(ret, "Failed to execute the volume get command")
        initial_value = ret['cluster.max-bricks-per-process']
        g.log.info("Successfully fetched the max bricks per-process value")

        # Reset the volume options
        ret, _, _ = reset_volume_option(self.mnode, 'all', 'all')
        self.assertEqual(ret, 0, "Failed to reset the volumes")
        g.log.info("Volumes reset was successful")

        # Fetch the max bricks per process value
        ret = get_volume_options(self.mnode, 'all')
        self.assertIsNotNone(ret, "Failed to execute the volume get command")

        # Comparing the values
        second_value = ret['cluster.max-bricks-per-process']
        self.assertEqual(initial_value, second_value, "Unexpected: Max"
                         " bricks per-process value is not equal")

        # Enable brick-multiplex in the cluster
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.brick-multiplex': 'enable'})
        self.assertTrue(ret, "Failed to enable brick-multiplex"
                        " for the cluster")
        g.log.info("Successfully enabled brick-multiplex in the cluster")

        # Fetch the max bricks per process value
        ret = get_volume_options(self.mnode, 'all')
        self.assertIsNotNone(ret, "Failed to execute the volume get command")

        # Comparing the values
        third_value = ret['cluster.max-bricks-per-process']
        self.assertEqual(initial_value, third_value, "Unexpected: Max bricks"
                         " per-process value is not equal")
