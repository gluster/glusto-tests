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
      Test to check that default log level of CLI should be INFO
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_start, volume_status,
                                           volume_info, volume_stop)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'], ['glusterfs']])
class TestDefaultLogLevelOfCLI(GlusterBaseClass):
    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Creating and starting the volume
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation/start operation"
                                 " failed: %s" % self.volname)
        g.log.info("Volme created and started successfully : %s", self.volname)

    def tearDown(self):
        # Stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_default_log_level_of_cli(self):
        """
        Test Case:
        1) Create and start a volume
        2) Run volume info command
        3) Run volume status command
        4) Run volume stop command
        5) Run volume start command
        6) Check the default log level of cli.log
        """
        # Check volume info operation
        ret, _, _ = volume_info(self.mnode)
        self.assertEqual(ret, 0, "Failed to execute volume info"
                         " command on node: %s" % self.mnode)
        g.log.info("Successfully executed the volume info command on"
                   " node: %s", self.mnode)

        # Check volume status operation
        ret, _, _ = volume_status(self.mnode)
        self.assertEqual(ret, 0, "Failed to execute volume status command"
                         " on node: %s" % self.mnode)
        g.log.info("Successfully executed the volume status command"
                   " on node: %s", self.mnode)

        # Check volume stop operation
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to stop the volume %s on node: %s"
                         % (self.volname, self.mnode))
        g.log.info("Successfully stopped the volume %s on node: %s",
                   self.volname, self.mnode)

        # Check volume start operation
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start the volume %s on node: %s"
                         % (self.volname, self.mnode))
        g.log.info("Successfully started the volume %s on node: %s",
                   self.volname, self.mnode)

        # Check the default log level of cli.log
        cmd = 'cat /var/log/glusterfs/cli.log | grep -F "] D [" | wc -l'
        ret, out, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to execute the command")
        self.assertEqual(int(out), 0, "Unexpected: Default log level of "
                         "cli.log is not INFO")
        g.log.info("Default log level of cli.log is INFO as expected")
