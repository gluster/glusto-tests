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
"""Test Description:
    Consistent info display while starting and stopping glusterd.
"""

from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_init import wait_for_glusterd_to_start


class TestGlusterdStartStopOutput(GlusterBaseClass):
    # It might so happen that glusterd is restarted in previous tests and
    # so reset the count before and after the test
    def setUp(self):
        self.get_super_method(self, 'setUp')()
        self.glusterd_start_cmd = 'systemctl start glusterd'
        self.glusterd_stop_cmd = 'systemctl stop glusterd'
        self._run_command_in_servers('systemctl reset-failed glusterd')

    def tearDown(self):
        self._run_command_in_servers('systemctl reset-failed glusterd')
        self._run_command_in_servers(self.glusterd_start_cmd)
        self.get_super_method(self, 'tearDown')()

    def _run_command_in_servers(self, cmd):
        """
        Function to run a given command in the servers and validate if they
        are a success and then return back the result of any one server.
        """
        ret, std_out, _ = g.run(self.servers[0], cmd)
        self.assertEqual(ret, 0, "Failed to execute %s the server." % (cmd))
        return std_out

    def test_glusterd_start_stop_consistent_info(self):
        """
        Test Case:
        1. Stop and Start glusterd and store the output returned.
        2. Check if consistent output is returned if glusterd is started and
        stopped 5 times.
        """
        # Stop glusterd to get the info for further comparison.
        stop_output = self._run_command_in_servers(self.glusterd_stop_cmd)

        # Start glusterd to get the info for further comparison.
        start_output = self._run_command_in_servers(self.glusterd_start_cmd)

        # Loop over the stop-stop cycle for 5 times and validate the output.
        for _ in range(5):
            # Validate stop output is the same.
            ret_value = self._run_command_in_servers(self.glusterd_stop_cmd)
            self.assertEqual(
                ret_value, stop_output,
                "%s is not consistent." % (self.glusterd_stop_cmd))
            sleep(2)

            # Validate start output is the same.
            ret_value = self._run_command_in_servers(self.glusterd_start_cmd)
            self.assertEqual(
                ret_value, start_output,
                "%s is not consistent." % (self.glusterd_start_cmd))
            ret = wait_for_glusterd_to_start(self.servers[0])
            self.assertTrue(ret, "Glusterd has not yet started in servers.")
