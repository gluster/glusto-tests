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

"""
    Test Description:
    Tests to check the 'options' file is updated with quorum changes
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed',
           'arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestUpdatesInOptionsFileOnQuorumChanges(GlusterBaseClass):
    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setting up Volume
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation/start failed: %s"
                                 % self.volname)
        g.log.info("Volme createdand started successfully : %s",
                   self.volname)

    def tearDown(self):
        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_updates_in_options_file_on_quorum_changes(self):
        """
        Test Case:
        1. Create and start a volume
        2. Check the output of '/var/lib/glusterd/options' file
        3. Store the value of 'global-option-version'
        4. Set server-quorum-ratio to 70%
        5. Check the output of '/var/lib/glusterd/options' file
        6. Compare the value of 'global-option-version' and check
           if the value of 'server-quorum-ratio' is set to 70%
        """
        # Checking 'options' file for quorum related entries
        cmd = "cat /var/lib/glusterd/options | grep global-option-version"
        ret, out, _ = g.run(self.mnode, cmd)
        previous_global_option_version = out.split('=')

        # Setting Quorum ratio in percentage
        self.quorum_perecent = {'cluster.server-quorum-ratio': '70%'}
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertTrue(ret, "Failed to set cluster.server-quorum-ratio"
                        " option on volumes")
        g.log.info("Successfully set cluster.server-quorum-ratio on cluster")

        # Checking 'options' file for quorum related entries
        cmd = "cat /var/lib/glusterd/options | grep global-option-version"
        ret, out, _ = g.run(self.mnode, cmd)
        new_global_option_version = out.split('=')
        self.assertEqual(int(previous_global_option_version[1]) + 1,
                         int(new_global_option_version[1]),
                         "Failed:The global-option-version didn't change on a"
                         " volume set operation")
        g.log.info("The global-option-version was successfully updated in the"
                   " options file")

        cmd = "cat /var/lib/glusterd/options | grep server-quorum-ratio"
        ret, out, _ = g.run(self.mnode, cmd)
        out = out.split("%")
        self.assertEqual(out[0], "cluster.server-quorum-ratio=70",
                         "Server-quorum-ratio is not updated in options file")
        g.log.info("The cluster.server-quorum-ratio was successfully set"
                   " to 70 in the options  file")
