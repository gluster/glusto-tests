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
    Test quorum cli commands in glusterd
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (
    set_volume_options,
    get_volume_options)


@runs_on([['replicated', 'arbiter', 'dispersed', 'distributed',
           'distributed-replicated', 'distributed-arbiter'],
          ['glusterfs']])
class TestGlusterDQuorumCLICommands(GlusterBaseClass):
    """ Testing Quorum CLI commands in GlusterD """

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup and mount the volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume and mount it")

    def set_and_check_vol_option(self, option_name, option_value,
                                 for_all=False):
        """ Function for setting and checking volume_options """
        # Set the volume option
        vol_option = {option_name: option_value}
        if not for_all:
            ret = set_volume_options(self.mnode, self.volname, vol_option)
        else:
            ret = set_volume_options(self.mnode, 'all', vol_option)
        self.assertTrue(ret, "gluster volume option set of %s to %s failed"
                        % (option_name, option_value))

        # Validate the option set
        if not for_all:
            ret = get_volume_options(self.mnode, self.volname, option_name)
        else:
            ret = get_volume_options(self.mnode, 'all', option_name)
        self.assertIsNotNone(ret, "The %s option is not present" % option_name)
        self.assertEqual(ret[option_name], option_value,
                         ("Volume option for %s is not equal to %s"
                          % (option_name, option_value)))
        g.log.info("Volume option %s is equal to the expected value %s",
                   option_name, option_value)

    def test_glusterd_quorum_cli_commands(self):
        """
        Test quorum CLI commands on glusterd
        1. Create a volume and start it.
        2. Set the quorum type to 'server' and verify it.
        3. Set the quorum type to 'none' and verify it.
        4. Set the quorum ratio and verify it.
        """
        # Set server quorum type to 'server' and validate it
        self.set_and_check_vol_option('cluster.server-quorum-type', 'server')

        # Set server quorum type to 'none' and validate it
        self.set_and_check_vol_option('cluster.server-quorum-type', 'none')

        # Set server quorum ratio to 90% and validate it
        self.set_and_check_vol_option('cluster.server-quorum-ratio', '90%',
                                      True)

    def tearDown(self):
        """tear Down Callback"""
        # Unmount volume and cleanup.
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to unmount and cleanup volume")
        g.log.info("Successful in unmount and cleanup of volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
