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
    Test Default volume behavior and quorum options
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (
    get_volume_options,
    volume_reset)
from glustolibs.gluster.gluster_init import (
    stop_glusterd,
    start_glusterd,
    is_glusterd_running,
    wait_for_glusterd_to_start)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.brickmux_ops import get_brick_processes_count
from glustolibs.gluster.peer_ops import wait_for_peers_to_connect


@runs_on([['replicated', 'arbiter', 'dispersed', 'distributed',
           'distributed-replicated', 'distributed-arbiter'],
          ['glusterfs']])
class TestGlusterDDefaultVolumeBehaviorQuorumOptions(GlusterBaseClass):
    """ Testing default volume behavior and Quorum options """

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup and mount the volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume and mount it")

    def _validate_vol_options(self, option_name, option_value, for_all=False):
        """ Function to validate default vol options """
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

    def _get_total_brick_processes_count(self):
        """
        Function to find the total number of brick processes in the cluster
        """
        count = 0
        self.brick_list = get_all_bricks(self.mnode, self.volname)
        for brick in self.brick_list:
            server = brick.split(":")[0]
            count += get_brick_processes_count(server)
        return count

    def test_glusterd_default_vol_behavior_and_quorum_options(self):
        """
        Test default volume behavior and quorum options
        1. Create a volume and start it.
        2. Check that no quorum options are found in vol info.
        3. Kill two glusterd processes.
        4. There shouldn't be any effect to the running glusterfsd
        processes.
        """
        # Check that quorum options are not set by default.
        self._validate_vol_options('cluster.server-quorum-type', 'off')
        self._validate_vol_options('cluster.server-quorum-ratio',
                                   '51 (DEFAULT)', True)

        # Get the count of number of glusterfsd processes running.
        count_before_glusterd_kill = self._get_total_brick_processes_count()

        # Kill two glusterd processes.
        server_list = [self.servers[1], self.servers[2]]
        ret = stop_glusterd(server_list)
        self.assertTrue(ret, "Failed to stop glusterd on the specified nodes.")
        ret = is_glusterd_running(server_list)
        self.assertNotEqual(ret, 0, ("Glusterd is not stopped on the servers"
                                     " where it was desired to be stopped."))
        g.log.info("Glusterd processes stopped in the desired servers.")

        # Get the count of number of glusterfsd processes running.
        count_after_glusterd_kill = self._get_total_brick_processes_count()

        # The count of glusterfsd processes should match
        self.assertEqual(count_before_glusterd_kill, count_after_glusterd_kill,
                         ("Glusterfsd processes are affected."))
        g.log.info("Glusterd processes are not affected.")

        # Start glusterd on all servers.
        ret = start_glusterd(self.servers)
        self.assertTrue(ret, "Failed to Start glusterd on the specified"
                             " nodes")
        g.log.info("Started glusterd on all nodes.")

        # Wait for glusterd to restart.
        ret = wait_for_glusterd_to_start(self.servers)
        self.assertTrue(ret, "Glusterd not up on all nodes.")
        g.log.info("Glusterd is up and running on all nodes.")

    def tearDown(self):
        """tear Down Callback"""
        # Wait for peers to connect.
        ret = wait_for_peers_to_connect(self.mnode, self.servers, 50)
        if not ret:
            raise ExecutionError("Peers are not in connected state.")

        # Unmount volume and cleanup.
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup volume")
        g.log.info("Successful in unmount and cleanup operations")

        # Reset the cluster options.
        ret = volume_reset(self.mnode, "all")
        if not ret:
            raise ExecutionError("Failed to Reset the cluster options.")
        g.log.info("Successfully reset cluster options.")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
