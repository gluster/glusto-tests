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
    Test brick status when quorum isn't met after glusterd restart.
"""


from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import (
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.gluster_init import (
    stop_glusterd,
    start_glusterd,
    restart_glusterd,
    wait_for_glusterd_to_start)
from glustolibs.gluster.brick_libs import (
    are_bricks_offline,
    get_all_bricks)


@runs_on([['distributed', 'distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'replicated', 'dispersed', 'arbiter'],
          ['glusterfs']])
class TestBrickStatusQuorumNotMet(GlusterBaseClass):
    def setUp(self):
        """
        setUp method for every test.
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup and mount the volume.
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume and mount it.")

    def test_offline_brick_status_when_quorum_not_met(self):
        """
        Test Brick status when Quorum is not met after glusterd restart.
        1. Create a volume and mount it.
        2. Set the quorum type to 'server'.
        3. Bring some nodes down such that quorum won't be met.
        4. Brick status should be offline in the node which is up.
        5. Restart glusterd in this node.
        6. The brick status still should be offline as quorum isn't met.
        """
        # Set the quorum type to server and validate it.
        vol_option = {'cluster.server-quorum-type': 'server'}
        ret = set_volume_options(self.mnode, self.volname, vol_option)
        self.assertTrue(ret, "gluster volume option set of %s to %s failed"
                        % ('cluster.server-quorum-type', 'server'))
        g.log.info("Cluster quorum set to type server.")

        # Get the brick list.
        brick_list = get_all_bricks(self.mnode, self.volname)

        # Stop glusterd processes.
        ret = stop_glusterd(self.servers[1:])
        self.assertTrue(ret, "Failed to stop glusterd on specified nodes.")
        g.log.info("Glusterd processes stopped in the desired servers.")

        # Get the brick status in a node where glusterd is up.
        ret = are_bricks_offline(self.mnode, self.volname, brick_list[0:1])
        self.assertTrue(ret, "Bricks are online")
        g.log.info("Bricks are offline as expected.")

        # Restart one of the node which is up.
        ret = restart_glusterd(self.servers[0])
        self.assertTrue(ret, ("Failed to restart glusterd on desired node."))
        g.log.info("Glusterd restarted on the desired node.")

        # Wait for glusterd to be online and validate it's running.
        self.assertTrue(wait_for_glusterd_to_start(self.servers[0]),
                        "Glusterd not up on the desired server.")
        g.log.info("Glusterd is up in the desired server.")

        # Get the brick status from the restarted node.
        ret = are_bricks_offline(self.mnode, self.volname, brick_list[0:1])
        self.assertTrue(ret, "Bricks are online")
        g.log.info("Bricks are offline as expected.")

        # Start glusterd on all servers.
        ret = start_glusterd(self.servers)
        self.assertTrue(ret, "Failed to start glusterd on the specified nodes")
        g.log.info("Initiated start of glusterd on all nodes.")

        # Wait for glusterd to start.
        ret = wait_for_glusterd_to_start(self.servers)
        self.assertTrue(ret, "Glusterd not up on all nodes.")
        g.log.info("Glusterd is up and running on all nodes.")

        # Wait for all volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname,
                                                   timeout=600)
        self.assertTrue(ret, ("All volume processes not up."))
        g.log.info("All volume processes are up.")

    def tearDown(self):
        """tear Down callback"""
        # unmount volume and cleanup.
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to unmount and cleanup volume")
        g.log.info("Successful in unmount and cleanup operations")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
