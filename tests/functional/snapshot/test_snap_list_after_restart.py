#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.gluster_init import (
    wait_for_glusterd_to_start,
    restart_glusterd)
from glustolibs.gluster.peer_ops import is_peer_connected
from glustolibs.gluster.snap_ops import (
    snap_create,
    snap_delete,
    snap_delete_all,
    get_snap_list)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class TestSnapshotListGlusterdRestart(GlusterBaseClass):
    """
    Test Cases in this module tests the snapshot listing
    before and after glusterd restart.
    """
    def setUp(self):
        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)
        self.snapshots = [('snap-test-snap-list-gd-restart-%s-%s'
                           % (self.volname, i))for i in range(0, 3)]

    def tearDown(self):

        self.get_super_method(self, 'tearDown')()

        # Delete snapshots created in the test case
        ret, _, _ = snap_delete_all(self.mnode)
        if ret:
            raise ExecutionError("Failed to delete the snapshots")
        g.log.info("Successfully deleted all snapshots")

        # Unmount and cleanup volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup Volume")
        g.log.info("Successful in Cleanup volume")

    def test_snap_list_glusterd_restart(self):
        """
        Verify snapshot list before and after glusterd restart

        * Create 3 snapshots of the volume
        * Delete one snapshot
        * List all snapshots created
        * Restart glusterd on all nodes
        * List all snapshots
          All snapshots must be listed except the one that was deleted
        """

        # pylint: disable=too-many-statements
        # Create snapshots
        for snap in self.snapshots:
            ret, _, _ = snap_create(self.mnode, self.volname, snap)
            self.assertEqual(ret, 0, ("Failed to create snapshot %s for "
                                      "volume %s" % (snap, self.volname)))
            g.log.info("Snapshot %s created successfully "
                       "for volume %s", snap, self.volname)

        # List the snapshots and validate with snapname
        snap_list = get_snap_list(self.mnode)
        self.assertIsNotNone(snap_list, "Failed to list all snapshots")
        self.assertEqual(len(snap_list), 3, "Failed to validate snap list")
        g.log.info("Successfully validated snap list")
        for snap in self.snapshots:
            self.assertIn(snap, snap_list, "Failed to validate the snapshot "
                          "%s in the snapshot list" % snap)
        g.log.info("Successfully validated the presence of snapshots using "
                   "snapname")

        # Delete one snapshot
        ret, _, _ = snap_delete(self.mnode, self.snapshots[0])
        self.assertEqual(ret, 0, ("Failed to delete snapshot %s"
                                  % self.snapshots[0]))
        g.log.info("Snapshots %s deleted Successfully", self.snapshots[0])

        # List the snapshots and validate with snapname
        snap_list = get_snap_list(self.mnode)
        self.assertIsNotNone(snap_list, "Failed to list all snapshots")
        self.assertEqual(len(snap_list), 2, "Failed to validate snap list")
        g.log.info("Successfully validated snap list")
        for snap in self.snapshots[1:]:
            self.assertIn(snap, snap_list, "Failed to validate the snapshot "
                          "%s in the snapshot list" % snap)
        g.log.info("Successfully validated the presence of snapshots using "
                   "snapname")

        # Restart glusterd on all the servers
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, ("Failed to restart glusterd on nodes %s"
                              % self.servers))
        g.log.info("Successfully restarted glusterd on nodes %s", self.servers)

        # Wait for glusterd to be online and validate glusterd running on all
        # server nodes
        self.assertTrue(
            wait_for_glusterd_to_start(self.servers),
            "Unexpected: glusterd not up on one or more of the nodes")
        g.log.info("Glusterd is up and running on all nodes")

        # Check if peers are connected
        self.assertTrue(
            is_peer_connected(self.mnode, self.servers),
            "Unexpected: Peers are not in connected state")
        g.log.info("Successful: All peers are in connected state")

        # List the snapshots after glusterd restart
        # All snapshots must be listed except the one deleted
        for server in self.servers:
            snap_list = get_snap_list(server)
            self.assertIsNotNone(
                snap_list, "Failed to get the list of snapshots in node %s"
                % server)
            self.assertEqual(
                len(snap_list), 2,
                "Unexpected: Number of snapshots not consistent in the node %s"
                % server)
            g.log.info("Successfully validated snap list for node %s", server)
            for snap in self.snapshots[1:]:
                self.assertIn(
                    snap, snap_list, "Failed to validate the snapshot "
                    "%s in the snapshot list" % snap)
            g.log.info("Successfully validated the presence of snapshots "
                       "using snapname for node %s", server)
