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
from glustolibs.gluster.peer_ops import wait_for_peers_to_connect
from glustolibs.gluster.snap_ops import (
    snap_create,
    get_snap_info,
    get_snap_info_by_volname,
    get_snap_info_by_snapname)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class TestSnapshotInfoGlusterdRestart(GlusterBaseClass):
    """
    Test Cases in this module tests the snapshot information
    after glusterd is restarted.
    """

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # SettingUp volume and Mounting the volume
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)
        self.snapshots = [('snap-test-snap-info-gd-restart-%s-%s'
                           % (self.volname, i))for i in range(0, 2)]

    def snapshot_info(self):
        """
        This section checks the snapshot information:
        * Using snapname
        * Using volname
        * Without using snapname or volname
        """
        # Check snapshot info using snapname
        for snap in self.snapshots:
            snap_info_chk = get_snap_info_by_snapname(self.mnode, snap)
            self.assertIsNotNone(snap_info_chk, "Failed to get snap info")
            self.assertEqual(snap_info_chk['name'], "%s" % snap,
                             "Failed to show snapshot info for %s" % snap)
            g.log.info("Successfully validated snapshot info for %s", snap)

        # Check snapshot info using volname
        snap_vol_info = get_snap_info_by_volname(self.mnode, self.volname)
        self.assertIsNotNone(snap_vol_info, "Failed to get snap info")
        self.assertEqual(snap_vol_info['originVolume']['name'],
                         "%s" % self.volname,
                         "Failed to show snapshot info for %s" % self.volname)
        g.log.info("Successfully validated snapshot info for %s", self.volname)

        # Validate snapshot information without using snapname or volname
        info_snaps = get_snap_info(self.mnode)
        self.assertIsNotNone(info_snaps, "Failed to get snap info")
        counter = 0
        for snap in self.snapshots:
            self.assertEqual(info_snaps[counter]['name'], snap,
                             "Failed to validate snap information")
            counter += 1
        g.log.info("Successfully validated snapshot information")

    def test_snap_info_glusterd_restart(self):
        """
        Verify snapshot info before and after glusterd restart

        * Create multiple snapshots
        * Check snapshot info
          - Without using snapname or volname
          - Using snapname
          - Using volname
        * Restart glusterd on all servers
        * Repeat the snapshot info step for all the three scenarios
          mentioned above
        """

        # pylint: disable=too-many-statements
        # Create snapshots with description
        for snap in self.snapshots:
            ret, _, _ = snap_create(self.mnode, self.volname, snap,
                                    description='$p3C!@l C#@R@cT#R$')
            self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                      % self.volname))
            g.log.info("Snapshot %s created successfully for volume %s",
                       snap, self.volname)

        # Perform the snapshot info tests before glusterd restart
        self.snapshot_info()

        # Restart Glusterd on all servers
        for server in self.servers:
            ret = restart_glusterd(server)
            self.assertTrue(ret, ("Failed to restart glusterd on node %s"
                                  % server))
            g.log.info("Successfully restarted glusterd on node %s", server)

        # Wait for glusterd to be online and validate glusterd running on all
        # server nodes
        self.assertTrue(
            wait_for_glusterd_to_start(self.servers),
            "Unexpected: glusterd not up on one or more of the nodes")
        g.log.info("Glusterd is up and running on all nodes")

        # Check if peers are connected
        self.assertTrue(
            wait_for_peers_to_connect(self.mnode, self.servers),
            "Unexpected: Peers are not in connected state")
        g.log.info("Successful: All peers are in connected state")

        # perform the snapshot info tests after glusterd restart
        self.snapshot_info()

    def tearDown(self):
        self.get_super_method(self, 'tearDown')()

        # Unmount and cleanup volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup Volume")
        g.log.info("Successful in Cleanup volume")
