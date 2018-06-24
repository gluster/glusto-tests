#  Copyright (C) 2017-2018 Red Hat, Inc. <http://www.redhat.com>
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
Description:

Test Cases in this module tests the
snapshot activation and deactivation status
when glusterd is down.
"""
import time
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.peer_ops import is_peer_connected
from glustolibs.gluster.gluster_init import (stop_glusterd,
                                             start_glusterd,
                                             is_glusterd_running)
from glustolibs.gluster.snap_ops import (snap_create,
                                         get_snap_info_by_snapname,
                                         get_snap_list, snap_deactivate,
                                         snap_activate)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs']])
class SnapshotGlusterddown(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)
        cls.snap = "snap1"

    def setUp(self):
        # SettingUp volume and Mounting the volume
        GlusterBaseClass.setUp.im_func(self)
        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_snap_glusterd_down(self):
        # pylint: disable=too-many-statements
        """
        Steps:

        1. create a volume
        2. mount volume
        3. create snapshot of that volume
        4. validate using snapshot info
        5. Activate snapshot
        6. List all snapshots present
        7. validate using snapshot info
        8. Stop glusterd on one node
        9. Check glusterd status
       10. deactivate created snapshot
       11. Start glusterd on that node
       12. Check glusterd status
       13. validate using snapshot info
       13. Check all peers are connected

        """
        # Creating snapshot:
        g.log.info("Starting to Create snapshot")
        ret, _, _ = snap_create(self.mnode, self.volname, self.snap)
        self.assertEqual(ret, 0, ("Failed to create snapshot %s for volume %s"
                                  % (self.snap, self.volname)))
        g.log.info("Snapshot %s created successfully "
                   "for volume %s", self.snap, self.volname)

        # Check snapshot info
        g.log.info("Checking snapshot info")
        snap_info = get_snap_info_by_snapname(self.mnode, self.snap)
        self.assertIsNotNone(snap_info, "Failed to get snap information"
                             "for snapshot %s" % self.snap)
        status = snap_info['snapVolume']['status']
        self.assertNotEqual(status, 'Started', "snapshot %s "
                            "not started" % self.snap)
        g.log.info("Successfully checked snapshot info")

        # Activating snapshot
        g.log.info("Starting to Activate Snapshot")
        ret, _, _ = snap_activate(self.mnode, self.snap)
        self.assertEqual(ret, 0, ("Failed to Activate snapshot %s"
                                  % self.snap))
        g.log.info("Snapshot %s activated successfully", self.snap)

        # snapshot list
        g.log.info("Starting to validate list of snapshots")
        snap_list1 = get_snap_list(self.mnode)
        self.assertIsNotNone(snap_list1, "Failed to list all the snapshot")
        self.assertEqual(len(snap_list1), 1, "Failed to validate snap list")
        g.log.info("Snapshot list successfully validated")

        # Check snapshot info
        g.log.info("Checking snapshot info")
        snap_info = get_snap_info_by_snapname(self.mnode, self.snap)
        status = snap_info['snapVolume']['status']
        self.assertEqual(status, 'Started', "Failed to"
                         "start snapshot info")
        g.log.info("Successfully checked snapshot info")

        # Stop Glusterd on one node
        g.log.info("Stopping Glusterd on one node")
        ret = stop_glusterd(self.servers[1])

        # Check Glusterd status
        g.log.info("Check glusterd running or not")
        count = 0
        while count < 80:
            ret = is_glusterd_running(self.servers[1])
            if ret == 1:
                break
            time.sleep(2)
            count += 2
        self.assertEqual(ret, 1, "Unexpected: glusterd running on node %s" %
                         self.servers[1])
        g.log.info("Expected: Glusterd not running on node %s",
                   self.servers[1])

        # de-activating snapshot
        g.log.info("Starting to de-activate Snapshot")
        ret, _, _ = snap_deactivate(self.mnode, self.snap)
        self.assertEqual(ret, 0, ("Failed to deactivate snapshot %s"
                                  % self.snap))
        g.log.info("Snapshot %s deactivated successfully", self.snap)

        # validate snapshot info
        g.log.info("Checking snapshot info")
        snap_info = get_snap_info_by_snapname(self.mnode, self.snap)
        status = snap_info['snapVolume']['status']
        self.assertNotEqual(status, 'Started', "snapshot %s "
                            "not started" % self.snap)
        g.log.info("Successfully validated snapshot info")

        # Start Glusterd on node
        g.log.info("Starting Glusterd on node %s", self.servers[1])
        ret = start_glusterd(self.servers[1])
        self.assertTrue(ret, "Failed to start glusterd on %s node"
                        % self.servers[1])
        g.log.info("Successfully started glusterd on "
                   "%s node", self.servers[1])

        # Check Glusterd status
        g.log.info("Check glusterd running or not")
        count = 0
        while count < 80:
            ret = is_glusterd_running(self.servers[1])
            if ret:
                break
            time.sleep(2)
            count += 2
        self.assertEqual(ret, 0, "glusterd not running on node %s "
                         % self.servers[1])
        g.log.info("glusterd is running on %s node",
                   self.servers[1])

        # validate snapshot info
        g.log.info("Checking snapshot info")
        snap_info = get_snap_info_by_snapname(self.mnode, self.snap)
        self.assertIsNotNone(snap_info, "Failed to get snap info for"
                             " snapshot %s" % self.snap)
        status = snap_info['snapVolume']['status']
        self.assertNotEqual(status, 'Started', "snapshot"
                            " %s failed to validate with snap info"
                            % self.snap)
        g.log.info("Successfully validated snapshot info")

        # Check all the peers are in connected state
        g.log.info("Validating all the peers are in connected state")
        for servers in self.servers:
            count = 0
            while count < 80:
                ret = is_peer_connected(self.mnode, servers)
                if ret:
                    break
                time.sleep(2)
                count += 2
            self.assertTrue(ret, "All the nodes are not in cluster")
        g.log.info("Successfully validated all the peers")

    def tearDown(self):

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")
