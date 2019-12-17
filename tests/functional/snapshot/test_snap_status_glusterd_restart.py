#  Copyright (C) 2020  Red Hat, Inc. <http://www.redhat.com>
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
snapshot Status when glusterd is restarted.

"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.gluster_init import (restart_glusterd,
                                             is_glusterd_running)
from glustolibs.gluster.snap_ops import (snap_create,
                                         get_snap_status,
                                         get_snap_status_by_snapname,
                                         snap_status_by_volname)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class TestSnapshotGlusterdRestart(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        cls.snapshots = [('snap-test-snap-status-gd-restart-%s-%s'
                          % (cls.volname, i))for i in range(0, 2)]

    def setUp(self):

        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()

        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):

        # Unmount and cleanup original volume

        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

    def test_snap_status_glusterd_restart(self):
        # pylint: disable=too-many-statements, too-many-branches
        """
        Test Case:
        1. Create volume
        2. Create two snapshots with description
        3. Check snapshot status informations with snapname, volume name and
           without snap name/volname.
        4. Restart glusterd on all nodes
        5. Follow step3 again and validate snapshot
        """

        # Creating snapshot with description
        for snap in self.snapshots:
            ret, _, _ = snap_create(self.mnode, self.volname, snap,
                                    description='$p3C!@l C#@R@cT#R$')
            self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                      % self.volname))
            g.log.info("Snapshot %s created successfully"
                       " for volume %s", snap, self.volname)

        # Validate snapshot status information
        # Check snapshot status
        snap_stat = get_snap_status(self.mnode)
        self.assertIsNotNone(snap_stat, "failed to get snap status")
        snap_count = 0
        for snap in self.snapshots:
            self.assertEqual(snap_stat[snap_count]['name'],
                             snap, "Failed to show snapshot status")
            snap_count += 1
        g.log.info("Successfully checked snapshot status")

        # Check snapshot status using snap name
        snap_status = get_snap_status_by_snapname(self.mnode,
                                                  self.snapshots[0])
        self.assertIsNotNone(snap_status, "failed to get snap status")
        self.assertEqual(snap_status['name'], "%s" % self.snapshots[0],
                         "Failed to show snapshot "
                         "status for %s" % self.snapshots[0])
        g.log.info("Successfully checked snapshot status for %s",
                   self.snapshots[0])

        # Check snapshot status using volname
        ret, snap_vol_status, _ = snap_status_by_volname(self.mnode,
                                                         self.volname)
        self.assertEqual(ret, 0, ("Failed to get snapshot statue "
                                  "by volume name"))
        self.assertIsNotNone(snap_vol_status, "failed to get snap status")
        for snap in self.snapshots:
            self.assertIn(snap, snap_vol_status,
                          "Failed to validate snapshot name")
        g.log.info("Successfully validated snapshot status for %s",
                   self.volname)

        # Restart Glusterd on all node
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, "Failed to stop glusterd")
        g.log.info("Successfully stopped glusterd on all node")

        # Check Glusterd status
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, "glusterd running on node ")
        g.log.info("glusterd is not running")

        # Validate snapshot status information
        # Check snapshot status
        snap_stat = get_snap_status(self.mnode)
        self.assertIsNotNone(snap_stat, "failed to get snap status")
        snap_count = 0
        for snap in self.snapshots:
            self.assertEqual(snap_stat[snap_count]['name'],
                             snap, "Failed to show snapshot status")
            snap_count += 1
        g.log.info("Successfully checked snapshot status")

        # Check snapshot status using snap name
        snap_status = get_snap_status_by_snapname(self.mnode,
                                                  self.snapshots[0])
        self.assertIsNotNone(snap_status, "failed to get snap status")
        self.assertEqual(snap_status['name'], "%s" % self.snapshots[0],
                         "Failed to show snapshot "
                         "status for %s" % self.snapshots[0])
        g.log.info("Successfully checked snapshot status for %s",
                   self.snapshots[0])

        # Check snapshot status using volname
        ret, snap_vol_status, _ = snap_status_by_volname(self.mnode,
                                                         self.volname)
        self.assertEqual(ret, 0, ("Failed to get snapshot statue "
                                  "by volume name"))
        self.assertIsNotNone(snap_vol_status, "failed to get snap status")
        for snap in self.snapshots:
            self.assertIn(snap, snap_vol_status,
                          "Failed to validate snapshot status "
                          "using volume name")
        g.log.info("Successfully validated snapshot status for %s",
                   self.volname)
