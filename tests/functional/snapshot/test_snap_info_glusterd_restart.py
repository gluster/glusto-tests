#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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
snapshot information after glusterd
is restarted.

"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.gluster_init import (restart_glusterd,
                                             is_glusterd_running)
from glustolibs.gluster.snap_ops import (snap_create,
                                         get_snap_info,
                                         get_snap_info_by_volname,
                                         get_snap_info_by_snapname)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class SnapshotInfo(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        cls.snap1 = "snap1"
        cls.snap2 = "snap2"

    def setUp(self):

        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def snapshot_info(self):
        # Check snapshot info using snap name
        g.log.info("Checking snapshot info using snap name")
        snap_info_chk = get_snap_info_by_snapname(self.mnode,
                                                  self.snap1)
        self.assertIsNotNone(snap_info_chk, "Failed to get snap info")
        self.assertEqual(snap_info_chk['name'], "%s" % self.snap1,
                         "Failed to show snapshot info for %s"
                         % self.snap1)
        g.log.info("Successfully checked snapshot info for %s", self.snap1)

        # Check snapshot info using volname
        g.log.info("Checking snapshot info using volname")
        snap_vol_info = get_snap_info_by_volname(self.mnode, self.volname)
        self.assertIsNotNone(snap_vol_info, "Failed to get snap info")
        self.assertEqual(snap_vol_info['originVolume']['name'], "%s"
                         % self.volname,
                         "Failed to show snapshot info for %s"
                         % self.volname)
        g.log.info("Successfully checked snapshot info for %s",
                   self.volname)

        # Validate snapshot information
        g.log.info("Validating snapshot information")
        info_snaps = get_snap_info(self.mnode)
        self.assertIsNotNone(snap_vol_info, "Failed to get snap info")
        for snap in range(0, 2):
            self.assertEqual(info_snaps[snap]['name'], "snap%s" % snap,
                             "Failed to validate"
                             "snap information")
        g.log.info("Successfully Validated snap Information")

    def test_snap_info(self):
        """
        1. Create volumes
        2. create multiple snapshots
        3. Check snapshot info for snapshots created
           using snap name, using volume name and
           without using snap name and volume name
        4. restart glusterd
        5. follow step 3
        """

        # pylint: disable=too-many-statements
        # Creating snapshot with description
        g.log.info("Starting to Create snapshot")
        for count in range(0, 2):
            self.snap = "snap%s" % count
            ret, _, _ = snap_create(self.mnode, self.volname,
                                    self.snap,
                                    description='$p3C!@l C#@R@cT#R$')
            self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                      % self.volname))
        g.log.info("Snapshot %s created successfully"
                   " for volume %s", self.snap, self.volname)
        self.snapshot_info()

        # Restart Glusterd on all node
        g.log.info("Restarting Glusterd on all node")
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, "Failed to stop glusterd")
        g.log.info("Successfully stopped glusterd on all node")

        # Check Glusterd status
        g.log.info("Check glusterd running or not")
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, "glusterd running on node ")
        g.log.info("glusterd is not running")

        self.snapshot_info()

    def tearDown(self):

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")
