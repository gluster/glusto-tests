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
snapshot Status and Info for Invalid cases.

"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_status, snap_info)


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'distributed-dispersed', 'dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class SnapshotstatusInvalidcases(GlusterBaseClass):
    """
    1. Create volumes
    2. create multiple snapshots
    3. Show status of non existing snapshots
    4. Show info of non existing snapshots
    5. Show status of snaps of non-existing volumes
    6. Show info of snaps of non-existing volume
    7. status of snapshots with invalid command
    8. Info of snapshots with invalid command
    """

    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)
        cls.snap5 = "snap5"
        cls.snap1 = "snap1"
        cls.volname1 = "volume1"

    def setUp(self):

        # SettingUp volume and Mounting the volume
        GlusterBaseClass.setUpClass.im_func(self)
        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_snap_invalid_case(self):

        # Creating snapshot:
        g.log.info("Starting to Create snapshot")
        for count in range(1, 3):
            self.snap = "snap%s" % count
            ret = snap_create(self.mnode, self.volname, self.snap)
            self.assertTrue(ret, ("Failed to create snapshot for volume %s"
                                  % self.volname))
        g.log.info("Snapshot %s created successfully"
                   " for volume %s", self.snap, self.volname)

        # Check snapshot info for non-existing snapshot
        g.log.info("Checking snapshot info")
        ret, _, _ = snap_info(self.mnode, self.snap5)
        self.assertEqual(ret, 1, "Unexpected: Successful in "
                         "getting information for"
                         "non-existing %s snapshot" % self.snap5)
        g.log.info("Expected result: failed to get information"
                   " for non-existing %s snapshot", self.snap5)

        # Check snapshot status for non-existing snapshot
        g.log.info("Checking snapshot status")
        ret, _, _ = snap_status(self.mnode, self.snap5)
        self.assertEqual(ret, 1, "Unexpected: Successful in getting "
                         "status for non-existing "
                         "%s snapshot" % self.snap5)
        g.log.info("Expected result: failed to get status"
                   " for non-existing %s snapshot", self.snap5)

        # Check snapshot info for non-existing volume
        g.log.info("Checking snapshot info")
        ret, _, _ = snap_info(self.mnode, self.volname1)
        self.assertEqual(ret, 1, "Unexpected: Successful in getting "
                         "information for"
                         "non-existing %s volume" % self.volname1)
        g.log.info("Expected result: failed to get information"
                   " for non-existing %s volume", self.volname1)

        # Check snapshot status for non-existing volume
        g.log.info("Checking snapshot status")
        ret, _, _ = snap_info(self.mnode, self.volname1)
        self.assertEqual(ret, 1, "Unexpected: Successful in getting "
                         "status for non-existing "
                         "%s volume" % self.volname1)
        g.log.info("Expected result: Need to fail to get status"
                   " for non-existing %s volume", self.volname1)

        # Invalid command
        g.log.info("Passing invalid status command")
        cmd = "gluster snapshot snap1 status"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 1, "Unexpected: Snapshot status"
                         " command Successful even with Invalid"
                         " command")
        g.log.info("Expected result: snapshot status command failed")

        # Invalid command
        g.log.info("Passing invalid info command")
        cmd = "gluster snapshot snap1 info"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 1, "Unexpected: Snapshot info "
                         "command Successful even with Invalid "
                         " command")
        g.log.info("Expected result: snapshot information command Failed")

    def tearDown(self):

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")
