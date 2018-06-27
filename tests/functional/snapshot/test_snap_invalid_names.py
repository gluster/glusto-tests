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
snapshot creation and listing Invalid names
and parameters.

"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.snap_ops import snap_create, get_snap_list


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs']])
class SnapshotInvalidNames(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)
        cls.snap1 = "snap1"
        cls.snapinvalid = "#64^@*)"
        cls.volname1 = "vola1"

    def setUp(self):
        # SettingUp volume and Mounting the volume
        GlusterBaseClass.setUp.im_func(self)
        g.log.info("Starting to SetUp and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):
        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

        # Unmount and cleanup-volume
        g.log.info("Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Cleanup Volume Successfully")

    def test_snap_list_invalid_cases_names(self):
        """
        Steps:
        1. create volume and mount it
        2. create snapshot with invalid snap name
           should fail
        3. create snapshot
        4. snapshot list Invalid command should fail
        5. snapshot list Invalid parameters with multiple
           and non-existing volume name should fail
        """
        # Creating snapshot with invalid snap name
        g.log.info("Creating snapshot with invalid snap name")
        ret, _, _ = snap_create(self.mnode, self.volname, self.snapinvalid)
        self.assertNotEqual(ret, 0, ("Unexpected: Snapshot %s created "
                                     "successfully for volume %s with "
                                     "invalid snap name"
                                     % (self.snapinvalid, self.volname)))
        g.log.info("Expected: Failed to create snapshot %s for volume %s"
                   "with invalid snap name", self.snap1, self.volname)

        # Creating snapshot
        g.log.info("Starting to Create snapshot")
        ret, _, _ = snap_create(self.mnode, self.volname, self.snap1)
        self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s",
                                  self.volname))
        g.log.info("Snapshot %s created "
                   "successfully for volume"
                   "%s", self.snapinvalid, self.volname)

        # validate snapshot list with volname
        g.log.info("validate snapshot list with volname")
        out = get_snap_list(self.mnode)
        self.assertIsNotNone(out, "Failed to list all snapshots")
        self.assertEqual(len(out), 1, "Failed to validate snap_list")
        g.log.info("Successfully validated snapshot list")

        # listing snapshot with invalid volume name which should fail
        g.log.info("snapshot list with invalid volume name should fail")
        cmd = ("gluster snap list %s" % self.volname1)
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertNotEqual(ret, 0, "Unexpected: Successfully listed "
                            "all snapshots with invalid volume name "
                            "%s" % self.volname1)
        g.log.info("Expected to fail listing the snapshot with invalid"
                   "volume name %s", self.volname1)

        # snapshot list with multiple and non-existing volume
        g.log.info("snapshot list Invalid parameter with "
                   "multiple and non-existing volume name should fail")
        cmd = ("gluster snap list %s %s"
               % (self.volname, self.volname1))
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertNotEqual(ret, 0, "Unexpected: listed all snapshots")
        g.log.info("Expected: Failed to list snapshots")
