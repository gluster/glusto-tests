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
snapshot deletion with snapname, with volumename
and delete all snapshot commands.

"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.snap_ops import (snap_create, snap_delete,
                                         snap_delete_by_volumename,
                                         snap_delete_all)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs']])
class SnapshotDeleteSnapVolume(GlusterBaseClass):

    def setUp(self):
        # SettingUp volume and Mounting the volume
        GlusterBaseClass.setUp.im_func(self)
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

    def test_snap_delete_snap_volume(self):
        """
        Steps:
        1. create a volume and mount it
        2. create 9 snapshots of volume
        3. delete snapshot with snapname
        4. delete other snapshots with volume name
        5. create one more snapshot
        6. delete created snapshot with snap delete command
        """

        # Creating snapshot
        g.log.info("Starting to Create snapshot")
        for snap_count in range(0, 9):
            self.snap = "snap%s" % snap_count
            ret, _, _ = snap_create(self.mnode, self.volname, self.snap)
            self.assertEqual(ret, 0, ("Failed to create snapshot %s "
                                      "for volume %s"
                                      % (self.snap, self.volname)))
            g.log.info("Snapshot %s created successfully"
                       " for volume %s", self.snap, self.volname)

        # deleting snapshot with snap name
        g.log.info("Starting to delete snapshot with snap name")
        for snap_count in range(0, 3):
            self.snap = "snap%s" % snap_count
            ret, _, _ = snap_delete(self.mnode, self.snap)
            self.assertEqual(ret, 0, ("Failed to delete snapshot snap%s"
                                      % snap_count))
            g.log.info("Snapshot snap%s deleted "
                       "successfully", snap_count)

        # delete all snapshot of volume
        g.log.info("Starting to delete snapshot with volume name")
        ret, _, _ = snap_delete_by_volumename(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to delete snapshot snap%s"
                                  % snap_count))
        g.log.info("Snapshot deleted successfully for volume "
                   "%s", self.volname)

        # create a new snapshot
        g.log.info("Creating a new snapshot")
        self.snap1 = "snapy"
        ret, _, _ = snap_create(self.mnode, self.volname, self.snap1)
        self.assertEqual(ret, 0, ("Failed to create snapshot %s for volume "
                                  "%s" % (self.snap1, self.volname)))
        g.log.info("Snapshot %s created successfully"
                   " for volume %s", self.snap1, self.volname)

        # delete all snapshot created
        g.log.info("Deleting all snapshots created")
        ret, _, _ = snap_delete_all(self.mnode)
        self.assertEqual(ret, 0, "Failed to delete snapshots")
        g.log.info("All Snapshots deleted successfully")
