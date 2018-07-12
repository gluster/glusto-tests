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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_list,
                                         snap_delete_all,
                                         get_snap_config,
                                         snap_delete_by_volumename,
                                         set_snap_config)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class DeleteSnapshotTests(GlusterBaseClass):
    """
    DeleteSnapshotTests contains tests which verifies the deletion of
    snapshots
    """
    def setUp(self):
        # SetUp volume and Mount volume
        GlusterBaseClass.setUpClass.im_func(self)
        g.log.info("Starting to SetUp Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup and Mount volume %s"
                                 % self.volname)
        g.log.info("Volume %s has been setup and mounted "
                   "successfully", self.volname)

    def test_snap_delete(self):
        # pylint: disable=too-many-statements
        """
        Delete a snapshot of a volume

        * Creating and deleting 10 snapshots
        * deleting previously created 10 snapshots
        * enabling auto-delete snapshot
        * Setting max-hard limit and max-soft-limit
        * Verify the limits by creating another 20 snapshots
        * Oldest of newly created snapshots will be deleted
        * Retaining the latest 8(softlimit) snapshots
        * cleanup snapshots and volumes
        """

        # creating 10 snapshots
        g.log.info("Creating 10 snapshots")
        for snap_count in range(0, 10):
            ret, _, _ = snap_create(self.mnode, self.volname,
                                    "snap%s" % snap_count, False,
                                    "Description with $p3c1al characters!")
            self.assertEqual(ret, 0, ("Failed to create snapshot"
                                      "snap%s" % snap_count))
            g.log.info("Snapshot snap%s of volume %s created"
                       "successfully.", snap_count, self.volname)

        # snapshot list
        g.log.info("Starting to list all snapshots")
        ret, _, _ = snap_list(self.mnode)
        self.assertEqual(ret, 0, ("Failed to list snapshot"
                                  "of volume %s" % self.volname))
        g.log.info("Snapshot list command for volume %s"
                   "was successful", self.volname)

        # deleting all 10 snapshots from the volume
        g.log.info("Starting to Delete 10 created snapshots")
        ret, _, _ = snap_delete_by_volumename(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to delete"
                                  "snapshot of volume %s" % self.volname))
        g.log.info("Snapshots of volume %s deleted Successfully", self.volname)

        # enabling auto-delete
        g.log.info("Enabling auto-delete")
        cmd = "gluster snapshot config auto-delete enable"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, ("Failed to enable auto-delete"
                                  "snapshot config"
                                  "option on volume % s" % self.volname))
        g.log.info("Snapshot auto-delete Successfully enabled")

        # setting max-hard-limit
        g.log.info("Setting max-hard-limit")
        option = {'snap-max-hard-limit': '10'}
        ret, _, _ = set_snap_config(self.mnode, option, self.volname)
        self.assertEqual(ret, 0, ("Failed to set snap-max-hardlimit"
                                  "config option for"
                                  "volume %s" % self.volname))
        g.log.info("snap-max-hardlimit config option Successfully set for"
                   "volume %s", self.volname)

        # Validating max-hard-limit
        g.log.info("Validating max-hard-limit")
        hardlimit = get_snap_config(self.mnode)
        self.assertIsNotNone(hardlimit, "Failed to get snap config")
        get_hardlimit = hardlimit['volumeConfig'][0]['hardLimit']
        self.assertEqual(get_hardlimit, '10', ("Failed to Validate"
                                               "max-hard-limit"))
        g.log.info("Successfully validated max-hard-limit")

        # setting max-soft-limit
        g.log.info("Setting max-soft-limit")
        option = {'snap-max-soft-limit': '80'}
        ret, _, _ = set_snap_config(self.mnode, option)
        self.assertEqual(ret, 0, ("Failed to set snap-max-soft-limit"
                                  "config option"))
        g.log.info("snap-max-soft-limit config option Successfully set")

        # Validating max-soft-limit
        g.log.info("Validating max-soft-limit")
        softlimit = get_snap_config(self.mnode)
        self.assertIsNotNone(softlimit, "Failed to get snap config")
        get_softlimit = softlimit['volumeConfig'][0]['softLimit']
        self.assertEqual(get_softlimit, '8', ("Failed to Validate"
                                              "max-soft-limit"))
        g.log.info("Successfully validated max-soft-limit")

        # creating 20 more snapshots. As the count
        # of snapshots crosses the
        # soft-limit the oldest of newly created snapshot should
        # be deleted only latest 8(softlimit) snapshots
        # should remain
        g.log.info("Starting to create 20 more snapshots")
        for snap_count in range(10, 30):
            ret, _, _ = snap_create(self.mnode, self.volname, "snap%s"
                                    % snap_count, False,
                                    "This is the Description with $p3c1al"
                                    "characters!")
            self.assertEqual(ret, 0, ("Failed to create snapshot snap%s"
                                      "for volume"
                                      "%s" % (snap_count, self.volname)))
            g.log.info("Snapshot snap%s of volume"
                       "%s created successfully.", snap_count, self.volname)

        # snapshot list to list total number of snaps after auto-delete
        g.log.info("validate total no.of snapshots after auto-delete enable")
        cmd = "gluster snapshot list | wc -l"
        ret, out, _ = g.run(self.mnode, cmd)
        tot = out.strip().split('\n')
        self.assertEqual(ret, 0, ("Failed to list snapshot of volume %s"
                                  % self.volname))
        g.log.info("Total number of snapshots created after auto-delete"
                   "enabled is %s", tot[0])
        self.assertEqual(tot[0], '8', ("Failed to validate snapshots with"
                                       "expected number of snapshots "))
        g.log.info("Successfully validated snapshots with expected"
                   "number of snapshots")

    def tearDown(self):

        # disabling auto-delete
        g.log.info("Disabling auto-delete")
        cmd = "gluster snapshot config auto-delete disable"
        ret, _, _ = g.run(self.mnode, cmd)
        if ret != 0:
            raise ExecutionError("Failed to disable auto-delete snapshot"
                                 "config option")
        g.log.info("Snapshot auto-delete Successfully disabled")

        # deleting created snapshots
        g.log.info("Deleting all created snapshots")
        ret, _, _ = snap_delete_all(self.mnode)
        if ret != 0:
            raise ExecutionError("Failed to delete snapshot of volume"
                                 "%s" % self.volname)
        g.log.info("Successfully deleted snapshots of"
                   "volume %s", self.volname)

        # setting back default max-soft-limit to 90%
        g.log.info("Setting max-soft-limit to default")
        cmd = {'snap-max-soft-limit': '90'}
        ret, _, _ = set_snap_config(self.mnode, cmd)
        if ret != 0:
            raise ExecutionError("Failed to set snap-max-soft-limit"
                                 "config option")
        g.log.info("snap-max-soft-limit config option Successfully set")

        # Unmount and cleanup-volume
        g.log.info("Starting to Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")
