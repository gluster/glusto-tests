#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_delete_all,
                                         set_snap_config,
                                         get_snap_config)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs', 'nfs', 'cifs']])
class DeleteSnapTests(GlusterBaseClass):
    """
    DeleteSnapTests contains tests which verifies the deletion of
    snapshots
    """

    def test_auto_delete_snap(self):
        """
        * enabling auto-delete snapshot
        * Setting max-hard limit and max-soft-limit
        * Validating max-hard-limit and max-soft-limit
        * Verify the limits by creating another 20 snapshots
        * Oldest of newly created snapshots will be deleted
        * Retaining the latest 8(softlimit) snapshots
        * cleanup snapshots and volumes
        """
        # Setup volume
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

        # enabling auto-delete
        cmd = "gluster snapshot config auto-delete enable"
        ret = g.run(self.mnode, cmd)
        self.assertTrue(ret, ("Failed to enable auto-delete snapshot config"
                              "option on volume % s" % self.volname))
        g.log.info("Snapshot auto-delete Successfully enabled")

        # setting max-hard-limit
        option = {'snap-max-hard-limit': '10'}
        ret = set_snap_config(self.mnode, option, self.volname)
        self.assertTrue(ret, ("Failed to set snap-max-hardlimit"
                              "config option for volume %s" % self.volname))
        g.log.info("snap-max-hardlimit config option Successfully set for"
                   "volume %s", self.volname)

        # Validating max-hard-limit
        hardlimit = get_snap_config(self.mnode)
        get_hardlimit = hardlimit['volumeConfig'][0]['hardLimit']
        if get_hardlimit != '10':
            self.assertTrue(ret, ("Failed to Validate max-hard-limit"))
        g.log.info("Successfully validated max-hard-limit")

        # setting max-soft-limit
        option = {'snap-max-soft-limit': '80'}
        ret = set_snap_config(self.mnode, option)
        self.assertTrue(ret, ("Failed to set snap-max-soft-limit"
                              "config option"))
        g.log.info("snap-max-soft-limit config option Successfully set")

        # Validating max-soft-limit
        softlimit = get_snap_config(self.mnode)
        get_softlimit = softlimit['volumeConfig'][0]['softLimit']
        if get_softlimit != '8':
            self.assertTrue(ret, ("Failed to Validate max-soft-limit"))
        g.log.info("Successfully validated max-soft-limit")

        # creating 20 snapshots. As the count
        # of snapshots crosses the
        # soft-limit the oldest of newly created snapshot should
        # be deleted only latest 8 snapshots
        # should remain.

        # creating 20 more snapshots
        for snap_count in range(10, 30):
            ret = snap_create(self.mnode, self.volname, "snap%s"
                              % snap_count, False,
                              "This is the Description with $p3c1al"
                              "characters!")
            self.assertTrue(ret, ("Failed to create snapshot snap%s for volume"
                                  "%s" % (snap_count, self.volname)))
            g.log.info("Snapshot snap%s of volume %s created successfully")

        # snapshot list to list total number of snaps after auto-delete
        cmd = "gluster snapshot list | wc -l"
        ret, out, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, ("Failed to list snapshot of volume %s"
                                  % self.volname))
        g.log.info("Total number of snapshots created after auto-delete"
                   "enabled is %s", out)
        if out != 8:
            g.log.info("Failed to validate snapshots with expected"
                       "number of snapshots")
        g.log.info("Snapshot Validation Successful")
        g.log.info("Snapshot list command for volume %s was successful",
                   self.volname)

    def tearDown(self):
        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

        # disabling auto-delete
        cmd = "gluster snapshot config auto-delete disable"
        ret = g.run(self.mnode, cmd)
        self.assertTrue(ret, ("Failed to disable auto-delete snapshot"
                              "config option"))
        g.log.info("Snapshot auto-delete Successfully disabled")

        # deleting created snapshots
        ret = snap_delete_all(self.mnode)
        self.assertTrue(ret, ("Failed to delete snapshot of volume"
                              "%s" % self.volname))
        g.log.info("Successfully deleted snapshots of volume %s",
                   self.volname)

        # setting back default max-soft-limit to 90%
        option = {'snap-max-soft-limit': '90'}
        ret = set_snap_config(self.mnode, option)
        self.assertTrue(ret, ("Failed to set snap-max-soft-limit"
                              "config option"))
        g.log.info("snap-max-soft-limit config option Successfully set")

        # cleanup-volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Cleanup volume %s Completed Successfully", self.volname)
