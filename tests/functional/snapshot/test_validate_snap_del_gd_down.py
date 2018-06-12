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

This test cases will validate snapshot delete behaviour
when glusterd is down on one node.

"""
import time
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.gluster_init import stop_glusterd, start_glusterd
from glustolibs.gluster.snap_ops import (get_snap_list, snap_delete,
                                         snap_delete_all, snap_create)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class SnapDelWhenGDDown(GlusterBaseClass):

    def setUp(self):

        # Setting and Mounting the volume
        GlusterBaseClass.setUp.im_func(self)
        g.log.info("Starting to Set and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):
        # deleting created snapshots
        ret, _, _ = snap_delete_all(self.mnode)
        if ret != 0:
            raise ExecutionError("Failed to delete snapshot of volume"
                                 "%s" % self.volname)
        g.log.info("Successfully deleted snapshots of volume %s", self.volname)

        # Unmount and volume cleanup
        g.log.info("Starting to Unmount and cleanup volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Clean Volume")
        g.log.info("Successful in Unmount and Clean Volume")

    def test_snap_del_gd_down(self):

        """
        Steps:
        1. Create volumes
        2. Create 5 snapshots
        3. Bring one node down
        4. Delete one snapshot
        5. list snapshot and validate delete
        6. Bring up the downed node
        7. Validate number of snaps after handshake on the
           brought down node.
        """
        # Create 5 snapshot
        g.log.info("Creating 5 snapshots for volume %s", self.volname)
        for i in range(0, 5):
            ret, _, _ = snap_create(self.mnode, self.volname, "snapy%s" % i)
            self.assertEqual(ret, 0, ("Failed to create snapshot for %s"
                                      % self.volname))
            g.log.info("Snapshot %s created successfully for volume  %s",
                       "snapy%s" % i, self.volname)

        # Check for no of snaps using snap_list it should be 5 now
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(5, len(snap_list), "No of snaps not consistent "
                         "for volume %s" % self.volname)
        g.log.info("Successfully validated number of snaps.")

        # Stopping glusterd service on server[1]
        ret = stop_glusterd(self.servers[1])
        self.assertTrue(ret, "Failed to stop glusterd service on node : %s"
                        % self.servers[1])
        g.log.info("Stopped glusterd services successfully on: %s",
                   self.servers[1])

        # Delete one snapshot snapy1
        ret, _, _ = snap_delete(self.servers[0], "snapy1")
        self.assertEqual(ret, 0, "Failed to delete snapshot snapy1")
        g.log.info("Successfully deleted snapshot of snapy1")

        # Check for no of snaps using snap_list it should be 4 now
        snap_list = get_snap_list(self.mnode)
        self.assertEqual(4, len(snap_list), "No of snaps not consistent "
                         "for volume %s" % self.volname)
        g.log.info("Successfully validated number of snaps.")

        # Starting glusterd services on server[1]
        ret = start_glusterd(self.servers[1])
        self.assertTrue(ret, "Failed to start glusterd on node "
                             ": %s" % self.servers[1])
        g.log.info("Started glusterd services successfully on: %s",
                   self.servers[1])

        # Check for no of snaps using snap_list it should be 4 for server[1]
        count = 0
        # putting wait here for glusterd handshake
        while count < 60:
            snap_list = get_snap_list(self.servers[1])
            if len(snap_list) == 4:
                break
            time.sleep(2)
            count += 2
        self.assertEqual(4, len(snap_list), "No of snaps not consistent "
                         "for volume %s" % self.volname)
        g.log.info("Successfully validated number of snaps.")
