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

"""
Description:
Test Cases in this module tests for
creating snapshot when the bricks are
down.

"""
import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.volume_ops import get_volume_info
from glustolibs.gluster.snap_ops import snap_create, snap_list
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           are_bricks_online,
                                           bring_bricks_offline,
                                           get_offline_bricks_list,
                                           get_online_bricks_list)


@runs_on([['distributed-replicated', 'replicated', 'distributed'],
          ['glusterfs', 'nfs', 'cifs']])
class CreateSnapwhenBricksareDown(GlusterBaseClass):
    """
    CreateSnapwhenBricksareDown contains tests
    which validates creating snapshot
    when the bricks are down
    """
    def setUp(self):
        # SetUp volume and Mount volume
        GlusterBaseClass.setUpClass.im_func(self)
        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_create_snap_bricks(self):
        """
        1. get brick list
        2. check all bricks are online
        3. Selecting one brick randomly to bring it offline
        4. get brick list
        5. check all bricks are online
        6. Offline Bricks list
        7. Online Bricks list
        8. Create snapshot of volume
        9. snapshot create should fail
        """

        bricks_list = []
        # get the bricks from the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # check all bricks are online
        g.log.info("Verifying all bricks are online or not.....")
        ret = are_bricks_online(self.mnode, self.volname,
                                bricks_list)
        self.assertTrue(ret, ("Not all bricks are online"))
        g.log.info("All bricks are online.")

        # Selecting one brick randomly to bring it offline
        g.log.info("Selecting one brick randomly to bring it offline")
        brick_to_bring_offline = random.choice(bricks_list)
        g.log.info("Brick to bring offline:%s ", brick_to_bring_offline)
        ret = bring_bricks_offline(self.volname, brick_to_bring_offline,
                                   None)
        self.assertTrue(ret, "Failed to bring the bricks offline")
        g.log.info("Randomly Selected brick: %s", brick_to_bring_offline)

        # get brick list
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # check all bricks are online
        g.log.info("Verifying all bricks are online or not.....")
        ret = are_bricks_online(self.mnode, self.volname,
                                bricks_list)
        self.assertFalse(ret, ("Not all bricks are online"))
        g.log.info("All bricks are online.")

        # get the bricks for the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # Offline Bricks list
        offbricks = get_offline_bricks_list(self.mnode, self.volname)
        g.log.info("Bricks Offline: %s", offbricks)

        # Online Bricks list
        onbricks = get_online_bricks_list(self.mnode, self.volname)
        g.log.info("Bricks Online: %s", onbricks)

        # Create snapshot of volume
        ret = snap_create(self.mnode, self.volname, "snap1",
                          False, "Description with $p3c1al characters!")
        self.assertTrue(ret, ("Failed to create snapshot snap1"))
        g.log.info("Snapshot snap1 of volume %s created Successfully",
                   self.volname)

        # Volume status
        ret = get_volume_info(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to perform gluster volume"
                              "info on volume %s"
                              % self.volname))
        g.log.info("Gluster volume info on volume %s is successful",
                   self.volname)
        # snapshot list
        ret = snap_list(self.mnode)
        self.assertTrue(ret, ("Failed to list snapshot of volume %s"
                              % self.volname))
        g.log.info("Snapshot list command for volume %s was successful",
                   self.volname)

    def tearDown(self):
        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

        # Unmount and cleanup-volume
        g.log.info("Starting to Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")
