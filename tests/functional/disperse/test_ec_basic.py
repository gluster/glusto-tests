#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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
Test Description:
    Tests basic volume operations like ec volume create, start, stop, delete
    as part of setUp() and tearDown(). The test case test_disperse_vol()
    verifies brick down, brick up scenarios and logs volume info and status.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.volume_libs import (log_volume_info_and_status)

from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           are_bricks_online,
                                           bring_bricks_offline,
                                           bring_bricks_online)


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs']])
class EcBasicTest(GlusterBaseClass):
    # Method to setup the environment for test case
    def setUp(self):
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=True)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    # Tests: Kill 2 bricks and start it again
    def test_disperse_vol(self):
        bricks_list = get_all_bricks(self.mnode, self.volname)

        ret = bring_bricks_offline(self.volname, bricks_list[0:2])
        self.assertTrue(ret, "Failed to bring down the bricks")
        g.log.info("Successfully brought the bricks down")

        ret = bring_bricks_online(self.mnode, self.volname, bricks_list[0:2])
        self.assertTrue(ret, "Failed to bring up the bricks")
        g.log.info("Successfully brought the bricks up")

        # Verifying all bricks online
        ret = are_bricks_online(self.mnode, self.volname, bricks_list)
        if not ret:
            self.assertTrue(ret, "All bricks are not online")

        g.log.info("Logging volume info and status")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed "
                              "on volume %s", self.volname))
        g.log.info("Successful in logging volume info and status "
                   "of volume %s", self.volname)

    def tearDown(self):
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
