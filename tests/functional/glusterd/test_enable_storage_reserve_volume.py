#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Description:
   This test case is authored to test posix storage.reserve option.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestPosixStorageReserveOption(GlusterBaseClass):
    def setUp(self):
        """
        setUp method for every test.
        """
        self.get_super_method(self, 'setUp')()

        # setup volume
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup volume")

    def test_enable_storage_reserve_volume(self):
        """
        1) Create a distributed-replicated volume and start it.
        2) Enable storage.reserve option on the volume using below command,
        gluster volume set storage.reserve.
            let's say, set it to a value of 50.
        3) Mount the volume on a client
        4) check df -h output of the mount point and backend bricks.
        """
        # Set volume option storage.reserve 50
        ret = set_volume_options(
            self.mnode, self.volname, {"storage.reserve ": 50})
        self.assertTrue(
            ret, "gluster volume set {} storage.reserve 50 Failed on server "
                 "{}".format(self.volname, self.mnode))
        # Mounting the volume on a client
        ret = self.mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to mount volume")

        ret, out, _ = g.run(
            self.clients[0], "df -h | grep -i '{}'".format(
                self.mounts[0].mountpoint))
        self.assertFalse(
            ret, "Failed to run cmd df -h on client {}".format
            (self.clients[0]))

        self.assertTrue("51%" in out.split(" "), "51 % is not in list ")

    def tearDown(self):
        """Tear Down callback"""
        # Unmount volume and cleanup.
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to unmount and cleanup volume")
        g.log.info("Successful in unmount and cleanup operations")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
