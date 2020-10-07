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
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.lib_utils import get_size_of_mountpoint
from glustolibs.gluster.brick_libs import get_all_bricks


@runs_on([['distributed'], ['glusterfs']])
class TestRenameWithBricksMinFreeLimitCrossed(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Changing dist_count to 1
        self.volume['voltype']['dist_count'] = 1

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

        self.first_client = self.mounts[0].client_system
        self.mount_point = self.mounts[0].mountpoint

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_rename_with_brick_min_free_limit_crossed(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Calculate the usable size and fill till it reachs min free limit
        3. Rename the file
        4. Try to perfrom I/O from mount point.(This should fail)
        """
        bricks = get_all_bricks(self.mnode, self.volname)

        # Calculate the usable size and fill till it reachs
        # min free limit
        node, brick_path = bricks[0].split(':')
        size = int(get_size_of_mountpoint(node, brick_path))
        min_free_size = size * 10 // 100
        usable_size = ((size - min_free_size) // 1048576) + 1
        ret, _, _ = g.run(self.first_client, "fallocate -l {}G {}/file"
                          .format(usable_size, self.mount_point))
        self.assertFalse(ret, "Failed to fill disk to min free limit")
        g.log.info("Disk filled up to min free limit")

        # Rename the file
        ret, _, _ = g.run(self.first_client, "mv {}/file {}/Renamedfile"
                          .format(self.mount_point, self.mount_point))
        self.assertFalse(ret, "Rename failed on file to Renamedfile")
        g.log.info("File renamed successfully")

        # Try to perfrom I/O from mount point(This should fail)
        ret, _, _ = g.run(self.first_client,
                          "fallocate -l 5G {}/mfile".format(self.mount_point))
        self.assertTrue(ret,
                        "Unexpected: Able to do I/O even when disks are "
                        "filled to min free limit")
        g.log.info("Expected: Unable to perfrom I/O as min free disk is hit")
