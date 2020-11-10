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
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.volume_libs import shrink_volume
from glustolibs.gluster.volume_libs import form_bricks_list_to_remove_brick
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed-replicated', 'distributed-dispersed',
           'distributed-arbiter', 'distributed'], ['glusterfs']])
class TestRemoveBrickCommandOptions(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def _run_io_on_mount_point(self, fname="file"):
        """Create a few files on mount point"""
        cmd = ("cd {};for i in `seq 1 5`; do mkdir dir$i;"
               "for j in `seq 1 10`;do touch {}$j;done;done"
               .format(self.mounts[0].mountpoint, fname))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "Failed to do I/O on mount point")

    def test_remove_brick_command_basic(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create some data on the volume.
        3. Run remove-brick start, status and finally commit.
        4. Check if there is any data loss or not.
        """
        # Create some data on the volume
        self._run_io_on_mount_point()

        # Collect arequal checksum before ops
        arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        # Run remove-brick start, status and finally commit
        ret = shrink_volume(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to remove-brick from volume")
        g.log.info("Remove-brick rebalance successful")

        # Check for data loss by comparing arequal before and after ops
        arequal_checksum_after = collect_mounts_arequal(self.mounts[0])
        self.assertEqual(arequal_checksum_before, arequal_checksum_after,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")

    def test_remove_brick_command_force(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create some data on the volume.
        3. Run remove-brick with force.
        4. Check if bricks are still seen on volume or not
        """
        # Create some data on the volume
        self._run_io_on_mount_point()

        # Remove-brick on the volume with force option
        brick_list_to_remove = form_bricks_list_to_remove_brick(self.mnode,
                                                                self.volname)
        self.assertIsNotNone(brick_list_to_remove, "Brick list is empty")

        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 brick_list_to_remove, option="force")
        self.assertFalse(ret, "Failed to run remove-brick with force")
        g.log.info("Successfully run remove-brick with force")

        # Get a list of all bricks
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Brick list is empty")

        # Check if bricks removed brick are present or not in brick list
        for brick in brick_list_to_remove:
            self.assertNotIn(brick, brick_list,
                             "Brick still present in brick list even "
                             "after removing")
