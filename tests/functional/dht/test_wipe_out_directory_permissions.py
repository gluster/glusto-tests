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
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import get_file_stat, get_fattr


@runs_on([['distributed'], ['glusterfs']])
class TestDhtWipeOutDirectoryPremissions(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Changing dist_count to 1
        self.volume['voltype']['dist_count'] = 1

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

        # Assign a variable for the first_client
        self.first_client = self.mounts[0].client_system

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def _check_permissions_of_dir(self):
        """Check permissions of dir created."""
        for brick_path in get_all_bricks(self.mnode, self.volname):
            node, path = brick_path.split(":")
            ret = get_file_stat(node, "{}/dir".format(path))
            self.assertEqual(int(ret["access"]), 755,
                             "Unexpected:Permissions of dir is %s and not %d"
                             % (ret["access"], 755))
        g.log.info("Permissions of dir directory is proper on all bricks")

    def _check_trusted_glusterfs_dht_on_all_bricks(self):
        """Check trusted.glusterfs.dht xattr on the backend bricks"""
        bricks = get_all_bricks(self.mnode, self.volname)
        possible_values = ["0x000000000000000000000000ffffffff",
                           "0x00000000000000000000000000000000"]
        for brick_path in bricks:
            node, path = brick_path.split(":")
            ret = get_fattr(node, "{}/dir".format(path),
                            "trusted.glusterfs.dht")
            self.assertEqual(
                ret, possible_values[bricks.index(brick_path)],
                "Value of trusted.glusterfs.dht is not as expected")
        g.log.info("Successfully checked value of trusted.glusterfs.dht.")

    def test_wipe_out_directory_permissions(self):
        """
        Test case:
        1. Create a 1 brick pure distributed volume.
        2. Start the volume and mount it on a client node using FUSE.
        3. Create a directory on the mount point.
        4. Check trusted.glusterfs.dht xattr on the backend brick.
        5. Add brick to the volume using force.
        6. Do lookup from the mount point.
        7. Check the directory permissions from the backend bricks.
        8. Check trusted.glusterfs.dht xattr on the backend bricks.
        9. From mount point cd into the directory.
        10. Check the directory permissions from backend bricks.
        11. Check trusted.glusterfs.dht xattr on the backend bricks.
        """
        # Create a directory on the mount point
        self.dir_path = "{}/dir".format(self.mounts[0].mountpoint)
        ret = mkdir(self.first_client, self.dir_path)
        self.assertTrue(ret, "Failed to create directory dir")

        # Check trusted.glusterfs.dht xattr on the backend brick
        self._check_trusted_glusterfs_dht_on_all_bricks()

        # Add brick to the volume using force
        brick_list = form_bricks_list(self.mnode, self.volname, 1,
                                      self.servers, self.all_servers_info)
        self.assertIsNotNone(brick_list,
                             "Failed to get available space on mount point")
        ret, _, _ = add_brick(self.mnode, self.volname, brick_list, force=True)
        self.assertEqual(ret, 0, ("Volume {}: Add-brick failed".format
                                  (self.volname)))

        # Do a lookup from the mount point
        cmd = "ls -lR {}".format(self.dir_path)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertEqual(ret, 0, "Failed to lookup")
        g.log.info("Lookup successful")

        # Check the directory permissions from the backend bricks
        self._check_permissions_of_dir()

        # Check trusted.glusterfs.dht xattr on the backend bricks
        self._check_trusted_glusterfs_dht_on_all_bricks()

        # From mount point cd into the directory
        ret, _, _ = g.run(self.first_client, "cd {};cd ..;cd {}"
                          .format(self.dir_path, self.dir_path))
        self.assertEqual(ret, 0, "Unable to cd into dir from mount point")

        # Check the directory permissions from backend bricks
        self._check_permissions_of_dir()

        # Check trusted.glusterfs.dht xattr on the backend bricks
        self._check_trusted_glusterfs_dht_on_all_bricks()
