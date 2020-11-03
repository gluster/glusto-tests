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

import string
from random import choice

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.dht_test_utils import find_hashed_subvol
from glustolibs.gluster.lib_utils import get_usable_size_per_disk
from glustolibs.gluster.glusterdir import get_dir_contents, mkdir
from glustolibs.gluster.glusterfile import get_dht_linkto_xattr
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (get_subvols, expand_volume)
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed'], ['glusterfs']])
class TestOneBrickFullAddBrickRebalance(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Changing dist_count to 3
        self.volume['voltype']['dist_count'] = 3

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

    @staticmethod
    def _get_random_string():
        letters = string.ascii_lowercase
        return ''.join(choice(letters) for _ in range(10))

    def test_one_brick_full_add_brick_rebalance(self):
        """
        Test case:
        1. Create a pure distribute volume with 3 bricks.
        2. Start it and mount it on client.
        3. Fill one disk of the volume till it's full
        4. Add brick to volume, start rebalance and wait for it to complete.
        5. Check arequal checksum before and after add brick should be same.
        6. Check if link files are present on bricks or not.
        """
        # Fill few bricks till it is full
        bricks = get_all_bricks(self.mnode, self.volname)

        # Calculate the usable size and fill till it reaches
        # min free limit
        usable_size = get_usable_size_per_disk(bricks[0])
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        fname = "abc"

        # Create directories in hierarchy
        dirp = "/dir1/dir2/"
        path = "{}{}".format(self.mounts[0].mountpoint, dirp)
        ret = mkdir(self.mounts[0].client_system, path, parents=True)
        self.assertTrue(ret, "Failed to create dir hierarchy")

        for _ in range(0, usable_size):

            # Create files inside directories
            while (subvols[find_hashed_subvol(subvols, dirp, fname)[1]][0] !=
                   subvols[0][0]):
                fname = self._get_random_string()
            ret, _, _ = g.run(self.mounts[0].client_system,
                              "fallocate -l 1G {}{}".format(path, fname))
            self.assertFalse(ret, "Failed to fill disk to min free limit")
            fname = self._get_random_string()
        g.log.info("Disk filled up to min free limit")

        # Collect arequal checksum before ops
        arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        # Add brick to volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)

        # Trigger rebalance and wait for it to complete
        ret, _, _ = rebalance_start(self.mnode, self.volname,
                                    force=True)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1800)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

        # Check for data loss by comparing arequal before and after ops
        arequal_checksum_after = collect_mounts_arequal(self.mounts[0])
        self.assertEqual(arequal_checksum_before, arequal_checksum_after,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")

        # Check if linkto files exist or not as rebalance is already
        # completed we shouldn't be seeing any linkto files
        for brick in bricks:
            node, path = brick.split(":")
            path += dirp
            list_of_files = get_dir_contents(node, path)
            self.assertIsNotNone(list_of_files, "Unable to get files")
            for filename in list_of_files:
                ret = get_dht_linkto_xattr(node, "{}{}".format(path,
                                                               filename))
                self.assertIsNone(ret, "Unable to fetch dht linkto xattr")
