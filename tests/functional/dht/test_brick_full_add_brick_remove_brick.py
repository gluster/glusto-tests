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
from glustolibs.gluster.volume_libs import (get_subvols, expand_volume,
                                            shrink_volume)
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed-replicated', 'distributed-arbiter'], ['glusterfs']])
class TestBrickFullAddBrickRemoveBrickRebalance(GlusterBaseClass):

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

    @staticmethod
    def _get_random_string():
        letters = string.ascii_lowercase
        return ''.join(choice(letters) for _ in range(5))

    def test_brick_full_add_brick_remove_brick(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Fill few bricks till min-free-limit is reached.
        3. Add brick to the volume.(This should pass.)
        4. Set cluster.min-free-disk to 30%.
        5. Remove bricks from the volume.(This should pass.)
        6. Check for data loss by comparing arequal before and after.
        """
        # Fill few bricks till it is full
        bricks = get_all_bricks(self.mnode, self.volname)

        # Calculate the usable size and fill till it reaches
        # min free limit
        usable_size = get_usable_size_per_disk(bricks[0])
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        filename = "abc"
        for _ in range(0, usable_size):
            while (subvols[find_hashed_subvol(subvols, "/", filename)[1]]
                   == subvols[0]):
                filename = self._get_random_string()
            ret, _, _ = g.run(self.mounts[0].client_system,
                              "fallocate -l 1G {}/{}".format(
                                  self.mounts[0].mountpoint, filename))
            self.assertFalse(ret, "Failed to fill disk to min free limit")
            filename = self._get_random_string()
        g.log.info("Disk filled up to min free limit")

        # Collect arequal checksum before ops
        arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        # Add brick to volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)

        # Set cluster.min-free-disk to 30%
        ret = set_volume_options(self.mnode, self.volname,
                                 {'cluster.min-free-disk': '30%'})
        self.assertTrue(ret, "Failed to set cluster.min-free-disk to 30%")

        # Remove bricks from the volume
        ret = shrink_volume(self.mnode, self.volname, rebalance_timeout=1800)
        self.assertTrue(ret, "Failed to remove-brick from volume")
        g.log.info("Remove-brick rebalance successful")

        # Check for data loss by comparing arequal before and after ops
        arequal_checksum_after = collect_mounts_arequal(self.mounts[0])
        self.assertEqual(arequal_checksum_before, arequal_checksum_after,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")
