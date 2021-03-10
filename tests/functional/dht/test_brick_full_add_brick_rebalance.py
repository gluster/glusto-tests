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
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import get_subvols, expand_volume
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['distributed-replicated', 'distributed-arbiter'], ['glusterfs']])
class TestBrickFullAddBrickRebalance(GlusterBaseClass):

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

    def test_brick_full_add_brick_rebalance(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create a data set on the client node such that all the available
           space is used and "No space left on device" error is generated.
        3. Set cluster.min-free-disk to 30%.
        4. Add bricks to the volume, trigger rebalance and wait for rebalance
           to complete.
        """
        # Create a data set on the client node such that all the available
        # space is used and "No space left on device" error is generated
        bricks = get_all_bricks(self.mnode, self.volname)

        # Calculate the usable size and fill till it reaches
        # min free limit
        usable_size = get_usable_size_per_disk(bricks[0])
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        filename = "abc"
        for subvol in subvols:
            while (subvols[find_hashed_subvol(subvols, "/", filename)[1]] ==
                   subvol):
                filename = self._get_random_string()
            ret, _, err = g.run(self.mounts[0].client_system,
                                "fallocate -l {}G {}/{}".format(
                                    usable_size, self.mounts[0].mountpoint,
                                    filename))
            err_msg = 'fallocate: fallocate failed: No space left on device'
            if ret and err == err_msg:
                ret = 0
            self.assertFalse(ret, "Failed to fill disk to min free limit")
        g.log.info("Disk filled up to min free limit")

        # Try to perfrom I/O from mount point(This should fail)
        ret, _, _ = g.run(self.mounts[0].client_system,
                          "fallocate -l 5G {}/mfile".format(
                              self.mounts[0].mountpoint))
        self.assertTrue(ret,
                        "Unexpected: Able to do I/O even when disks are "
                        "filled to min free limit")
        g.log.info("Expected: Unable to perfrom I/O as min free disk is hit")

        # Set cluster.min-free-disk to 30%
        ret = set_volume_options(self.mnode, self.volname,
                                 {'cluster.min-free-disk': '30%'})
        self.assertTrue(ret, "Failed to set cluster.min-free-disk to 30%")

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
                                             timeout=1200)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")
