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

from random import choice

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterfile import get_fattr
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_fix_layout_to_complete)
from glustolibs.gluster.volume_libs import (form_bricks_list_to_add_brick,
                                            replace_brick_from_volume)


@runs_on([['distributed-replicated', 'distributed-arbiter'], ['glusterfs']])
class TestAddBrickReplaceBrickFixLayout(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Changing dist_count to 3
        self.volume['voltype']['dist_count'] = 3

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

        self.first_client = self.mounts[0].client_system

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def _replace_a_old_added_brick(self, brick_to_be_replaced):
        """Replace a old brick from the volume"""
        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        self.servers, self.all_servers_info,
                                        src_brick=brick_to_be_replaced)
        self.assertTrue(ret, "Failed to replace brick %s "
                        % brick_to_be_replaced)
        g.log.info("Successfully replaced brick %s", brick_to_be_replaced)

    def _check_trusted_glusterfs_dht_on_all_bricks(self):
        """Check trusted.glusterfs.dht xattr on the backend bricks"""
        bricks = get_all_bricks(self.mnode, self.volname)
        fattr_value = []
        for brick_path in bricks:
            node, path = brick_path.split(":")
            ret = get_fattr(node, "{}".format(path), "trusted.glusterfs.dht")
            fattr_value += [ret]
        self.assertEqual(len(set(fattr_value)), 4,
                         "Value of trusted.glusterfs.dht is not as expected")
        g.log.info("Successfully checked value of trusted.glusterfs.dht.")

    def test_add_brick_replace_brick_fix_layout(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create files and dirs on the mount point.
        3. Add bricks to the volume.
        4. Replace 2 old bricks to the volume.
        5. Trigger rebalance fix layout and wait for it to complete.
        6. Check layout on all the bricks through trusted.glusterfs.dht.
        """
        # Create directories with some files on mount point
        cmd = ("cd %s; for i in {1..10}; do mkdir dir$i; for j in {1..5};"
               " do dd if=/dev/urandom of=dir$i/file$j bs=1M count=1; done;"
               " done" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create dirs and files.")

        # Orginal brick list before add brick
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Empty present brick list")

        # Add bricks to the volume
        add_brick_list = form_bricks_list_to_add_brick(
            self.mnode, self.volname, self.servers, self.all_servers_info)
        self.assertIsNotNone(add_brick_list, "Empty add brick list")

        ret, _, _ = add_brick(self.mnode, self.volname, add_brick_list)
        self.assertFalse(ret, "Failed to add bricks to the volume")
        g.log.info("Successfully added bricks to the volume")

        # Replace 2 old bricks to the volume
        for _ in range(0, 2):
            brick = choice(brick_list)
            self._replace_a_old_added_brick(brick)
            brick_list.remove(brick)

        # Start rebalance and wait for it to complete
        ret, _, _ = rebalance_start(self.mnode, self.volname, fix_layout=True)
        self.assertFalse(ret, "Failed to start rebalance on volume")

        ret = wait_for_fix_layout_to_complete(self.mnode, self.volname,
                                              timeout=800)
        self.assertTrue(ret, "Rebalance failed on volume")

        # Check layout on all the bricks through trusted.glusterfs.dht
        self._check_trusted_glusterfs_dht_on_all_bricks()
