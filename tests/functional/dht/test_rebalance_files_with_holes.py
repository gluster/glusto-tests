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
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import expand_volume, shrink_volume


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed', 'replicated',
           'arbiter', 'dispersed'], ['glusterfs']])
class TestAddBrickRebalanceFilesWithHoles(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

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

    def test_add_brick_rebalance_files_with_holes(self):
        """
        Test case:
        1. Create a volume, start it and mount it using fuse.
        2. On the volume root, create files with holes.
        3. After the file creation is complete, add bricks to the volume.
        4. Trigger rebalance on the volume.
        5. Wait for rebalance to complete.
        """
        # On the volume root, create files with holes
        cmd = ("cd %s;for i in {1..5000}; do dd if=/dev/urandom"
               " of=file_with_holes$i bs=1M count=1 seek=100M; done"
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create files with holes")

        # After the file creation is complete, add bricks to the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)

        # Trigger rebalance on the volume
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=9000)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed'], ['glusterfs']])
class TestRemoveBrickRebalanceFilesWithHoles(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

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

    def test_remove_brick_rebalance_files_with_holes(self):
        """
        Test case:
        1. Create a volume, start it and mount it using fuse.
        2. On the volume root, create files with holes.
        3. After the file creation is complete, remove-brick from volume.
        4. Wait for remove-brick to complete.
        """
        # On the volume root, create files with holes
        cmd = ("cd %s;for i in {1..2000}; do dd if=/dev/urandom"
               " of=file_with_holes$i bs=1M count=1 seek=100M; done"
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create files with holes")

        # After the file creation is complete, remove-brick from volume
        # Wait for remove-brick to complete
        ret = shrink_volume(self.mnode, self.volname, rebalance_timeout=16000)
        self.assertTrue(ret, "Failed to remove-brick from volume")
        g.log.info("Remove-brick rebalance successful")
