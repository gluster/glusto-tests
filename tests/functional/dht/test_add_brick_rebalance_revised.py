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
    rebalance_start, wait_for_rebalance_to_complete, get_rebalance_status)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed'], ['glusterfs']])
class TestAddBrickRebalanceRevised(GlusterBaseClass):

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

    def _run_command_50_times(self, operation, msg):
        """
        Run a command 50 times on the mount point and display msg if fails
        """
        cmd = ("cd %s; for i in {1..50}; do %s;done"
               % (self.mounts[0].mountpoint, operation))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, msg)

    def _add_bricks_to_volume(self):
        """Add bricks to the volume"""
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)

    def _trigger_rebalance_and_wait(self, rebal_force=False):
        """Start rebalance with or without force and wait"""
        # Trigger rebalance on volume
        ret, _, _ = rebalance_start(self.mnode, self.volname,
                                    force=rebal_force)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1200)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

    def _check_if_files_are_skipped_or_not(self):
        """Check if files are skipped or not"""
        rebalance_status = get_rebalance_status(self.mnode, self.volname)
        ret = int(rebalance_status['aggregate']['skipped'])
        self.assertNotEqual(ret, 0, "Hardlink rebalance skipped")

    def _check_arequal_checksum_is_equal_before_and_after(self):
        """Check if arequal checksum is equal or not"""
        self.assertEqual(
            self.arequal_checksum_before, self.arequal_checksum_after,
            "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")

    def test_add_brick_rebalance_with_hardlinks(self):
        """
        Test case:
        1. Create a volume, start it and mount it using fuse.
        2. Create 50 files on the mount point and create 50 hardlinks for the
           files.
        3. After the files and hard links creation is complete, add bricks to
           the volume and trigger rebalance on the volume.
        4. Wait for rebalance to complete and check if files are skipped
           or not.
        5. Trigger rebalance on the volume with force and repeat step 4.
        """
        # Tuple of ops to be done
        ops = (("dd if=/dev/urandom of=file_$i bs=1M count=1",
                "Failed to create 50 files"),
               ("ln file_$i hardfile_$i",
                "Failed to create hard links for files"))

        # Create 50 files on the mount point and create 50 hard links
        # for the files.
        for operation, msg in ops:
            self._run_command_50_times(operation, msg)

        # Collect arequal checksum before add brick op
        self.arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        # After the file creation is complete, add bricks to the volume
        self._add_bricks_to_volume()

        # Trigger rebalance on the volume, wait for it to complete
        self._trigger_rebalance_and_wait()

        # Check if hardlinks are skipped or not
        self._check_if_files_are_skipped_or_not()

        # Trigger rebalance with force on the volume, wait for it to complete
        self._trigger_rebalance_and_wait(rebal_force=True)

        # Check if hardlinks are skipped or not
        self._check_if_files_are_skipped_or_not()

        # Compare arequals checksum before and after rebalance
        self.arequal_checksum_after = collect_mounts_arequal(self.mounts[0])
        self._check_arequal_checksum_is_equal_before_and_after()

    def test_add_brick_rebalance_with_sticky_bit(self):
        """
        Test case:
        1. Create a volume, start it and mount it using fuse.
        2. Create 50 files on the mount point and set sticky bit to the files.
        3. After the files creation and sticky bit addition is complete,
           add bricks to the volume and trigger rebalance on the volume.
        4. Wait for rebalance to complete.
        5. Check for data corruption by comparing arequal before and after.
        """
        # Tuple of ops to be done
        ops = (("dd if=/dev/urandom of=file_$i bs=1M count=1",
                "Failed to create 50 files"),
               ("chmod +t file_$i",
                "Failed to enable sticky bit for files"))

        # Create 50 files on the mount point and enable sticky bit.
        for operation, msg in ops:
            self._run_command_50_times(operation, msg)

        # Collect arequal checksum before add brick op
        self.arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        # After the file creation and sticky bit addtion is complete,
        # add bricks to the volume
        self._add_bricks_to_volume()

        # Trigger rebalance on the volume, wait for it to complete
        self._trigger_rebalance_and_wait()

        # Compare arequals checksum before and after rebalance
        self.arequal_checksum_after = collect_mounts_arequal(self.mounts[0])
        self._check_arequal_checksum_is_equal_before_and_after()
