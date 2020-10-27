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
from glustolibs.gluster.glusterfile import get_md5sum
from glustolibs.gluster.volume_libs import shrink_volume
from glustolibs.io.utils import validate_io_procs, wait_for_io_to_complete


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed'], ['glusterfs']])
class TestCopyHugeFileWithRemoveBrickInProgress(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

        self.first_client = self.mounts[0].client_system

    def tearDown(self):

        # If cp is running then wait for it to complete
        if self.cp_running:
            if not wait_for_io_to_complete(self.io_proc, [self.mounts[0]]):
                g.log.error("I/O failed to stop on clients")
            ret, _, _ = g.run(self.first_client, "rm -rf /mnt/huge_file.txt")
            if ret:
                g.log.error("Failed to remove huge file from /mnt.")

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_copy_huge_file_with_remove_brick_in_progress(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create files and dirs on the mount point.
        3. Start remove-brick and copy huge file when remove-brick is
           in progress.
        4. Commit remove-brick and check checksum of orginal and copied file.
        """
        # Create a directory with some files inside
        cmd = ("cd %s; for i in {1..10}; do mkdir dir$i; for j in {1..5};"
               " do dd if=/dev/urandom of=dir$i/file$j bs=1M count=1; done;"
               " done" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret,
                         "Failed to create dirs and files.")

        # Create a hug file under /mnt dir
        ret, _, _ = g.run(self.first_client,
                          "fallocate -l 10G /mnt/huge_file.txt")
        self.assertFalse(ret, "Failed to create hug file at /mnt")

        # Copy a huge file when remove-brick is in progress
        self.cp_running = False
        cmd = ("sleep 60; cd %s;cp ../huge_file.txt ."
               % self.mounts[0].mountpoint)
        self.io_proc = [g.run_async(self.first_client, cmd)]
        self.rename_running = True

        # Start remove-brick on volume and wait for it to complete
        ret = shrink_volume(self.mnode, self.volname, rebalance_timeout=1000)
        self.assertTrue(ret, "Failed to remove-brick from volume")
        g.log.info("Remove-brick rebalance successful")

        # Validate if copy was successful or not
        ret = validate_io_procs(self.io_proc, [self.mounts[0]])
        self.assertTrue(ret, "dir rename failed on mount point")
        self.cp_running = False

        # Check checksum of orginal and copied file
        original_file_checksum = get_md5sum(self.first_client,
                                            "/mnt/huge_file.txt")
        copied_file_checksum = get_md5sum(self.first_client,
                                          "{}/huge_file.txt"
                                          .format(self.mounts[0].mountpoint))
        self.assertEqual(original_file_checksum.split(" ")[0],
                         copied_file_checksum.split(" ")[0],
                         "md5 checksum of original and copied file are"
                         " different")
        g.log.info("md5 checksum of original and copied file are same.")

        # Remove original huge file
        ret, _, _ = g.run(self.first_client, "rm -rf /mnt/huge_file.txt")
        self.assertFalse(ret, "Failed to remove huge_file from mount point")
