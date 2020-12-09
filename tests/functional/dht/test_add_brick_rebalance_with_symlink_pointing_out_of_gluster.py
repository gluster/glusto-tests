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
from glustolibs.gluster.glusterfile import get_md5sum
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.io.utils import (validate_io_procs, wait_for_io_to_complete)


@runs_on([['distributed-replicated', 'distributed-arbiter'], ['glusterfs']])
class TestAddBrickRebalanceWithSymlinkPointingOutOfGluster(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Setup and mount volume")

        self.is_io_running = False

    def tearDown(self):

        # Remove the temporary dir created for test
        ret, _, _ = g.run(self.mounts[0].client_system, "rm -rf /mnt/tmp/")
        if ret:
            raise ExecutionError("Failed to remove /mnt/tmp create for test")

        # If I/O processes are running wait for it to complete
        if self.is_io_running:
            if not wait_for_io_to_complete(self.list_of_io_processes,
                                           [self.mounts[0]]):
                raise ExecutionError("Failed to wait for I/O to complete")

        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_add_brick_rebalance_with_symlink_pointing_out_of_volume(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create symlinks on the volume such that the files for the symlink
           are outside the volume.
        3. Once all the symlinks are create a data file using dd:
           dd if=/dev/urandom of=FILE bs=1024 count=100
        4. Start copying the file's data to all the symlink.
        5. When data is getting copied to all files through symlink add brick
           and start rebalance.
        6. Once rebalance is complete check the md5sum of each file through
           symlink and compare if it's same as the orginal file.
        """
        # Create symlinks on volume pointing outside volume
        cmd = ("cd %s; mkdir -p /mnt/tmp;for i in {1..100};do "
               "touch /mnt/tmp/file$i; ln -sf /mnt/tmp/file$i link$i;done"
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(
            ret, "Failed to create symlinks pointing outside volume")

        # Create a data file using dd inside mount point
        cmd = ("cd %s; dd if=/dev/urandom of=FILE bs=1024 count=100"
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "Failed to create data file on mount point")

        # Start copying data from file to symliks
        cmd = ("cd %s;for i in {1..100};do cat FILE >> link$i;done"
               % self.mounts[0].mountpoint)
        self.list_of_io_processes = [
            g.run_async(self.mounts[0].client_system, cmd)]
        self.is_copy_running = True

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

        # Validate if I/O was successful or not.
        ret = validate_io_procs(self.list_of_io_processes, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.is_copy_running = False

        # Get md5sum of the original file and compare it with that of
        # all files through the symlink
        original_file_md5sum = get_md5sum(self.mounts[0].client_system,
                                          "{}/FILE".format(
                                              self.mounts[0].mountpoint))
        self.assertIsNotNone(original_file_md5sum,
                             'Failed to get md5sum of original file')
        for number in range(1, 101):
            symlink_md5sum = get_md5sum(self.mounts[0].client_system,
                                        "{}/link{}".format(
                                            self.mounts[0].mountpoint, number))
            self.assertEqual(original_file_md5sum.split(' ')[0],
                             symlink_md5sum.split(' ')[0],
                             "Original file and symlink checksum not equal"
                             " for link%s" % number)
        g.log.info("Symlink and original file checksum same on all symlinks")
