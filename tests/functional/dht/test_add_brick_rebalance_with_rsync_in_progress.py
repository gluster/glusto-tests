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
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.io.utils import collect_mounts_arequal, run_linux_untar


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed'], ['glusterfs']])
class TestAddBrickRebalanceWithRsyncInProgress(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Changing dist_count to 3
        self.volume['voltype']['dist_count'] = 3

        # Set I/O flag to false
        self.is_io_running = False

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

    def tearDown(self):

        # Wait for I/O if not completed
        if self.is_io_running:
            if not self._wait_for_untar_and_rsync_completion():
                g.log.error("I/O failed to stop on clients")

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume % s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def _wait_for_untar_and_rsync_completion(self):
        """Wait for untar and rsync to complete"""
        has_process_stopped = []
        for proc in self.list_of_io_processes:
            try:
                ret, _, _ = proc.async_communicate()
                if not ret:
                    has_process_stopped.append(False)
                has_process_stopped.append(True)
            except ValueError:
                has_process_stopped.append(True)
        return all(has_process_stopped)

    def test_add_brick_rebalance_with_rsync_in_progress(self):
        """
        Test case:
        1. Create, start and mount a volume.
        2. Create a directory on the mount point and start linux utar.
        3. Create another directory on the mount point and start rsync of
           linux untar directory.
        4. Add bricks to the volume
        5. Trigger rebalance on the volume.
        6. Wait for rebalance to complete on volume.
        7. Wait for I/O to complete.
        8. Validate if checksum of both the untar and rsync is same.
        """
        # List of I/O processes
        self.list_of_io_processes = []

        # Create a dir to start untar
        self.linux_untar_dir = "{}/{}".format(self.mounts[0].mountpoint,
                                              "linuxuntar")
        ret = mkdir(self.clients[0], self.linux_untar_dir)
        self.assertTrue(ret, "Failed to create dir linuxuntar for untar")

        # Start linux untar on dir linuxuntar
        ret = run_linux_untar(self.clients[0], self.mounts[0].mountpoint,
                              dirs=tuple(['linuxuntar']))
        self.list_of_io_processes += ret
        self.is_io_running = True

        # Create a new directory and start rsync
        self.rsync_dir = "{}/{}".format(self.mounts[0].mountpoint,
                                        'rsyncuntarlinux')
        ret = mkdir(self.clients[0], self.rsync_dir)
        self.assertTrue(ret, "Failed to create dir rsyncuntarlinux for rsync")

        # Start rsync for linux untar on mount point
        cmd = ("for i in `seq 1 3`; do rsync -azr {} {};sleep 120;done"
               .format(self.linux_untar_dir, self.rsync_dir))
        ret = g.run_async(self.clients[0], cmd)
        self.list_of_io_processes.append(ret)

        # Add bricks to the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick with rsync on volume %s"
                        % self.volname)

        # Trigger rebalance on the volume
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=6000)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)

        # Wait for IO to complete.
        ret = self._wait_for_untar_and_rsync_completion()
        self.assertFalse(ret, "IO didn't complete or failed on client")
        self.is_io_running = False

        # As we are running rsync and untar together, there are situations
        # when some of the new files created by linux untar is not synced
        # through rsync which causes checksum to retrun different value,
        # Hence to take care of this corner case we are rerunning rsync.
        cmd = "rsync -azr {} {}".format(self.linux_untar_dir, self.rsync_dir)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed sync left behind files")

        # Check daata consistency on both the directories
        rsync_checksum = collect_mounts_arequal(
            self.mounts[0], path='rsyncuntarlinux/linuxuntar/')
        untar_checksum = collect_mounts_arequal(self.mounts[0],
                                                path='linuxuntar')
        self.assertEqual(
            rsync_checksum, untar_checksum,
            "Checksum on untar dir and checksum on rsync dir didn't match")
