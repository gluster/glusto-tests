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
from glustolibs.gluster.glusterdir import get_dir_contents
from glustolibs.io.memory_and_cpu_utils import (
    wait_for_logging_processes_to_stop)
from glustolibs.gluster.brick_libs import get_all_bricks


@runs_on([['arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestMemoryLeakWithRm(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Set test_id for get gathering
        self.test_id = self.id()

        # Set I/O flag to false
        self.is_io_running = False

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_memory_leak_with_rm(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create 10,000 files each of size 200K
        3. Delete the files created at step 2
        4. Check if the files are deleted from backend
        5. Check if there are any memory leaks and OOM killers.
        """
        # Start monitoring resource usage on servers and clients
        monitor_proc_dict = self.start_memory_and_cpu_usage_logging(
            self.test_id, count=30)
        self.assertIsNotNone(monitor_proc_dict,
                             "Failed to start monitoring on servers and "
                             "clients")
        # Create files on mount point
        cmd = ('cd %s;for i in {1..10000};'
               'do dd if=/dev/urandom bs=200K count=1 of=file$i;done;'
               'rm -rf %s/file*'
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create and delete files on"
                         " mountpoint")
        g.log.info("Successfully created and removed files on mountpoint")

        # Delete files from mount point and check if all files
        # are deleted or not from mount point as well as backend bricks.
        ret, _, _ = g.run(self.clients[0],
                          "rm -rf {}/*".format(self.mounts[0].mountpoint))
        self.assertFalse(ret, "rm -rf * failed on mount point")

        ret = get_dir_contents(self.clients[0],
                               "{}/".format(self.mounts[0].mountpoint))
        self.assertEqual(ret, [], "Unexpected: Files and directories still "
                         "seen from mount point")

        for brick in get_all_bricks(self.mnode, self.volname):
            node, brick_path = brick.split(":")
            ret = get_dir_contents(node, "{}/".format(brick_path))
            self.assertEqual(ret, [], "Unexpected: Files and dirs still seen "
                             "on brick %s on node %s" % (brick_path, node))
        g.log.info("rm -rf * on mount point successful")

        # Wait for monitoring processes to complete
        ret = wait_for_logging_processes_to_stop(monitor_proc_dict,
                                                 cluster=True)
        self.assertTrue(ret,
                        "ERROR: Failed to stop monitoring processes")

        # Check if there are any memory leaks and OOM killers
        ret = self.check_for_memory_leaks_and_oom_kills_on_servers(
            self.test_id)
        self.assertFalse(ret,
                         "Memory leak and OOM kills check failed on servers")

        ret = self.check_for_memory_leaks_and_oom_kills_on_clients(
            self.test_id)
        self.assertFalse(ret,
                         "Memory leak and OOM kills check failed on clients")
        g.log.info("No memory leaks or OOM kills found on serves and clients")
