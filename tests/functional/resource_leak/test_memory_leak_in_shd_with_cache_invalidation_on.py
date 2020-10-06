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
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.io.utils import (validate_io_procs, wait_for_io_to_complete)
from glustolibs.io.memory_and_cpu_utils import (
    wait_for_logging_processes_to_stop)


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'replicated',
           'arbiter', 'dispersed'], ['glusterfs']])
class TestMemoryLeakInShdWithCacheInvalidationOn(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Set test_id for get gathering
        self.test_id = self.id()

        # Set I/O flag to false
        self.is_io_running = False

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

    def tearDown(self):

        # Wait for I/O to complete
        if self.is_io_running:
            if wait_for_io_to_complete(self.list_of_io_processes,
                                       self.mounts[0]):
                raise ExecutionError("Failed to wait for I/O to complete")

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_memory_leak_in_shd_with_cache_invalidation_on(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Set features.cache-invalidation to ON.
        3. Start I/O from mount point.
        4. Run gluster volume heal command in a loop
        5. Check if there are any memory leaks and OOM killers on servers.
        """
        # Start monitoring resource usage on servers and clients
        monitor_proc_dict = self.start_memory_and_cpu_usage_logging(
            self.test_id, count=10)
        self.assertIsNotNone(monitor_proc_dict,
                             "Failed to start monitoring on servers and"
                             " clients")

        # Set features.cache-invalidation to ON
        ret = set_volume_options(self.mnode, self.volname,
                                 {'features.cache-invalidation': 'on'})
        self.assertTrue(ret, "Failed to set features.cache-invalidation to ON")
        g.log.info("Successfully set features.cache-invalidation to ON")

        # Start multiple I/O from mount points
        self.list_of_io_processes = []
        cmd = ("cd {};for i in `seq 1 1000`;do echo 'abc' > myfile;done"
               .format(self.mounts[0].mountpoint))
        ret = g.run_async(self.mounts[0].client_system, cmd)
        self.list_of_io_processes = [ret]
        self.is_io_running = True

        # Run gluster volume heal command in a loop for 100 iterations
        for iteration in range(0, 100):
            g.log.info("Running gluster volume heal command for %d time",
                       iteration)
            ret = trigger_heal(self.mnode, self.volname)
            self.assertTrue(ret, "Heal command triggered successfully")
        g.log.info("Ran gluster volume heal command in a loop for "
                   "100 iterations.")

        # Wait for I/O to complete and validate I/O on mount points
        ret = validate_io_procs(self.list_of_io_processes, self.mounts[0])
        self.assertTrue(ret, "I/O failed on mount point")
        self.is_io_running = False

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
