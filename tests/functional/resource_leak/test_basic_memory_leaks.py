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
from glustolibs.io.utils import (run_linux_untar, validate_io_procs,
                                 wait_for_io_to_complete)
from glustolibs.io.memory_and_cpu_utils import (
    wait_for_logging_processes_to_stop)


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed', 'replicated',
           'arbiter', 'dispersed'], ['glusterfs']])
class TestBasicMemoryleak(GlusterBaseClass):

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

        # Wait for I/O to complete
        if self.is_io_running:
            if wait_for_io_to_complete(self.list_of_io_processes,
                                       self.mounts):
                raise ExecutionError("Failed to wait for I/O to complete")

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_basic_memory_leak(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Start I/O from mount point.
        3. Check if there are any memory leaks and OOM killers.
        """
        # Start monitoring resource usage on servers and clients
        monitor_proc_dict = self.start_memory_and_cpu_usage_logging(
            self.test_id, count=30)
        self.assertIsNotNone(monitor_proc_dict,
                             "Failed to start monitoring on servers and "
                             "clients")

        # Create a dir to start untar
        self.linux_untar_dir = "{}/{}".format(self.mounts[1].mountpoint,
                                              "linuxuntar")
        ret = mkdir(self.mounts[1].client_system, self.linux_untar_dir)
        self.assertTrue(ret, "Failed to create dir linuxuntar for untar")

        # Start multiple I/O from mount points
        self.list_of_io_processes = []
        cmd = ("cd {};for i in `seq 1 100`; do mkdir dir.$i ;"
               "for j in `seq 1 1000`; do dd if=/dev/random "
               "of=dir.$i/testfile.$j bs=1k count=10;done;done"
               .format(self.mounts[0].mountpoint))
        ret = g.run_async(self.mounts[0].client_system, cmd)
        self.list_of_io_processes = [ret]

        # Start linux untar on dir linuxuntar
        ret = run_linux_untar(self.mounts[1].client_system,
                              self.mounts[1].mountpoint,
                              dirs=tuple(['linuxuntar']))
        self.list_of_io_processes += ret
        self.is_io_running = True

        # Wait for I/O to complete and validate I/O on mount points
        ret = validate_io_procs(self.list_of_io_processes, self.mounts)
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

        ret = self.check_for_memory_leaks_and_oom_kills_on_clients(
            self.test_id)
        self.assertFalse(ret,
                         "Memory leak and OOM kills check failed on clients")
        g.log.info("No memory leaks or OOM kills found on serves and clients")
