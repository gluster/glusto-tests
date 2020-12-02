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
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.


from datetime import datetime, timedelta
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.lib_utils import get_usable_size_per_disk
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.io.memory_and_cpu_utils import (
    wait_for_logging_processes_to_stop)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.io.utils import validate_io_procs


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestMemLeakAfterSSLEnabled(GlusterBaseClass):

    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()
        self.test_id = self.id()
        # Setup Volume
        self.volume['dist_count'] = 2
        self.volume['replica_count'] = 3
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError("Failed to Setup and Mount Volume")

    def tearDown(self):

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_mem_leak_on_gluster_procs_after_ssl_enabled(self):
        """
        Steps:
        Scenario 1:
        1) Enable management encryption on the cluster.
        2) Create a 2X3 volume.
        3) Mount the volume using FUSE on a client node.
        4) Start doing IO on the mount (ran IO till the volume is ~88% full)
        5) Simultaneously start collecting the memory usage for
           'glusterfsd' process.
        6) Issue the command "# gluster v heal <volname> info" continuously
           in a loop.
        """

        # Fill the vol approx 88%
        bricks = get_all_bricks(self.mnode, self.volname)
        usable_size = int(get_usable_size_per_disk(bricks[0]) * 0.88)

        procs = []
        counter = 1
        for _ in get_subvols(self.mnode, self.volname)['volume_subvols']:
            filename = "{}/test_file_{}".format(self.mounts[0].mountpoint,
                                                str(counter))
            proc = g.run_async(self.mounts[0].client_system,
                               "fallocate -l {}G {}".format(usable_size,
                                                            filename))
            procs.append(proc)
            counter += 1

        # Start monitoring resource usage on servers and clients
        # default interval = 60 sec
        # count = 780 (60 *12)  => for 12 hrs
        monitor_proc_dict = self.start_memory_and_cpu_usage_logging(
            self.test_id, count=780)
        self.assertIsNotNone(monitor_proc_dict,
                             "Failed to start monitoring on servers and "
                             "clients")

        ret = validate_io_procs(procs, self.mounts)
        self.assertTrue(ret, "IO Failed")

        # Perform gluster heal info for 12 hours
        end_time = datetime.now() + timedelta(hours=12)
        while True:
            curr_time = datetime.now()
            cmd = "gluster volume heal %s info" % self.volname
            ret, _, _ = g.run(self.mnode, cmd)
            self.assertEqual(ret, 0, "Failed to execute heal info cmd")
            if curr_time > end_time:
                g.log.info("Successfully ran for 12 hours. Checking for "
                           "memory leaks")
                break

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
        g.log.info(
            "No memory leaks/OOM kills found on serves and clients")
