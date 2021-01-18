#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_libs import (get_subvols, bulk_volume_creation,
                                            volume_stop, volume_start,
                                            set_volume_options)
from glustolibs.io.memory_and_cpu_utils import (
    wait_for_logging_processes_to_stop)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.brickmux_ops import (enable_brick_mux,
                                             disable_brick_mux,
                                             is_brick_mux_enabled)
from glustolibs.gluster.mount_ops import mount_volume, umount_volume


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestMemLeakAfterMgmntEncrypEnabled(GlusterBaseClass):

    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()
        self.test_id = self.id()
        # Setup Volume
        self.volume['dist_count'] = 2
        self.volume['replica_count'] = 3

        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup and Mount Volume")

        # Disable I/O encryption
        self._disable_io_encryption()

    def tearDown(self):
        # Disable brick_mux
        if is_brick_mux_enabled(self.mnode):
            ret = disable_brick_mux(self.mnode)
            self.assertTrue(ret, "Failed to brick multiplex")
            g.log.info("Disable brick multiplex")

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _run_io(self):
        """ Run IO and fill vol upto ~88%"""
        bricks = get_all_bricks(self.mnode, self.volname)
        usable_size = int(get_usable_size_per_disk(bricks[0]) * 0.88)

        self.procs = []
        counter = 1
        for _ in get_subvols(self.mnode, self.volname)['volume_subvols']:
            filename = "{}/test_file_{}".format(self.mounts[0].mountpoint,
                                                str(counter))
            proc = g.run_async(self.mounts[0].client_system,
                               "fallocate -l {}G {}".format(usable_size,
                                                            filename))
            self.procs.append(proc)
            counter += 1

    def _perform_gluster_v_heal_for_12_hrs(self):
        """ Run 'guster v heal info' for 12 hours"""
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

    def _verify_memory_leak(self):
        """ Verify memory leak is found """

        ret = self.check_for_memory_leaks_and_oom_kills_on_servers(
            self.test_id)
        self.assertFalse(ret,
                         "Memory leak and OOM kills check failed on servers")

        ret = self.check_for_memory_leaks_and_oom_kills_on_clients(
            self.test_id)
        self.assertFalse(ret,
                         "Memory leak and OOM kills check failed on clients")

    def _disable_io_encryption(self):
        """ Disables IO encryption """
        # UnMount Volume
        g.log.info("Starting to Unmount Volume %s", self.volname)
        ret, _, _ = umount_volume(self.mounts[0].client_system,
                                  self.mounts[0].mountpoint,
                                  mtype=self.mount_type)
        self.assertEqual(ret, 0, "Failed to Unmount volume")

        # Stop Volume
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to Stop volume")

        # Disable server and client SSL usage
        options = {"server.ssl": "off",
                   "client.ssl": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, "Failed to set volume options")

        # Start Volume
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to Start volume")

        # Mount Volume
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, "Failed to mount the volume back")

    def test_mem_leak_on_gluster_procs_with_management_encrpytion(self):
        """
        Steps:
        1) Enable management encryption on the cluster.
        2) Create a 2X3 volume.
        3) Mount the volume using FUSE on a client node.
        4) Start doing IO on the mount (ran IO till the volume is ~88% full)
        5) Simultaneously start collecting the memory usage for
           'glusterfsd' process.
        6) Issue the command "# gluster v heal <volname> info" continuously
           in a loop.
        """
        # Run IO
        self._run_io()

        # Start monitoring resource usage on servers and clients
        # default interval = 60 sec, count = 780 (60 *12)  => for 12 hrs
        monitor_proc_dict = self.start_memory_and_cpu_usage_logging(
            self.test_id, count=780)
        self.assertIsNotNone(monitor_proc_dict,
                             "Failed to start monitoring on servers and "
                             "clients")

        ret = validate_io_procs(self.procs, self.mounts)
        self.assertTrue(ret, "IO Failed")

        self._perform_gluster_v_heal_for_12_hrs()

        # Wait for monitoring processes to complete
        ret = wait_for_logging_processes_to_stop(monitor_proc_dict,
                                                 cluster=True)
        self.assertTrue(ret, "ERROR: Failed to stop monitoring processes")

        # Check if there are any memory leaks and OOM killers
        self._verify_memory_leak()
        g.log.info("No memory leaks/OOM kills found on serves and clients")

    def test_mem_leak_on_gluster_procs_with_brick_multiplex(self):
        """
        Steps:
        1) Enable cluster.brick-multiplex
        2) Enable SSL on management layer
        3) Start creating volumes
        4) Mount a volume and starting I/O
        5) Monitor the memory consumption by glusterd process
        """

        # Enable cluster.brick-mulitplex
        ret = enable_brick_mux(self.mnode)
        self.assertTrue(ret, "Failed to enable brick-multiplex")

        # Verify the operation
        ret = is_brick_mux_enabled(self.mnode)
        self.assertTrue(ret, "Brick mux enble op not successful")

        # Create few volumes
        self.volume['replica_count'] = 3
        ret = bulk_volume_creation(self.mnode, 20, self.all_servers_info,
                                   self.volume, is_force=True)

        self.assertTrue(ret, "Failed to create bulk volume")

        # Run IO
        self._run_io()

        # Start memory usage logging
        monitor_proc_dict = self.start_memory_and_cpu_usage_logging(
            self.test_id, count=60)
        self.assertIsNotNone(monitor_proc_dict,
                             "Failed to start monitoring on servers and "
                             "clients")

        ret = validate_io_procs(self.procs, self.mounts)
        self.assertTrue(ret, "IO Failed")

        # Wait for monitoring processes to complete
        ret = wait_for_logging_processes_to_stop(monitor_proc_dict,
                                                 cluster=True)
        self.assertTrue(ret, "ERROR: Failed to stop monitoring processes")

        # Check if there are any memory leaks and OOM killers
        self._verify_memory_leak()
        g.log.info("No memory leaks/OOM kills found on serves and clients")

        # Disable Brick multiplex
        ret = disable_brick_mux(self.mnode)
        self.assertTrue(ret, "Failed to brick multiplex")
