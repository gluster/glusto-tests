#  Copyright (C) 2017-2020 Red Hat, Inc. <http://www.redhat.com>
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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    get_rebalance_status,
    rebalance_start,
    rebalance_status,
    rebalance_stop,
    wait_for_fix_layout_to_complete,
    wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    log_volume_info_and_status,
    verify_all_process_of_volume_are_online,
    form_bricks_list_to_add_brick,
    wait_for_volume_process_to_be_online)
from glustolibs.io.utils import (
    collect_mounts_arequal,
    list_all_files_and_dirs_mounts,
    wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_ops import add_brick


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestExerciseRebalanceCommand(GlusterBaseClass):
    """
    Steps:
    1) Create a gluster volume and start it.
    2) create some data on the mount point.
    3) Calculate arequal checksum on the mount point.
    4) Add few bricks to the volume.
    5) Initiate rebalance using command
        gluster volume rebalance <vol> start
    6) Check the status of the rebalance using command,
        gluster volume rebalance <vol> status
    7) While migration in progress stop the rebalance in the mid
        gluster volume rebalance <vol> stop.
    8) check whether migration is stopped or not.
    9) once rebalance stops , calculate the checksum on the mount point.
    """
    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUpClass')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", self.clients)
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                   "file_dir_ops.py")
        ret = upload_scripts(self.clients, self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 self.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   self.clients)

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for index, mount_obj in enumerate(self.mounts, start=1):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 2 "
                   "--max-num-of-dirs 2 "
                   "--num-of-files 10 %s" % (self.script_upload_path,
                                             index + 10,
                                             mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Wait for IO to complete
        g.log.info("Wait for IO to complete")
        ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
        if not ret:
            raise ExecutionError("IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        if not ret:
            raise ExecutionError("Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

    def test_fix_layout_start(self):
        # pylint: disable=too-many-statements
        # Get arequal checksum before starting fix-layout
        g.log.info("Getting arequal checksum before fix-layout")
        arequal_checksum_before_fix_layout = collect_mounts_arequal(self.
                                                                    mounts)

        # Log Volume Info and Status before expanding the volume.
        g.log.info("Logging volume info and Status before expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, "Logging volume info and status failed on "
                        "volume %s" % self.volname)
        g.log.info("Successful in logging volume info and status of volume "
                   "%s", self.volname)

        # Form brick list for expanding volume
        add_brick_list = form_bricks_list_to_add_brick(
            self.mnode, self.volname, self.servers, self.all_servers_info,
            distribute_count=1)
        self.assertIsNotNone(add_brick_list, ("Volume %s: Failed to form "
                                              "bricks list to expand",
                                              self.volname))
        g.log.info("Volume %s: Formed bricks list to expand", self.volname)

        # Expanding volume by adding bricks to the volume
        g.log.info("Volume %s: Expand start")
        ret, _, _ = add_brick(self.mnode, self.volname, add_brick_list)
        self.assertEqual(ret, 0, ("Volume %s: Expand failed", self.volname))
        g.log.info("Volume %s: Expand successful", self.volname)

        # Wait for gluster processes to come online
        g.log.info("Wait for gluster processes to come online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s: one or more volume process are "
                              "not up", self.volname))
        g.log.info("All volume %s processes are online", self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, "Logging volume info and status failed on "
                             "volume %s" % self.volname)
        g.log.info("Successful in logging volume info and status of volume "
                   "%s", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Start Rebalance fix-layout
        g.log.info("Starting fix-layout on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname, fix_layout=True)
        self.assertEqual(ret, 0, ("Volume %s: fix-layout start failed"
                                  "%s", self.volname))
        g.log.info("Volume %s: fix-layout start success", self.volname)

        # Wait for fix-layout to complete
        g.log.info("Waiting for fix-layout to complete")
        ret = wait_for_fix_layout_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s: Fix-layout is still in-progress",
                              self.volname))
        g.log.info("Volume %s: Fix-layout completed successfully",
                   self.volname)

        # Check Rebalance status after fix-layout is complete
        g.log.info("Checking Rebalance status")
        ret, _, _ = rebalance_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Volume %s: Failed to get rebalance status",
                                  self.volname))
        g.log.info("Volume %s: Successfully got rebalance status",
                   self.volname)

        # Get arequal checksum after fix-layout is complete
        g.log.info("arequal after fix-layout is complete")
        arequal_checksum_after_fix_layout = collect_mounts_arequal(self.mounts)

        # Compare arequals checksum before and after fix-layout
        g.log.info("Comparing checksum before and after fix-layout")
        self.assertEqual(arequal_checksum_before_fix_layout,
                         arequal_checksum_after_fix_layout,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")

        # Check if there are any file migrations after fix-layout
        status_info = get_rebalance_status(self.mnode, self.volname)
        for node in range(len(status_info['node'])):
            status_info = get_rebalance_status(self.mnode, self.volname)
            file_migration_count = status_info['node'][node]['files']
            self.assertEqual(int(file_migration_count), 0, (
                "Server %s: Few files are migrated", self.servers[node]))
            g.log.info("Server %s: No files are migrated")

        # Check if new bricks contains any files
        for brick in add_brick_list:
            brick_node, brick_path = brick.split(":")
            cmd = ('find %s -type f ! -perm 1000 | grep -ve .glusterfs'
                   % brick_path)
            _, out, _ = g.run(brick_node, cmd)
            self.assertEqual(len(out), 0, (
                ("Files(excluded linkto files) are present on %s:%s"),
                (brick_node, brick_path)))
            g.log.info("No files (excluded linkto files) are present on %s:%s",
                       brick_node, brick_path)

    def test_rebalance_start_status_stop(self):
        # pylint: disable=too-many-statements
        # Form brick list for expanding volume
        add_brick_list = form_bricks_list_to_add_brick(
            self.mnode, self.volname, self.servers, self.all_servers_info,
            distribute_count=1)
        self.assertIsNotNone(add_brick_list, ("Volume %s: Failed to form "
                                              "bricks list to expand",
                                              self.volname))
        g.log.info("Volume %s: Formed bricks list to expand", self.volname)

        # Expanding volume by adding bricks to the volume
        g.log.info("Volume %s: Expand start")
        ret, _, _ = add_brick(self.mnode, self.volname, add_brick_list)
        self.assertEqual(ret, 0, ("Volume %s: Expand failed", self.volname))
        g.log.info("Volume %s: Expand successful", self.volname)

        # Wait for gluster processes to come online
        g.log.info("Wait for gluster processes to come online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s: one or more volume process are "
                              "not up", self.volname))
        g.log.info("All volume %s processes are online", self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, "Logging volume info and status failed on "
                             "volume %s" % self.volname)
        g.log.info("Successful in logging volume info and status of volume "
                   "%s", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Getting arequal checksum before rebalance start
        g.log.info("Getting arequal before rebalance start")
        arequal_checksum_before_rebalance_start = collect_mounts_arequal(
            self.mounts)

        # Start Rebalance
        g.log.info("Starting Rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Volume %s: Failed to start rebalance",
                                  self.volname))
        g.log.info("Volume %s: Rebalance started ", self.volname)

        # Stop on-going rebalance
        g.log.info("Stop rebalance on the volume")
        ret, _, _ = rebalance_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Volume %s: Failed to stop rebalance",
                                  self.volname))
        g.log.info("Checking whether the migration is stopped or not")

        # Wait till the on-going file migration completes on all servers
        count = 0
        while count < 80:
            rebalance_count = 0
            for server in self.servers:
                ret, _, _ = g.run(server, "pgrep rebalance")
                if ret != 0:
                    rebalance_count += 1
            if rebalance_count == len(self.servers):
                break
            sleep(2)
            count += 1
        g.log.info("Volume %s: Rebalance process is not running on servers",
                   self.volname)

        # List all files and dirs from mount point
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        g.log.info("Listing all files and directories is successful")

        # Getting arequal checksum after the rebalance is stopped
        g.log.info("Getting arequal checksum after the rebalance is stopped")
        arequal_checksum_after_rebalance_stop = collect_mounts_arequal(self.
                                                                       mounts)

        # Comparing arequals checksum before start of rebalance and
        #                       after the rebalance is stopped
        g.log.info("Comparing arequals checksum before start of rebalance and"
                   "after the rebalance is stopped")
        self.assertEqual(arequal_checksum_before_rebalance_start,
                         arequal_checksum_after_rebalance_stop,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")

    def test_rebalance_with_force(self):

        # Getting arequal checksum before rebalance
        g.log.info("Getting arequal checksum before rebalance")
        arequal_checksum_before_rebalance = collect_mounts_arequal(self.mounts)

        # Log Volume Info and Status before expanding the volume.
        g.log.info("Logging volume info and Status before expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, "Logging volume info and status failed on "
                             "volume %s" % self.volname)
        g.log.info("Successful in logging volume info and"
                   "status of volume %s", self.volname)

        # Expanding volume by adding bricks to the volume
        g.log.info("Start adding bricks to volume")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Volume %s: Expand failed", self.volname))
        g.log.info("Volume %s: Expand successful", self.volname)

        # Wait for gluster processes to come online
        g.log.info("Wait for gluster processes to come online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s: one or more volume process are "
                              "not up", self.volname))
        g.log.info("All volume %s processes are online", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and"
                   "status of volume %s", self.volname)

        # Start Rebalance with force
        g.log.info("Starting Rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, ("Volume %s: Failed to start rebalance with "
                                  "force", self.volname))
        g.log.info("Volume %s: Started rebalance with force option",
                   self.volname)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=600)
        self.assertTrue(ret, ("Volume %s: Rebalance is still in-progress ",
                              self.volname))
        g.log.info("Volume %s: Rebalance completed", self.volname)

        # Getting arequal checksum after rebalance
        g.log.info("Getting arequal checksum after rebalance with force "
                   "option")
        arequal_checksum_after_rebalance = collect_mounts_arequal(self.mounts)

        # Comparing arequals checksum before and after rebalance with force
        # option
        g.log.info("Comparing arequals checksum before and after rebalance"
                   "with force option")
        self.assertEqual(arequal_checksum_before_rebalance,
                         arequal_checksum_after_rebalance,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")

        # Checking if rebalance skipped any files
        status = get_rebalance_status(self.mnode, self.volname)
        for each_node in status['node']:
            self.assertEqual(int(each_node['skipped']), 0,
                             "Few files are skipped on node %s" %
                             each_node['nodeName'])
            g.log.info("No files are skipped on %s", each_node['nodeName'])

    def tearDown(self):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
