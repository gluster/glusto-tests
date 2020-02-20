#  Copyright (C) 2018-2020 Red Hat, Inc. <http://www.redhat.com>
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

import random
from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    wait_for_rebalance_to_complete, rebalance_start, get_rebalance_status)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    log_volume_info_and_status,
    wait_for_volume_process_to_be_online)
from glustolibs.io.utils import (
    list_all_files_and_dirs_mounts,
    wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.gluster_init import (
    stop_glusterd, restart_glusterd,
    is_glusterd_running)


@runs_on([['distributed', 'dispersed', 'replicated',
           'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs']])
class RebalanceValidation(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        cls.all_mounts_procs = []
        for index, mount_obj in enumerate(cls.mounts, start=1):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 3 "
                   "--dir-length 3 "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 10 %s" % (
                       cls.script_upload_path,
                       index + 10, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            cls.all_mounts_procs.append(proc)

        # Wait for IO to complete
        g.log.info("Wait for IO to complete as IO validation did not "
                   "succeed in test method")
        ret = wait_for_io_to_complete(cls.all_mounts_procs, cls.mounts)
        if not ret:
            raise ExecutionError("IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

    def test_stop_glusterd_while_rebalance_in_progress(self):

        # Log Volume Info and Status before expanding the volume.
        g.log.info("Logging volume info and Status before expanding volume")
        log_volume_info_and_status(self.mnode, self.volname)
        g.log.info("Successful in logging volume info and status of "
                   "volume %s", self.volname)

        # Expanding volume by adding bricks to the volume
        g.log.info("Start adding bricks to volume")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Volume %s: Expand failed", self.volname))
        g.log.info("Volume %s: Expand success", self.volname)

        # Wait for gluster processes to come online
        g.log.info("Wait for gluster processes to come online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s: one or more volume process are "
                              "not up", self.volname))
        g.log.info("All volume %s processes are online", self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        log_volume_info_and_status(self.mnode, self.volname)
        g.log.info("Successful in logging volume info and status of "
                   "volume %s", self.volname)

        # Start Rebalance
        g.log.info("Starting rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Volume %s: Rebalance start failed",
                                  self.volname))
        g.log.info("Volume %s: Started rebalance", self.volname)

        # Wait for at least one file to be lookedup/scanned on the nodes
        status_info = get_rebalance_status(self.mnode, self.volname)
        count = 0
        while count < 100:
            lookups_start_count = 0
            for node in range(len(status_info['node'])):
                status_info = get_rebalance_status(self.mnode, self.volname)
                lookups_file_count = status_info['node'][node]['lookups']
                if int(lookups_file_count) > 0:
                    lookups_start_count += 1
                    sleep(5)
            if lookups_start_count == len(self.servers):
                g.log.info("Volume %s: At least one file is lookedup/scanned "
                           "on all nodes", self.volname)
                break
            count += 1

        # Form a new list of servers without mnode in it to prevent mnode
        # from glusterd failure
        nodes = self.servers[:]
        nodes.remove(self.mnode)

        # Stop glusterd on a server
        random_server = random.choice(nodes)
        g.log.info("Stop glusterd on server %s", random_server)
        ret = stop_glusterd(random_server)
        self.assertTrue(ret, ("Server %s: Failed to stop glusterd",
                              random_server))
        g.log.info("Server %s: Stopped glusterd", random_server)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=600)
        self.assertTrue(ret, ("Rebalance is either timed out or failed"
                              "%s", self.volname))
        g.log.info("Volume %s: Rebalance completed successfully",
                   self.volname)

    def tearDown(self):

        # restart glusterd on all servers
        g.log.info("Restart glusterd on all servers %s", self.servers)
        ret = restart_glusterd(self.servers)
        if not ret:
            raise ExecutionError("Failed to restart glusterd on all "
                                 "servers %s" % self.servers)
        g.log.info("Glusterd restart successful on all servers %s",
                   self.servers)

        # Check if glusterd is running on all servers(expected: active)
        g.log.info("Check if glusterd is running on all servers %s"
                   "(expected: active)", self.servers)
        ret = is_glusterd_running(self.servers)
        if ret != 0:
            raise ExecutionError("Glusterd is not running on all servers "
                                 "%s" % self.servers)
        g.log.info("Glusterd is running on all the servers %s", self.servers)

        # Validate all the peers are in connected state
        count = 0
        while count < 80:
            ret = self.validate_peers_are_connected()
            if ret:
                g.log.info("All peers are in connected state")
                break
            sleep(2)
            count += 1
        if not ret:
            raise ExecutionError("All peers are in connected state")

    @classmethod
    def tearDownClass(cls):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        cls.get_super_method(cls, 'tearDownClass')()
