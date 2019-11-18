#  Copyright (C) 2019 Red Hat, Inc. <http://www.redhat.com>
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

from random import choice
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    wait_for_rebalance_to_complete, rebalance_start)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.io.utils import (
    list_all_files_and_dirs_mounts,
    wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.gluster_init import (
    stop_glusterd, restart_glusterd,
    is_glusterd_running)
from glustolibs.gluster.brick_libs import get_all_bricks


@runs_on([['distributed'],
          ['glusterfs']])
class RebalanceValidation(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
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
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 1 "
                   "--dir-length 1 "
                   "--max-num-of-dirs 1 "
                   "--num-of-files 1 %s" % (cls.script_upload_path,
                                            index + 10,
                                            mount_obj.mountpoint))
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

    def test_rebalance_start_when_glusterd_down(self):

        # Expanding volume by adding bricks to the volume
        g.log.info("Start adding bricks to volume")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Volume %s: Expand failed", self.volname))
        g.log.info("Volume %s: Expand success", self.volname)

        # Get all servers IP addresses which are part of volume
        ret = get_all_bricks(self.mnode, self.volname)
        list_of_servers_used = []
        for brick in ret:
            list_of_servers_used.append(brick.split(":")[0])
        self.assertTrue(ret, ("Failed to get server IP list for volume %s",
                              self.volname))
        g.log.info("Succesfully got server IP list for volume %s",
                   self.volname)

        # Form a new list of servers without mnode in it to prevent mnode
        # from glusterd failure
        for element in list_of_servers_used:
            if element == self.mnode:
                list_of_servers_used.remove(element)

        # Stop glusterd on a server
        self.random_server = choice(list_of_servers_used)
        g.log.info("Stop glusterd on server %s", self.random_server)
        ret = stop_glusterd(self.random_server)
        self.assertTrue(ret, ("Server %s: Failed to stop glusterd",
                              self.random_server))
        g.log.info("Server %s: Stopped glusterd", self.random_server)

        # Start Rebalance
        g.log.info("Starting rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Volume %s: Failed to start rebalance",
                                  self.volname))
        g.log.info("Volume %s: Rebalance start success", self.volname)
        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertFalse(ret, ("Volume %s: Rebalance is completed",
                               self.volname))
        g.log.info("Expected: Rebalance failed on one or more nodes."
                   " Check rebalance status for more details")
        error_msg1 = "\"fix layout on / failed\""
        error_msg2 = "\"Transport endpoint is not connected\""
        ret, _, _ = g.run(self.mnode, "grep -w %s /var/log/glusterfs/"
                          "%s-rebalance.log| grep -w %s"
                          % (error_msg1, self.volname, error_msg2))
        self.assertEqual(ret, 0, ("Unexpected : Rebalance failed on volume %s"
                                  "not because of glusterd down on a node",
                                  self.volname))
        g.log.info("\n\nRebalance failed on volume %s due to glusterd down on"
                   "one of the nodes\n\n", self.volname)

    def tearDown(self):

        # restart glusterd on the stopped server
        g.log.info("Restart glusterd on %s", self.random_server)
        ret = restart_glusterd(self.random_server)
        if not ret:
            raise ExecutionError("Failed to restart glusterd %s" %
                                 self.random_server)
        g.log.info("Successfully restarted glusterd on %s",
                   self.random_server)

        # Check if glusterd is running on all servers(expected: active)
        g.log.info("Check if glusterd is running on all servers %s"
                   "(expected: active)", self.servers)
        ret = is_glusterd_running(self.servers)
        if ret != 0:
            raise ExecutionError("Glusterd is not running on all servers"
                                 " %s" % self.servers)
        g.log.info("Glusterd is running on all the servers "
                   "%s", self.servers)

        # Check peer status from every node
        count = 0
        while count < 80:
            ret = self.validate_peers_are_connected()
            if ret:
                break
            sleep(2)
            count += 1
        if not ret:
            raise ExecutionError("All peers are in connected state")

        # Validate all the peers are in connected state
        g.log.info("Validating all the peers are in Cluster and Connected")
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Validating Peers to be in Cluster "
                                 "Failed")
        g.log.info("All peers are in connected state")

    @classmethod
    def tearDownClass(cls):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDownClass.im_func(cls)
