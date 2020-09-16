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
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-131 USA.

"""
Description:
    Rebalance with special files
"""

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    rebalance_start,
    get_rebalance_status,
    wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    log_volume_info_and_status)
from glustolibs.io.utils import wait_for_io_to_complete
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed', 'distributed-arbiter', 'distributed-replicated',
           'distributed-dispersed'], ['glusterfs']])
class TestRebalanceWithSpecialFiles(GlusterBaseClass):
    """ Rebalance with special files"""

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        self.all_mounts_procs = []

        # Setup and mount the volume
        g.log.info("Starting to setup and mount the volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup Volume and Mount it")

        # Upload IO script for running IO on mounts
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                   "file_dir_ops.py")
        ret = upload_scripts(self.mounts[0].client_system,
                             self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    def test_rebalance_with_special_files(self):
        """
        Rebalance with special files
        - Create Volume and start it.
        - Create some special files on mount point.
        - Once it is complete, start some IO.
        - Add brick into the volume and start rebalance
        - All IO should be successful.
        """
        # Create pipe files at mountpoint.
        cmd = (
            "for i in {1..500};do mkfifo %s/fifo${i}; done"
            % (self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to create pipe files")
        g.log.info("Pipe files created successfully")

        # Create block device files at mountpoint.
        cmd = (
            "for i in {1..500};do mknod %s/blk${i} blockfile 1 5;done"
            % (self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to create block files")
        g.log.info("Block files created successfully")

        # Create character device files at mountpoint.
        cmd = (
            "for i in {1..500};do mknod %s/charc${i} characterfile 1 5;done"
            % (self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to create character files")
        g.log.info("Character files created successfully")

        # Create files at mountpoint.
        cmd = (
            "/usr/bin/env python %s create_files "
            "-f 1000 --fixed-file-size 1M --base-file-name file %s"
            % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(
            self.mounts[0].client_system, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Log the volume info and status before expanding volume.
        log_volume_info_and_status(self.mnode, self.volname)

        # Expand the volume.
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume %s", self.volname))
        g.log.info("Expanding volume is successful on "
                   "volume %s", self.volname)

        # Log the volume info after expanding volume.
        log_volume_info_and_status(self.mnode, self.volname)

        # Start Rebalance.
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Check rebalance is in progress
        rebalance_status = get_rebalance_status(self.mnode, self.volname)
        ret = rebalance_status['aggregate']['statusStr']
        self.assertEqual(ret, "in progress", ("Rebalance is not in "
                                              "'in progress' state, either "
                                              "rebalance is in completed state"
                                              " or failed to get rebalance "
                                              "status"))
        g.log.info("Rebalance is in 'in progress' state")

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        # Wait for IO to complete.
        self.assertTrue(wait_for_io_to_complete(self.all_mounts_procs,
                                                self.mounts[0]),
                        "IO failed on some of the clients")
        g.log.info("IO completed on the clients")

    def tearDown(self):
        """tear Down callback"""
        # Unmount Volume and cleanup.
        g.log.info("Starting to Unmount Volume and Cleanup")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and "
                                 "Cleanup Volume")
        g.log.info("Successful in Unmount Volume and cleanup.")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
