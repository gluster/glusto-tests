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
    Rebalance with one brick down in replica
"""

from random import choice

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    log_volume_info_and_status,
    volume_start)
from glustolibs.gluster.brick_libs import (
    get_all_bricks,
    bring_bricks_offline)
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.io.utils import (
    wait_for_io_to_complete,
    collect_mounts_arequal)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed-arbiter', 'distributed-replicated',
           'distributed-dispersed'], ['glusterfs']])
class TestRebalanceWithBrickDown(GlusterBaseClass):
    """ Rebalance with brick down in replica"""

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

    def test_rebalance_with_brick_down(self):
        """
        Rebalance with brick down in replica
        - Create a Replica volume.
        - Bring down one of the brick down in the replica pair
        - Do some IO and create files on the mount point
        - Add a pair of bricks to the volume
        - Initiate rebalance
        - Bring back the brick which was down.
        - After self heal happens, all the files should be present.
        """
        # Log the volume info and status before brick is down.
        log_volume_info_and_status(self.mnode, self.volname)

        # Bring one fo the bricks offline
        brick_list = get_all_bricks(self.mnode, self.volname)
        ret = bring_bricks_offline(self.volname, choice(brick_list))

        # Log the volume info and status after brick is down.
        log_volume_info_and_status(self.mnode, self.volname)

        # Create files at mountpoint.
        cmd = (
            "/usr/bin/env python %s create_files "
            "-f 2000 --fixed-file-size 1k --base-file-name file %s"
            % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(
            self.mounts[0].client_system, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete.
        self.assertTrue(wait_for_io_to_complete(self.all_mounts_procs,
                                                self.mounts[0]),
                        "IO failed on some of the clients")
        g.log.info("IO completed on the clients")

        # Compute the arequal checksum before bringing all bricks online
        arequal_before_all_bricks_online = collect_mounts_arequal(self.mounts)

        # Log the volume info and status before expanding volume.
        log_volume_info_and_status(self.mnode, self.volname)

        # Expand the volume.
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume %s", self.volname))
        g.log.info("Expanding volume is successful on "
                   "volume %s", self.volname)

        # Log the voluem info after expanding volume.
        log_volume_info_and_status(self.mnode, self.volname)

        # Start Rebalance.
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        # Log the voluem info and status before bringing all bricks online
        log_volume_info_and_status(self.mnode, self.volname)

        # Bring all bricks online.
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Not able to start volume with force option")
        g.log.info("Volume start with force option successful.")

        # Log the volume info and status after bringing all beicks online
        log_volume_info_and_status(self.mnode, self.volname)

        # Monitor heal completion.
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "heal has not yet completed")
        g.log.info("Self heal completed")

        # Compute the arequal checksum after all bricks online.
        arequal_after_all_bricks_online = collect_mounts_arequal(self.mounts)

        # Comparing arequal checksum before and after the operations.
        self.assertEqual(arequal_before_all_bricks_online,
                         arequal_after_all_bricks_online,
                         "arequal checksum is NOT MATCHING")
        g.log.info("arequal checksum is SAME")

    def tearDown(self):
        """tear Down callback"""
        # Unmount Volume and cleanup.
        g.log.info("Starting to Unmount Volume and Cleanup")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Filed to Unmount Volume and "
                                 "Cleanup Volume")
        g.log.info("Successful in Unmount Volume and cleanup.")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
