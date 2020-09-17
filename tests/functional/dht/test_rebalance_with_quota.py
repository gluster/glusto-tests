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
# GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Description:
    Rebalance with quota on mountpoint.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              get_rebalance_status,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status,
    expand_volume)
from glustolibs.gluster.quota_ops import (
    quota_enable,
    quota_set_hard_timeout,
    quota_set_soft_timeout,
    quota_limit_usage)
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.io.utils import (
    wait_for_io_to_complete,
    collect_mounts_arequal)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed', 'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs']])
class TestRebalanceWithQuotOnRoot(GlusterBaseClass):
    """ Rebalance with quota enabled on mountpoint """

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        self.all_mounts_procs = []

        # Setup and Mount the volume
        g.log.info("Starting to Setup volume and mount it.")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup Volume and mount it")

        # Upload IO script for running IO on mounts
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                   "file_dir_ops.py")
        ret = upload_scripts(self.mounts[0].client_system,
                             self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to client")

    def test_rebalance_with_quota_enabled(self):
        """
        Test rebalance with quota enabled on root.
        1. Create Volume of type distribute
        2. Set Quota limit on the root directory
        3. Do some IO to reach the Hard limit
        4. After IO ends, compute arequal checksum
        5. Add bricks to the volume.
        6. Start rebalance
        7. After rebalance is completed, check arequal checksum
        """
        # Enable Quota
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(
            ret, 0, ("Failed to enable quota on the volume %s", self.volname))
        g.log.info("Successfully enabled quota on volume %s", self.volname)

        # Set the Quota timeouts to 0 for strict accounting
        ret, _, _ = quota_set_hard_timeout(self.mnode, self.volname, 0)
        self.assertEqual(
            ret, 0, ("Failed to set hard-timeout to 0 for %s", self.volname))
        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, 0)
        self.assertEqual(
            ret, 0, ("Failed to set soft-timeout to 0 for %s", self.volname))
        g.log.info(
            "Quota soft and hard timeout has been set to 0 for %s",
            self.volname)

        # Set the quota limit of 1 GB on root dir of the volume
        ret, _, _ = quota_limit_usage(self.mnode, self.volname, "/", "1GB")
        self.assertEqual(ret, 0, "Failed to set Quota for dir root")
        g.log.info("Successfully set quota limit for dir root")

        # Do some IO until hard limit is reached.
        cmd = (
            "/usr/bin/env python %s create_files "
            "-f 1024 --fixed-file-size 1M --base-file-name file %s"
            % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(
            self.mounts[0].client_system, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete and validate IO
        self.assertTrue(wait_for_io_to_complete(self.all_mounts_procs,
                                                self.mounts[0]),
                        "IO failed on some of the clients")
        g.log.info("IO completed on the clients")

        # Validate quota
        ret = quota_validate(self.mnode, self.volname,
                             path='/', hard_limit=1073741824,
                             sl_exceeded=True, hl_exceeded=True)
        self.assertTrue(ret, "Quota validate Failed for '/'")
        g.log.info("Quota Validated for path '/'")

        # Compute arequal checksum.
        arequal_checksum_before_rebalance = collect_mounts_arequal(self.mounts)

        # Log Volume info and status before expanding volume.
        log_volume_info_and_status(self.mnode, self.volname)

        # Expand the volume.
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume %s", self.volname))
        g.log.info("Expanding volume is successful on "
                   "volume %s", self.volname)

        # Log volume info and status after expanding volume.
        log_volume_info_and_status(self.mnode, self.volname)

        # Perform rebalance start operation.
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to  start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Rebalance started.")

        # Check rebalance is in progress
        rebalance_status = get_rebalance_status(self.mnode, self.volname)
        ret = rebalance_status['aggregate']['statusStr']
        self.assertEqual(ret, "in progress", ("Rebalance is not in "
                                              "'in progress' state, either "
                                              "rebalance is in completed state"
                                              "  or failed to get rebalance "
                                              "status"))
        g.log.info("Rebalance is 'in progress' state")

        # Wait till rebalance ends.
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        # Validate quota
        ret = quota_validate(self.mnode, self.volname,
                             path='/', hard_limit=1073741824,
                             sl_exceeded=True, hl_exceeded=True)
        self.assertTrue(ret, "Quota validate Failed for '/'")
        g.log.info("Quota Validated for path '/'")

        # Compute arequal checksum.
        arequal_checksum_after_rebalance = collect_mounts_arequal(self.mounts)

        # Comparing arequals checksum before and after rebalance.
        self.assertEqual(arequal_checksum_before_rebalance,
                         arequal_checksum_after_rebalance,
                         "arequal checksum is NOT MATCHING")
        g.log.info("arequal checksum is SAME")

    def tearDown(self):
        """tear Down Callback"""
        # Unmount volume and do cleanup
        g.log.info("Starting to Unmount volume and cleanup")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Faile to Unmount and cleanup volume")
        g.log.info("Successful in Unmount and cleanup of volumes")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
