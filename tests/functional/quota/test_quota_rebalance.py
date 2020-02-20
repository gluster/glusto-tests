#  Copyright (C) 2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.quota_ops import (
    quota_enable,
    quota_set_hard_timeout,
    quota_set_soft_timeout,
    quota_limit_usage)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (
    wait_for_io_to_complete,
    validate_io_procs)
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.rebalance_ops import (
    rebalance_start,
    get_rebalance_status,
    wait_for_rebalance_to_complete)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'],
          ['glusterfs', 'nfs']])
class TestQuotaRebalance(GlusterBaseClass):
    """
        Test if the quota limits are honored while a rebalance
        is in progress.
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, "setUpClass")()

        # Upload io scripts for running IO on mounts
        g.log.info(
            "Upload io scripts to clients %s for running IO on mounts",
            cls.clients)
        cls.script_upload_path = (
            "/usr/share/glustolibs/io/scripts/file_dir_ops.py")
        ret = upload_scripts(cls.clients, [cls.script_upload_path])
        if not ret:
            raise ExecutionError(
                "Failed to upload IO scripts to clients %s" % cls.clients)
        g.log.info(
            "Successfully uploaded IO scripts to clients %s", cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, "setUp")()
        self.all_mounts_procs = []

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, "tearDown")()

    def test_quota_rebalance(self):
        """
        * Enable quota on the volume
        * set hard and soft time out to zero.
        * Create some files and directories from mount point
          so that the limits are reached.
        * Perform add-brick operation on the volume.
        * Start rebalance on the volume.
        * While rebalance is in progress, create some more files
          and directories from the mount point until limit is hit
        """

        # pylint: disable=too-many-statements
        # Enable Quota on volume
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(
            ret, 0, ("Failed to enable quota on the volume %s", self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

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

        # Set limit of 100 MB on root dir of the volume
        ret, _, _ = quota_limit_usage(self.mnode, self.volname, "/", "100MB")
        self.assertEqual(ret, 0, "Failed to set Quota for dir '/'")
        g.log.info("Successfully set quota limit for dir '/'")

        # Do some IO  until hard limit is reached
        cmd = (
            "/usr/bin/env python %s create_files "
            "-f 100 --fixed-file-size 1M --base-file-name file %s"
            % (self.script_upload_path,
               self.mounts[0].mountpoint))
        proc = g.run_async(
            self.mounts[0].client_system, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete and validate IO
        self.assertTrue(
            wait_for_io_to_complete(self.all_mounts_procs, self.mounts[0]),
            "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Add bricks to the volume
        if "replica_count" in self.volume["voltype"]:
            new_bricks_count = self.volume["voltype"]["replica_count"]
        elif "disperse_count" in self.volume["voltype"]:
            new_bricks_count = self.volume["voltype"]["disperse_count"]
        else:
            new_bricks_count = 3
        bricks_list = form_bricks_list(
            self.mnode,
            self.volname,
            new_bricks_count,
            self.servers,
            self.all_servers_info)
        g.log.info("new brick list: %s", bricks_list)
        ret, _, _ = add_brick(self.mnode, self.volname, bricks_list, False)
        self.assertEqual(ret, 0, "Failed to add the bricks to the volume")
        g.log.info("Successfully added bricks to volume")

        # Perform rebalance start operation
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Rebalance Start Failed")

        # Wait for at least one file to be lookedup/scanned on the nodes
        status_info = get_rebalance_status(self.mnode, self.volname)
        count = 0
        while count < 20:
            lookups_start_count = 0
            for node in range(len(status_info['node'])):
                status_info = get_rebalance_status(self.mnode, self.volname)
                lookups_file_count = status_info['node'][node]['lookups']
                if int(lookups_file_count) > 0:
                    lookups_start_count += 1
                    sleep(2)
            if lookups_start_count == len(self.servers):
                g.log.info("Volume %s: At least one file is lookedup/scanned "
                           "on all nodes", self.volname)
                break
            count += 1

        # Perform some more IO and check if hard limit is honoured
        self.all_mounts_procs = []
        cmd = (
            "/usr/bin/env python %s create_files "
            "-f 100 --fixed-file-size 1M --base-file-name newfile %s"
            % (self.script_upload_path,
               self.mounts[0].mountpoint))
        proc = g.run_async(
            self.mounts[0].client_system, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete and validate IO
        # This should fail as the quotas were already reached
        self.assertFalse(
            validate_io_procs(self.all_mounts_procs, self.mounts[0]),
            "Unexpected: IO passed on the client even after quota is reached")
        g.log.info("Expected: IO failed as quota is reached")

        # Wait for rebalance to finish
        ret = wait_for_rebalance_to_complete(
            self.mnode, self.volname, timeout=180)
        self.assertTrue(ret, "Unexpected: Rebalance did not complete")
        g.log.info("Rebalance completed as expected")
