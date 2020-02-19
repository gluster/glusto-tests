#  Copyright (C) 2016-2020  Red Hat, Inc. <http://www.redhat.com>
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

""" Description:
        Test Cases in this module tests the client side quorum.
"""
from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (
    set_volume_options, get_subvols)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_libs import (bring_bricks_offline)
from glustolibs.io.utils import (is_io_procs_fail_with_error)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs']])
class ClientSideQuorumTests(GlusterBaseClass):
    """
    ClientSideQuorumTests contains tests which verifies the
    client side quorum Test Cases
    """
    @classmethod
    def setUpClass(cls):
        """
        Upload the necessary scripts to run tests.
        """

        # calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        # stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_client_side_quorum_with_auto_option(self):
        """
        Test Script to verify the Client Side Quorum with auto option

        * set cluster.quorum-type to auto.
        * start I/O from the mount point.
        * kill 2 of the brick process from the each and every replica set
        * perform ops

        """
        # pylint: disable=too-many-branches,too-many-statements
        # set cluster.quorum-type to auto
        options = {"cluster.quorum-type": "auto"}
        g.log.info("setting cluster.quorum-type to auto on "
                   "volume %s", self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for"
                              "volume %s" % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # write files on all mounts
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        cmd = ("/usr/bin/env python %s create_files "
               "-f 10 --base-file-name file %s" % (
                   self.script_upload_path,
                   self.mounts[0].mountpoint))
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "IO failed on %s with %s"
                         % (self.mounts[0].client_system, err))

        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s:", num_subvols)

        # bring bricks offline( 2 bricks ) for all the subvolumes
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            g.log.info("sub-volume %s brick list : %s", i, subvol_brick_list)
            # For volume type: 1 * 2, bring 1 brick offline
            if len(subvol_brick_list) == 2:
                bricks_to_bring_offline = subvol_brick_list[0:1]
            else:
                bricks_to_bring_offline = subvol_brick_list[0:2]
            g.log.info("Going to bring down the brick process "
                       "for %s", bricks_to_bring_offline)
            ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
            self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                                  "check the log file for more details."))
            g.log.info("Brought down the brick process "
                       "for %s successfully", bricks_to_bring_offline)

        # create a file test_file
        # cannot use python module here since we need the stderr output
        all_mounts_procs = []
        g.log.info("Start creating 2 files on mountpoint...")
        cmd = ("dd if=/dev/urandom of=%s/test_file bs=1M count=1"
               % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while creating files")

        # create directory user1
        all_mounts_procs = []
        g.log.info("Start creating directory...")
        cmd = ("mkdir %s/user1 " % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while creating folder")

        # create h/w link to file
        all_mounts_procs = []
        g.log.info("Start creating hard link for file0.txt")
        cmd = ("ln %s/file0.txt %s/file0.txt_hwlink"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while creating hardlink")

        # create s/w link
        all_mounts_procs = []
        g.log.info("Start creating soft link for file1.txt")
        cmd = ("ln -s %s/file1.txt %s/file1.txt_swlink"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while creating softlink")

        # append to file
        all_mounts_procs = []
        g.log.info("Appending to file1.txt")
        cmd = ("cat %s/file0.txt >> %s/file1.txt"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while appending to file")

        # modify the file
        all_mounts_procs = []
        g.log.info("Modifying file1.txt")
        cmd = ("echo 'Modify Contents' > %s/file1.txt"
               % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while modifying file")

        # truncate the file
        all_mounts_procs = []
        g.log.info("Truncating file1.txt")
        cmd = "truncate -s 0 %s/file1.txt" % self.mounts[0].mountpoint
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while truncating file")

        # read the file
        all_mounts_procs = []
        g.log.info("Starting reading file")
        cmd = ("cat %s/file1.txt" % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while reading file")

        # stat on file
        all_mounts_procs = []
        g.log.info("stat on file1.txt")
        cmd = "stat %s/file1.txt" % self.mounts[0].mountpoint
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while stat file")

        # stat on dir
        all_mounts_procs = []
        g.log.info("stat on %s", self.mounts[0].mountpoint)
        cmd = "stat %s" % self.mounts[0].mountpoint
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while stat dir")

        # ls on mount point
        all_mounts_procs = []
        g.log.info("ls on %s", self.mounts[0].mountpoint)
        cmd = "ls %s" % self.mounts[0].mountpoint
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while ls mountpoint")
