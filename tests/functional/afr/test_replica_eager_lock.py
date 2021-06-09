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

"""
Test Cases in this module related to Glusterd volume status while
IOs in progress
"""
import random

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs, wait_for_io_to_complete,
                                 list_all_files_and_dirs_mounts)
from glustolibs.gluster.profile_ops import (profile_start, profile_stop,
                                            profile_info)
from glustolibs.gluster.volume_ops import get_volume_options


@runs_on([['replicated'], ['glusterfs']])
class VolumeStatusWhenIOInProgress(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.counter = 1
        cls.get_super_method(cls, 'setUpClass')()

        # checking for peer status from every node
        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peer probe failed ")

        # Uploading file_dir script in all client direcotries
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

    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume()
        if ret:
            g.log.info("Volme created successfully : %s", self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
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

        ret, _, _ = profile_stop(random.choice(self.servers), self.volname)
        self.assertEqual(ret, 0, (
            "Volume profile failed to stop for volume %s" % self.volname))

        # unmounting the volume and Cleaning up the volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if ret:
            g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_eagerlock_while_io_in_progress(self):
        '''
        Create replica volume then mount the volume, once
        volume mounted successfully on client, start running IOs on
        mount point then run the "gluster volume <volname> profile info"
        command on all clusters randomly.
        Then check that IOs completed successfully or not on mount point.
        Check that files in mount point listing properly or not.
        check the release directory value should be less or equals '4'
        '''

        status_on = "on"
        validate_profiles = ('cluster.eager-lock',
                             'diagnostics.count-fop-hits',
                             'diagnostics.latency-measurement')

        ret, _, _ = profile_start(random.choice(self.servers), self.volname)
        self.assertEqual(ret, 0, (
            "Volume profile failed to start for volume %s" % self.volname))

        for validate_profile in validate_profiles:
            out = get_volume_options(
                random.choice(self.servers), self.volname,
                option=(validate_profile))
            self.assertIsNotNone(out, "Volume get failed for volume "
                                 "%s" % self.volname)
            self.assertEqual(out[validate_profile], status_on, "Failed to "
                             "match profile information")

        # Mounting a volume
        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, "Volume mount failed for %s" % self.volname)

        # run IOs
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 15 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 25 %s" % (self.script_upload_path,
                                             self.counter,
                                             mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10
        self.io_validation_complete = False

        # this command should not get hang while io is in progress
        # pylint: disable=unused-variable
        for i in range(20):
            ret, _, _ = profile_info(
                random.choice(self.servers), self.volname)
            self.assertEqual(ret, 0, ("Volume profile info failed on "
                                      "volume %s" % self.volname))

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

        volume_profile_info = "gluster v profile %s info"
        _, out, _ = g.run(random.choice(self.servers),
                          volume_profile_info % self.volname + " | grep"
                          "OPENDIR | awk '{print$8}'")
        self.assertIsNotNone(out, "Failed to get volume %s profile info" %
                             self.volname)
        out.strip().split('\n')
        for value in out:
            self.assertLessEqual(value, '4', "Failed to Validate profile"
                                 " on volume %s" % self.volname)
