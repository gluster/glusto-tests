#  Copyright (C) 2017-2018 Red Hat, Inc. <http://www.redhat.com>
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

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.constants import \
    TEST_LAYOUT_IS_COMPLETE as LAYOUT_IS_COMPLETE
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    get_rebalance_status,
    rebalance_start,
    rebalance_status,
    wait_for_rebalance_to_complete
)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    log_volume_info_and_status,
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online
)
from glustolibs.io.utils import (
    list_all_files_and_dirs_mounts,
    validate_io_procs,
    wait_for_io_to_complete
)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
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

    def setUp(self):
        """
        - Setup Volume and Mount Volume
        - setUp starts the io from all the mounts.
        - IO creates deep dirs and files.
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []

        # Value of counter(index) is used for dirname-start-num argument for
        # file_dir_ops.py create_deep_dirs_with_files.

        # The --dir-length argument value for
        # file_dir_ops.py create_deep_dirs_with_files is set to 10
        # (refer to the cmd in setUp method). This means every mount
        #  will create
        # 10 top level dirs. For every mountpoint/testcase
        # to create new set of
        # dirs, we are incrementing the counter by --dir-length value i.e 10
        # in this test suite.

        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

        for index, mount_obj in enumerate(self.mounts, start=1):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       sys.version_info.major, self.script_upload_path,
                       index + 10, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

    def tearDown(self):
        """If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Unmount Volume and Cleanup Volume
        """
        # Wait for IO to complete if io validation is not executed in the
        # test method
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO on all mounts: Success")

            # List all files and dirs created
            g.log.info("List all files and directories:")
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories: Successful")

        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Unmount Volume and Cleanup Volume: Fail")
        g.log.info("Unmount Volume and Cleanup Volume: Success")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_expanding_volume_when_io_in_progress(self):
        # pylint: disable=too-many-statements
        # Log Volume Info and Status before expanding the volume.
        g.log.info("Logging volume info and Status before expanding volume")
        log_volume_info_and_status(self.mnode, self.volname)

        # Expanding volume by adding bricks to the volume when IO in progress
        g.log.info("Start adding bricks to volume when IO in progress")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume while IO in "
                              "progress on volume %s", self.volname))
        g.log.info("Expanding volume while IO in progress on "
                   "volume %s : Success", self.volname)

        # Wait for gluster processes to come online
        g.log.info("Wait for gluster processes to come online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Waiting for volume %s process to be online", self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        log_volume_info_and_status(self.mnode, self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Start Rebalance
        g.log.info("Starting Rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Started rebalance on the volume %s: Success",
                   self.volname)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=600)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance status on volume %s: Complete",
                   self.volname)

        # Check Rebalance status after rebalance is complete
        g.log.info("Checking Rebalance status")
        ret, _, _ = rebalance_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to get rebalance status for the "
                                  "volume %s", self.volname))
        g.log.info("Rebalance status on volume %s: Complete",
                   self.volname)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO on all mounts: Complete")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("List all files and directories: Success")

        # DHT Layout validation
        g.log.debug("Verifying hash layout values %s:%s",
                    self.clients[0], self.mounts[0].mountpoint)
        ret = validate_files_in_dir(self.clients[0], self.mounts[0].mountpoint,
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "LAYOUT_IS_COMPLETE: FAILED")
        g.log.info("LAYOUT_IS_COMPLETE: PASS")

        # Checking if there are any migration failures
        status = get_rebalance_status(self.mnode, self.volname)
        for each_node in status['node']:
            self.assertEqual(0, int(each_node['failures']),
                             "Rebalance failed to migrate few files on %s" %
                             each_node['nodeName'])
            g.log.info("No migration failures on %s", each_node['nodeName'])
