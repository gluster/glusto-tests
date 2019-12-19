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

from glustolibs.gluster.constants import FILETYPE_FILES
from glustolibs.gluster.constants import \
    TEST_FILE_EXISTS_ON_HASHED_BRICKS as FILE_ON_HASHED_BRICKS
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (get_rebalance_status,
                                              rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    log_volume_info_and_status,
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.io.utils import (collect_mounts_arequal,
                                 list_all_files_and_dirs_mounts,
                                 validate_io_procs)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed', 'dispersed', 'replicated',
           'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs']])
class RebalanceValidation(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            clients = ", ".join(cls.clients)
            g.log.error("Failed to upload IO scripts to clients %s",
                        clients)
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            g.log.error("Failed to Setup_Volume and Mount_Volume")
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_rebalance_with_hidden_files(self):
        # pylint: disable=too-many-statements
        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_files "
                   "--base-file-name . -f 99 %s" % (
                       sys.version_info.major, self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

        # Verify DHT values across mount points
        for mount_obj in self.mounts:
            g.log.debug("Verifying hash layout values %s:%s",
                        mount_obj.client_system, mount_obj.mountpoint)
            ret = validate_files_in_dir(mount_obj.client_system,
                                        mount_obj.mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS,
                                        file_type=FILETYPE_FILES)
            self.assertTrue(ret, "Expected - Files are created on only "
                                 "sub-volume according to its hashed value")
            g.log.info("Hash layout values are verified %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

        # Getting areequal checksum before rebalance
        g.log.info("Getting areequal checksum before rebalance")
        arequal_checksum_before_rebalance = collect_mounts_arequal(self.mounts)

        # Log Volume Info and Status before expanding the volume.
        g.log.info("Logging volume info and Status before expanding volume")
        log_volume_info_and_status(self.mnode, self.volname)

        # Expanding volume by adding bricks to the volume
        g.log.info("Start adding bricks to volume")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume %s", self.volname))
        g.log.info("Expanding volume is successful on "
                   "volume %s", self.volname)

        # Wait for gluster processes to come online
        g.log.info("Wait for gluster processes to come online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online ",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        log_volume_info_and_status(self.mnode, self.volname)

        # Start Rebalance
        g.log.info("Starting Rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        # Checking if there are any migration failures
        status = get_rebalance_status(self.mnode, self.volname)
        for each_node in status['node']:
            failed_files_count = int(each_node['failures'])
            self.assertEqual(failed_files_count, 0,
                             "Rebalance failed to migrate few files on %s" %
                             each_node['nodeName'])
            g.log.info("There are no migration failures")

        # Getting areequal checksum after rebalance
        g.log.info("Getting areequal checksum after rebalance")
        arequal_checksum_after_rebalance = collect_mounts_arequal(self.
                                                                  mounts)

        # Comparing arequals checksum before and after rebalance
        g.log.info("Comparing arequals checksum before and after rebalance")
        self.assertEqual(arequal_checksum_before_rebalance,
                         arequal_checksum_after_rebalance,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")
