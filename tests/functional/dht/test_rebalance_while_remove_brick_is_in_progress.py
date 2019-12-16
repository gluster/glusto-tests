#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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

from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.constants import \
    TEST_LAYOUT_IS_COMPLETE as LAYOUT_IS_COMPLETE
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (get_remove_brick_status,
                                              rebalance_start)
from glustolibs.gluster.volume_libs import (form_bricks_list_to_remove_brick,
                                            log_volume_info_and_status)
from glustolibs.io.utils import (list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed', 'distributed-replicated',
           'distributed-dispersed'],
          ['glusterfs']])
class RemoveBrickValidation(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

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
        all_mounts_procs = []
        for index, mount_obj in enumerate(self.mounts, start=1):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 15 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       sys.version_info.major, self.script_upload_path,
                       index + 10, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Wait for IO to complete
        g.log.info("Wait for IO to complete as IO validation did not "
                   "succeed in test method")
        ret = wait_for_io_to_complete(all_mounts_procs, self.mounts)
        if not ret:
            raise ExecutionError("IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        if not ret:
            raise ExecutionError("Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

    def tearDown(self):

        status_info = get_remove_brick_status(self.mnode, self.volname,
                                              bricks_list=self.
                                              remove_brick_list)
        status = status_info['aggregate']['statusStr']
        if 'in progress' in status:
            # Stop remove-brick operation
            g.log.info("Vol %s: Stop remove brick", self.volname)
            ret, _, _ = remove_brick(self.mnode, self.volname,
                                     self.remove_brick_list, "stop")
            g.log.info("Volume %s shrink stopped ", self.volname)

        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_rebalance_while_remove_brick_in_progress(self):
        """
        - Create directories and files on the mount point.
        -  now remove one of the brick from the volume
            gluster volume remove-brick <vol> <brick> start
        - immediately start rebalance on the same volume
            gluster volume rebalance <vol> start
        """
        # pylint: disable=too-many-statements
        # DHT Layout validation
        for mount in self.mounts:
            g.log.debug('Check DHT values %s:%s', mount.client_system,
                        mount.mountpoint)
            ret = validate_files_in_dir(self.clients[0], mount.mountpoint,
                                        test_type=LAYOUT_IS_COMPLETE,
                                        file_type=FILETYPE_DIRS)
            self.assertTrue(ret, "TEST_LAYOUT_IS_COMPLETE: FAILED")
            g.log.info("TEST_LAYOUT_IS_COMPLETE: PASS")

        # Log Volume Info and Status before shrinking the volume.
        g.log.info("Logging volume info and Status before shrinking volume")
        log_volume_info_and_status(self.mnode, self.volname)
        g.log.info("Successful in logging volume info and status of volume "
                   "%s", self.volname)

        # Form bricks list for Shrinking volume
        self.remove_brick_list = form_bricks_list_to_remove_brick(
            self.mnode, self.volname, subvol_name=1)
        self.assertIsNotNone(self.remove_brick_list, ("Volume %s: Failed to "
                                                      "form bricks list for "
                                                      "shrink", self.volname))
        g.log.info("Volume %s: Formed bricks list for shrink", self.volname)

        # Shrink volume by removing bricks with option start
        g.log.info("Start removing bricks for %s", self.volname)
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 self.remove_brick_list, "start")
        self.assertEqual(ret, 0, ("Volume %s: Remove-brick status failed",
                                  self.volname))
        g.log.info("Volume %s: Remove-brick start success ", self.volname)

        # Log remove-brick status
        g.log.info("Logging Remove-brick status")
        ret, out, err = remove_brick(self.mnode, self.volname,
                                     self.remove_brick_list, "status")
        self.assertEqual(ret, 0, ("Volume %s: Remove-brick status failed",
                                  self.volname))
        g.log.info("Volume %s: Remove-brick status", self.volname)
        g.log.info(out)

        # Start rebalance while volume shrink in-progress
        g.log.info("Volume %s: Start rebalance while volume shrink is "
                   "in-progress")
        _, _, err = rebalance_start(self.mnode, self.volname)
        self.assertIn("Either commit or stop the remove-brick task.", err,
                      "Rebalance started successfully while volume shrink"
                      " is in-progress")
        g.log.info("Failed to start rebalance while volume shrink is "
                   "in progress <EXPECTED>")
