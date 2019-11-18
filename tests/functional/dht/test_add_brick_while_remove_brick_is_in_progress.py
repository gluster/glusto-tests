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


from glusto.core import Glusto as g

from glustolibs.gluster.brick_ops import add_brick, remove_brick
from glustolibs.gluster.constants import \
    TEST_LAYOUT_IS_COMPLETE as LAYOUT_IS_COMPLETE
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import get_remove_brick_status
from glustolibs.gluster.volume_libs import (form_bricks_list_to_add_brick,
                                            form_bricks_list_to_remove_brick,
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
        GlusterBaseClass.setUpClass.im_func(cls)

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
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Form brick list for expanding volume
        self.add_brick_list = form_bricks_list_to_add_brick(
            self.mnode, self.volname, self.servers, self.all_servers_info,
            distribute_count=1)
        if not self.add_brick_list:
            g.log.error("Volume %s: Failed to form bricks list for expand",
                        self.volname)
            raise ExecutionError("Volume %s: Failed to form bricks list for"
                                 "expand" % self.volname)
        g.log.info("Volume %s: Formed bricks list for expand", self.volname)

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        all_mounts_procs = []
        for index, mount_obj in enumerate(self.mounts, start=1):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 2 "
                   "--max-num-of-dirs 2 "
                   "--num-of-files 10 %s" % (self.script_upload_path,
                                             index + 10,
                                             mount_obj.mountpoint))
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
            # Shrink volume by removing bricks with option start
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
        GlusterBaseClass.tearDown.im_func(self)

    def test_add_brick_while_remove_brick_is_in_progress(self):
        # DHT Layout and hash validation
        g.log.debug("Verifying hash layout values %s:%s",
                    self.clients[0], self.mounts[0].mountpoint)
        ret = validate_files_in_dir(self.clients[0], self.mounts[0].mountpoint,
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "LAYOUT_IS_COMPLETE: FAILED")
        g.log.info("LAYOUT_IS_COMPLETE: PASS")

        # Log Volume Info and Status before shrinking the volume.
        g.log.info("Logging volume info and Status before shrinking volume")
        log_volume_info_and_status(self.mnode, self.volname)

        # Form bricks list for volume shrink
        self.remove_brick_list = form_bricks_list_to_remove_brick(
            self.mnode, self.volname, subvol_name=1)
        self.assertIsNotNone(self.remove_brick_list, ("Volume %s: Failed to "
                                                      "form bricks list for "
                                                      "shrink", self.volname))
        g.log.info("Volume %s: Formed bricks list for shrink", self.volname)

        # Shrink volume by removing bricks
        g.log.info("Start removing bricks from volume")
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 self.remove_brick_list, "start")
        self.assertEqual(ret, 0, ("Volume %s shrink failed ",
                                  self.volname))
        g.log.info("Volume %s shrink started ", self.volname)
        # Log remove-brick status
        g.log.info("Logging Remove-brick status")
        ret, out, err = remove_brick(self.mnode, self.volname,
                                     self.remove_brick_list, "status")
        self.assertEqual(ret, 0, ("Remove-brick status failed on %s ",
                                  self.volname))
        g.log.info("Remove-brick status %s", self.volname)
        g.log.info(out)

        # Expanding volume while volume shrink is in-progress
        g.log.info("Volume %s: Expand volume while volume shrink in-progress",
                   self.volname)
        _, _, err = add_brick(self.mnode, self.volname, self.add_brick_list)
        self.assertIn("rebalance is in progress", err, "Successfully added"
                      "bricks to the volume <NOT EXPECTED>")
        g.log.info("Volume %s: Failed to add-bricks while volume shrink "
                   "in-progress <EXPECTED>", self.volname)

        # cleanup add-bricks list
        for brick in self.add_brick_list:
            brick_node, brick_path = brick.split(":")
            ret, _, _ = g.run(brick_node, ("rm -rf %s", brick_path))
            if ret != 0:
                g.log.error("Failed to clean %s:%s", brick_node, brick_path)
        g.log.info("Successfully cleaned backend add-brick bricks list")
