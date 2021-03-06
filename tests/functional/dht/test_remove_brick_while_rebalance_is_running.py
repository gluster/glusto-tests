#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.constants import \
    TEST_LAYOUT_IS_COMPLETE as LAYOUT_IS_COMPLETE
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    get_rebalance_status, rebalance_start, rebalance_stop
)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    form_bricks_list_to_remove_brick,
    log_volume_info_and_status,
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online
)
from glustolibs.io.utils import (
    list_all_files_and_dirs_mounts,
    wait_for_io_to_complete
)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'replicated'],
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

        # Form bricks list for Shrinking volume
        self.remove_brick_list = form_bricks_list_to_remove_brick(
            self.mnode, self.volname, subvol_name=1)

        if not self.remove_brick_list:
            g.log.error("Volume %s: Failed to form bricks list for shrink",
                        self.volname)
            raise ExecutionError("Volume %s: Failed to form bricks list for"
                                 " shrink" % self.volname)
        g.log.info("Volume %s: Formed bricks list for volume shrink",
                   (self.remove_brick_list, self.volname))

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for index, mount_obj in enumerate(self.mounts, start=1):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 10 %s" % (
                       self.script_upload_path,
                       index + 10, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Wait for IO to complete
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

    def tearDown(self):

        status_info = get_rebalance_status(self.mnode, self.volname)
        status = status_info['aggregate']['statusStr']
        if 'in progress' in status:
            # Stop rebalance on the volume
            g.log.info("Stop Rebalance on volume %s", self.volname)
            ret, _, _ = rebalance_stop(self.mnode, self.volname)
            if ret != 0:
                raise ExecutionError("Volume %s: Rebalance stop failed" %
                                     self.volname)
            g.log.info("Volume %s: Rebalance stop success", self.volname)

        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_remove_brick_while_rebalance_is_running(self):

        # DHT Layout validation
        g.log.debug("Verifying hash layout values %s:%s",
                    self.clients[0], self.mounts[0].mountpoint)
        ret = validate_files_in_dir(self.clients[0], self.mounts[0].mountpoint,
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "LAYOUT_IS_COMPLETE: FAILED")
        g.log.info("LAYOUT_IS_COMPLETE: PASS")

        # Log Volume Info and Status before expanding the volume.
        g.log.info("Logging volume info and Status before expanding volume")
        log_volume_info_and_status(self.mnode, self.volname)

        # Expanding volume by adding bricks to the volume
        g.log.info("Start adding bricks to volume")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Volume %s: Expand failed", self.volname))
        g.log.info("Volume %s: Expand successful", self.volname)

        # Wait for gluster processes to come online
        g.log.info("Wait for gluster processes to come online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s: one or more volume process are "
                              "not up", self.volname))
        g.log.info("All volume %s processes are online", self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        log_volume_info_and_status(self.mnode, self.volname)

        # Verify volume's all process are online
        g.log.info("Volume %s: Verifying that all process are online",
                   self.volname)
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online ",
                              self.volname))
        g.log.info("Volume %s: All process are online", self.volname)

        # Start Rebalance
        g.log.info("Starting rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Volume %s: Failed to start rebalance",
                                  self.volname))
        g.log.info("Volume %s: Rebalance started ", self.volname)

        # Check if rebalance is running
        status_info = get_rebalance_status(self.mnode, self.volname)
        status = status_info['aggregate']['statusStr']
        if 'in progress' in status:
            # Shrinking volume by removing bricks
            g.log.info("Start removing bricks from volume")
            _, _, err = remove_brick(self.mnode, self.volname,
                                     self.remove_brick_list, "start")
            self.assertIn("Rebalance is in progress", err, "Successfully "
                          "removed bricks while volume rebalance is "
                          "in-progress")
            g.log.info("Failed to start remove-brick as rebalance is "
                       "in-progress")
        else:
            g.log.error("Rebalance process is not running")
            raise ExecutionError("Rebalance process is not running")
