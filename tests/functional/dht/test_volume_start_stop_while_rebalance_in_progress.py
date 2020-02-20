#  Copyright (C) 2017-2020 Red Hat, Inc. <http://www.redhat.com>
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

from glustolibs.gluster.constants import FILETYPE_DIRS, FILETYPE_FILES
from glustolibs.gluster.constants import \
    TEST_FILE_EXISTS_ON_HASHED_BRICKS as FILE_ON_HASHED_BRICKS
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    get_rebalance_status,
    rebalance_start
)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    log_volume_info_and_status,
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online
)
from glustolibs.gluster.volume_ops import get_volume_info, volume_stop
from glustolibs.io.utils import (
    list_all_files_and_dirs_mounts,
    wait_for_io_to_complete
)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed', 'distributed-replicated',
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
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        # The --dir-length argument value for
        # file_dir_ops.py create_deep_dirs_with_files is set to 10
        # (refer to the cmd in setUp method). This means every mount will
        # create
        # 10 top level dirs. For every mountpoint/testcase to create new set of
        # dirs, we are incrementing the counter by --dir-length value i.e 10
        # in this test suite.
        #
        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        cls.all_mounts_procs = []
        for index, mount_obj in enumerate(cls.mounts, start=1):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 1 "
                   "--dir-length 2 "
                   "--max-num-of-dirs 2 "
                   "--num-of-files 55 %s" % (
                       cls.script_upload_path,
                       index + 10, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            cls.all_mounts_procs.append(proc)
        cls.io_validation_complete = False

        # Wait for IO to complete
        if not cls.io_validation_complete:
            g.log.info("Wait for IO to complete")
            ret = wait_for_io_to_complete(cls.all_mounts_procs, cls.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

            # List all files and dirs created
            g.log.info("List all files and directories:")
            ret = list_all_files_and_dirs_mounts(cls.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

    def test_volume_start_stop_while_rebalance_is_in_progress(self):
        # DHT Layout and hash validation
        for mount_obj in self.mounts:
            g.log.debug("Verifying hash layout values %s:%s",
                        mount_obj.client_system, mount_obj.mountpoint)
            ret = validate_files_in_dir(mount_obj.client_system,
                                        mount_obj.mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS,
                                        file_type=FILETYPE_FILES |
                                        FILETYPE_DIRS)
            self.assertTrue(ret, "Hash Layout Values: Fail")
            g.log.info("Hash layout values are verified %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

        # Log Volume Info and Status before expanding the volume.
        g.log.info("Logging volume info and Status before expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        g.log.error(ret, "Logging volume info and status failed on "
                         "volume %s", self.volname)
        g.log.info("Logging volume info and status was successful for volume "
                   "%s", self.volname)

        # Expanding volume by adding bricks to the volume
        g.log.info("Start adding bricks to volume")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info,)
        self.assertTrue(ret, ("Failed to expand the volume on volume %s ",
                              self.volname))
        g.log.info("Expanding volume is successful on volume %s", self.volname)

        # Wait for gluster processes to come online
        g.log.info("Wait for gluster processes to come online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Error: Volume processes failed to come up for "
                              "%s", self.volname))
        g.log.info("All processes are up for volume %s", self.volname)

        # Wait for gluster processes to come online
        g.log.info("Wait for gluster processes to come online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Error: Volume processes failed to come up for "
                              "%s", self.volname))
        g.log.info("All processes are up for volume %s", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online ",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Start Rebalance
        g.log.info("Starting rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s ",
                   self.volname)

        # Logging rebalance status
        g.log.info("Logging rebalance status")
        status_info = get_rebalance_status(self.mnode, self.volname)
        status = status_info['aggregate']['statusStr']

        self.assertIn('in progress', status,
                      "Rebalance process is not running")
        g.log.info("Rebalance process is running")

        ret, out, err = volume_stop(self.mnode, self.volname)
        g.log.debug("Rebalance info: %s", out)

        self.assertIn("rebalance session is in progress", err, " Volume "
                      "stopped successfully while rebalance session is in "
                      "progress")
        g.log.info("Volume stop failed as rebalance session is in "
                   "progress")

        # Check volume info to check the status of volume
        g.log.info("Checking volume info for the volume status")
        status_info = get_volume_info(self.mnode, self.volname)
        status = status_info[self.volname]['statusStr']
        self.assertIn('Started', status, ("Volume %s state is \"Stopped\"",
                                          self.volname))
        g.log.info("Volume %s state is \"Started\"", self.volname)

    @classmethod
    def tearDownClass(cls):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Volume %s unmount and cleanup: Success", cls.volname)

        # Calling GlusterBaseClass tearDownClass
        cls.get_super_method(cls, 'tearDownClass')()
