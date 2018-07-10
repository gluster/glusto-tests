#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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
    Description: BVT-Volume Verification Tests (VVT). Tests the Basic
    Volume Operations like start, status, stop, delete.

"""

import pytest
from glusto.core import Glusto as g
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_init import is_glusterd_running
from glustolibs.gluster.volume_ops import volume_stop, volume_start
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.volume_libs import log_volume_info_and_status
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, get_mounts_stat


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class VolumeAccessibilityTests(GlusterBaseClass):
    """ VolumeAccessibilityTests contains tests which verifies
        accessablity of the volume.
    """
    @classmethod
    def setUpClass(cls):
        """Upload the necessary scripts to run tests.
        """
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
        """Setup Volume
        """
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Setup_Volume
        g.log.info("Starting to Setup Volume %s", self.volname)
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup Volume %s" % self.volname)
        g.log.info("Successful in Setup Volume %s", self.volname)

    def tearDown(self):
        """Cleanup the volume
        """
        # Cleanup Volume
        g.log.info("Starting to Setup Volume %s", self.volname)
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup_Volume %s" % self.volname)
        g.log.info("Successful in Setup Volume %s", self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    @pytest.mark.bvt_vvt
    def test_volume_create_start_stop_start(self):
        """Tests volume create, start, status, stop, start.
        Also Validates whether all the brick process are running after the
        start of the volume.
        """
        # Verify volume processes are online
        g.log.info("Verify volume %s processes are online", self.volname)
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online" %
                              self.volname))
        g.log.info("Successfully Verified volume %s processes are online",
                   self.volname)

        # Stop Volume
        g.log.info("Stopping Volume %s", self.volname)
        ret, _, _ = volume_stop(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Failed to stop volume %s" % self.volname)
        g.log.info("Successfully stopped volume %s", self.volname)

        # Start Volume
        g.log.info("Starting Volume %s", self.volname)
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start volume %s" % self.volname)
        g.log.info("Successfully started volume %s", self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Log Volume Info and Status
        g.log.info("Logging Volume %s Info and Status", self.volname)
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to Log volume %s info and status",
                              self.volname))
        g.log.info("Successfully logged Volume %s Info and Status",
                   self.volname)

        # Verify volume's all process are online
        g.log.info("Verify volume %s processes are online", self.volname)
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online" %
                              self.volname))
        g.log.info("Successfully verified volume %s processes are online",
                   self.volname)

        # Log Volume Info and Status
        g.log.info("Logging Volume %s Info and Status", self.volname)
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to Log volume %s info and status",
                              self.volname))
        g.log.info("Successfully logged Volume %s Info and Status",
                   self.volname)

        # Check if glusterd is running on all servers(expected: active)
        g.log.info("Check if glusterd is running on all servers"
                   "(expected: active)")
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, "Glusterd is not running on all servers")
        g.log.info("Glusterd is running on all the servers")

    @pytest.mark.bvt_vvt
    def test_file_dir_create_ops_on_volume(self):
        """Test File Directory Creation on the volume.
        """
        # Mount Volume
        g.log.info("Starting to Mount Volume %s", self.volname)
        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, ("Failed to Mount Volume %s", self.volname))
        g.log.info("Successful in Mounting Volume %s", self.volname)

        # Start IO on all mounts.
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (self.script_upload_path, count,
                                            mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Validate IO
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # UnMount Volume
        g.log.info("Starting to Unmount Volume %s", self.volname)
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, ("Failed to Unmount Volume %s" % self.volname))
        g.log.info("Successfully Unmounted Volume %s", self.volname)

    def _check_any_stale_space_present(self):
        # Get all bricks
        directories = []
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, ("Failed to get the brick list"
                                          "of volume %s" % self.volname))
        # Run ls .glusterfs/ inside all the bricks
        for brick in brick_list:
            brick_node, brick_path = brick.split(":")
            cmd1 = ("cd %s; ls .glusterfs/" % brick_path)
            ret, list_dir, _ = g.run(brick_node, cmd1)
            self.assertEqual(ret, 0, ("Failed to run cmd on node"
                                      "%s" % brick_node))
            g.log.info("Succeful in ruuning cmd and the output"
                       "is %s", list_dir)
            directories.append(list_dir)
        return directories

    @pytest.mark.bvt_vvt
    def test_volume_sanity(self):
        """
        This test verifies that files/directories creation or
        deletion doesn't leave behind any stale spaces
        """
        self.all_mounts_procs = []
        # Mount Volume
        g.log.info("Starting to Mount Volume %s", self.volname)
        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, ("Failed to Mount Volume %s" % self.volname))
        g.log.info("Successful in Mounting Volume %s", self.volname)

        # Get the list of directories under .glusterfs before
        # creating any files
        before_creating_files = self._check_any_stale_space_present()

        # Creating files on client side
        for mount_object in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("python %s create_files -f 100 --fixed-file-size 1k %s"
                       % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Creating directories in the mount point
        for mount_object in self.mounts:
            g.log.info("Creating Directories on %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            cmd = ("python %s create_deep_dir -d 0 -l 10 %s"
                   % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, cmd,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Creating hard links
        for mount_object in self.mounts:
            g.log.info("Creating hard links on %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            cmd = ("python %s create_hard_links --dest-dir %s %s"
                   % (self.script_upload_path, mount_object.mountpoint,
                      mount_object.mountpoint))
            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Get stat of all the files/dirs created.
        g.log.info("Get stat of all the files/dirs created.")
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfully got stat of all files/dirs created")

        # Remove the files, directories and hard-links which created
        for mount_object in self.mounts:
            cmd = ("cd %s; rm -rf *" % mount_object.mountpoint)
            g.log.info("Running cmd %s nodes %s",
                       cmd, mount_object.client_system)
            ret, _, _ = g.run(mount_object.client_system, cmd)
            self.assertEqual(ret, 0, "Failed to delete the files/dir on"
                             " %s" % mount_object.client_system)
        g.log.info("Succesfully deleted all the files")

        # Get the list of directories under .glusterfs after
        # deleting files on client side
        after_del_files = self._check_any_stale_space_present()

        # Compare the output before and after running io's
        self.assertListEqual(before_creating_files, after_del_files,
                             "Both list are not equal.\n Before creating"
                             " file:%s\n After deleting file: "
                             "%s" % (before_creating_files, after_del_files))
        g.log.info("Both list are equal. Before creating file:%s "
                   "After deleting file :%s", before_creating_files,
                   after_del_files)

        # UnMount Volume
        g.log.info("Starting to Unmount Volume %s", self.volname)
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, ("Failed to Unmount Volume %s" % self.volname))
        g.log.info("Successfully Unmounted Volume %s", self.volname)
