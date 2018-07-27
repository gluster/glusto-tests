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

"""
Test Cases in this module related to Glusterd volume status while
IOs in progress
"""
import random
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs, wait_for_io_to_complete,
                                 list_all_files_and_dirs_mounts)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class VolumeStatusWhenIOInProgress(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.counter = 1
        GlusterBaseClass.setUpClass.im_func(cls)

        # checking for peer status from every node
        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peer probe failed ")
        else:
            g.log.info("All server peers are already in connected state "
                       "%s:", cls.servers)

        # Uploading file_dir script in all client direcotries
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
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume()
        if ret:
            g.log.info("Volme created successfully : %s", self.volname)
        else:
            raise ExecutionError("Volume creation failed: %s" % self.volname)

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

        # unmounting the volume and Cleaning up the volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if ret:
            g.log.info("Volume deleted successfully : %s", self.volname)
        else:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_volume_status_inode_while_io_in_progress(self):
        '''
        Create any type of volume then mount the volume, once
        volume mounted successfully on client, start running IOs on
        mount point then run the "gluster volume status volname inode"
        command on all clusters randomly.
            "gluster volume status volname inode" command should not get
        hang while IOs in progress.
        Then check that IOs completed successfully or not on mount point.
        Check that files in mount point listing properly or not.
        '''

        # Mounting a volume
        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, "Volume mount failed for %s" % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)

        # After Mounting immediately writing IO's are failing some times,
        # that's why keeping sleep for 10 secs
        sleep(10)

        # run IOs
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
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

        # performing  "gluster volume status volname inode" command on
        # all cluster servers randomly while io is in progress,
        # this command should not get hang while io is in progress
        # pylint: disable=unused-variable
        for i in range(20):
            ret, _, _ = g.run(random.choice(self.servers),
                              "gluster --timeout=12000 volume status %s "
                              "inode" % self.volname)
            self.assertEqual(ret, 0, ("Volume status 'inode' failed on "
                                      "volume %s" % self.volname))
            g.log.info("Successful in logging volume status"
                       "'inode' of volume %s", self.volname)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")
