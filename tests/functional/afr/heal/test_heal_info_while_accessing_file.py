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

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           are_bricks_offline,
                                           get_all_bricks)

from glustolibs.gluster.heal_ops import get_heal_info_summary
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)


@runs_on([['distributed-replicated'],
          ['glusterfs']])
class TestSelfHeal(GlusterBaseClass):
    """
    Description:
        Test cases related to
        healing in default configuration of the volume
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [script_local_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        cls.counter = 1
        # int: Value of counter is used for dirname-start-num argument for
        # file_dir_ops.py create_deep_dirs_with_files.

        # The --dir-length argument value for file_dir_ops.py
        # create_deep_dirs_with_files is set to 10 (refer to the cmd in setUp
        # method). This means every mount will create
        # 10 top level dirs. For every mountpoint/testcase to create new set of
        # dirs, we are incrementing the counter by --dir-length value i.e 10 in
        # this test suite.

        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        self.bricks_list = get_all_bricks(self.mnode, self.volname)

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Cleanup and umount volume
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

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_heal_info_shouldnot_list_files_being_accessed(self):
        """
        - bring brick 1 offline
        - create files and validate IO
        - get entries before accessing file
        - get first filename from active subvol without offline bricks
        - access and modify the file
        - while accessing - get entries
        - Compare entries before accessing and while accessing
        - validate IO
        """

        # Bring 1-st brick offline
        brick_to_bring_offline = [self.bricks_list[0]]
        g.log.info('Bringing bricks %s offline...', brick_to_bring_offline)
        ret = bring_bricks_offline(self.volname, brick_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline'
                        % brick_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 brick_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % brick_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   brick_to_bring_offline)

        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

            # Creating files
            cmd = "/usr/bin/env python%d %s create_files -f 100 %s" % (
                sys.version_info.major, self.script_upload_path,
                mount_obj.mountpoint)

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Get entries before accessing file
        g.log.info("Getting entries_before_accessing file...")
        entries_before_accessing = get_heal_info_summary(
            self.mnode, self.volname)
        self.assertNotEqual(entries_before_accessing, None,
                            'Can`t get heal info summary')
        g.log.info(
            "Getting entries_before_accessing file finished successfully")

        # Get filename to access from active subvol without offline bricks
        # Get last subvol
        subvols = get_subvols(self.mnode, self.volname)
        subvol_without_offline_brick = subvols['volume_subvols'][-1]

        # Get first brick server and brick path
        # and get first file from filelist
        subvol_mnode, mnode_brick = subvol_without_offline_brick[0].split(':')
        ret, file_list, _ = g.run(subvol_mnode, 'ls %s' % mnode_brick)
        file_to_edit = file_list.splitlines()[0]

        # Access and modify the file
        g.log.info("Start modifying IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Modifying IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)

            cmd = ("cd %s/ ; "
                   "dd if=/dev/zero of=%s bs=1G count=1"
                   % (mount_obj.mountpoint, file_to_edit))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            g.log.info("IO on %s:%s is modified successfully",
                       mount_obj.client_system, mount_obj.mountpoint)
        self.io_validation_complete = False

        # Get entries while accessing file
        g.log.info("Getting entries while accessing file...")
        entries_while_accessing = get_heal_info_summary(
            self.mnode, self.volname)
        self.assertNotEqual(entries_before_accessing, None,
                            'Can`t get heal info summary')
        g.log.info("Getting entries while accessing file "
                   "finished successfully")

        # Compare dicts before accessing and while accessing
        g.log.info('Comparing entries before modifying and while modifying...')
        self.assertDictEqual(entries_before_accessing, entries_while_accessing)
        g.log.info('Comparison entries before modifying and while modifying'
                   'finished successfully.')

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True
