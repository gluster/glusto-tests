#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
Description: Test cases related to afr snapshot.
"""
from time import sleep
import sys
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.snap_ops import (snap_create, snap_restore_complete)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.heal_libs import (is_heal_complete)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class TestAFRSnapshot(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
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
        self.all_mounts_procs, self.io_validation_complete = [], False

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
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
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

            # List all files and dirs created
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Checking brick dir and cleaning it.
        for brick_path in self.bricks_list:
            server, brick = brick_path.split(':')
            cmd = "rm -rf " + brick
            ret, _, _ = g.run(server, cmd)
            if ret:
                raise ExecutionError("Failed to delete the brick "
                                     "dirs of deleted volume.")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_entry_transaction_crash_consistency_create(self):
        """
        Test entry transaction crash consistency : create

        Description:
        - Create IO
        - Calculate arequal before creating snapshot
        - Create snapshot
        - Modify the data
        - Stop the volume
        - Restore snapshot
        - Start the volume
        - Get arequal after restoring snapshot
        - Compare arequals
        """

        # Creating files on client side
        count = 1
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python%d %s create_files "
                   "--base-file-name %d -f 200 %s"
                   % (sys.version_info.major, self.script_upload_path,
                      count, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            count = count + 10

        # Wait for IO to complete
        ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed to complete on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Get arequal before creating snapshot
        ret, result_before_snapshot = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, "Collecting arequal-checksum failed")

        # Create snapshot
        snapshot_name = ('entry_transaction_crash_consistency_create-%s-%s'
                         % (self.volname, self.mount_type))
        ret, _, err = snap_create(self.mnode, self.volname, snapshot_name)
        self.assertEqual(ret, 0, err)
        g.log.info("Snapshot %s created successfully", snapshot_name)

        # Modify the data
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python%d %s append %s"
                   % (sys.version_info.major, self.script_upload_path,
                      mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Wait for IO to complete
        ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed to complete on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Restore snapshot
        ret = snap_restore_complete(self.mnode, self.volname,
                                    snapshot_name)
        self.assertTrue(ret, 'Failed to restore snapshot %s'
                        % snapshot_name)
        g.log.info("Snapshot %s restored successfully", snapshot_name)

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Wait for volume graph to get loaded.
        sleep(10)

        # Get arequal after restoring snapshot
        ret, result_after_restoring = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, "Collecting arequal-checksum failed")

        # Checking arequal before creating snapshot
        # and after restoring snapshot
        self.assertEqual(result_before_snapshot, result_after_restoring,
                         'Checksums are not equal')
        g.log.info('Checksums are equal')

    def test_entry_transaction_crash_consistency_delete(self):
        """
        Test entry transaction crash consistency : delete

        Description:
        - Create IO of 50 files
        - Delete 20 files
        - Calculate arequal before creating snapshot
        - Create snapshot
        - Delete 20 files more
        - Stop the volume
        - Restore snapshot
        - Start the volume
        - Get arequal after restoring snapshot
        - Compare arequals
        """

        # Creating files on client side
        count = 1
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python%d %s create_files "
                   "--base-file-name %d -f 25 %s" % (
                       sys.version_info.major, self.script_upload_path,
                       count, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            count = count + 10

        # Wait for IO to complete
        ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed to complete on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Delete 20 files from the dir
        for mount_object in self.mounts:
            self.io_validation_complete = False
            g.log.info("Deleting files for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            command = ("for file in `ls -1 | head -n 20`;do "
                       "rm -rf %s/$file; done" % mount_object.mountpoint)

            ret, _, err = g.run(mount_object.client_system, command)
            self.assertEqual(ret, 0, err)
            self.io_validation_complete = True
            g.log.info("Deleted files for %s:%s successfully",
                       mount_object.client_system, mount_object.mountpoint)

        # Get arequal before creating snapshot
        ret, result_before_snapshot = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, "Collecting arequal-checksum failed")

        # Create snapshot
        snapshot_name = ('entry_transaction_crash_consistency_delete-%s-%s'
                         % (self.volname, self.mount_type))
        ret, _, err = snap_create(self.mnode, self.volname, snapshot_name)
        self.assertEqual(ret, 0, err)
        g.log.info("Snapshot %s created successfully", snapshot_name)

        # Delete all the remaining files
        for mount_object in self.mounts:
            self.io_validation_complete = False
            command = ("for file in `ls -1 | head -n 20`;do "
                       "rm -rf %s/$file; done" % mount_object.mountpoint)
            ret, _, err = g.run(mount_object.client_system, command)
            self.assertEqual(ret, 0, err)
            self.io_validation_complete = True
            g.log.info("Deleted files for %s:%s successfully",
                       mount_object.client_system, mount_object.mountpoint)

        # Restore snapshot
        ret = snap_restore_complete(self.mnode, self.volname,
                                    snapshot_name)
        self.assertTrue(ret, 'Failed to restore snapshot %s'
                        % snapshot_name)
        g.log.info("Snapshot %s restored successfully", snapshot_name)

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Wait for volume graph to get loaded.
        sleep(10)

        # Get arequal after restoring snapshot
        ret, result_after_restoring = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, "Collecting arequal-checksum failed")

        # Checking arequal before creating snapshot
        # and after restoring snapshot
        self.assertEqual(result_before_snapshot, result_after_restoring,
                         'Checksums are not equal')
        g.log.info('Checksums are equal')

    def test_entry_transaction_crash_consistency_rename(self):
        """
        Test entry transaction crash consistency : rename

        Description:
        - Create IO of 50 files
        - Rename 20 files
        - Calculate arequal before creating snapshot
        - Create snapshot
        - Rename 20 files more
        - Stop the volume
        - Restore snapshot
        - Start the volume
        - Get arequal after restoring snapshot
        - Compare arequals
        """

        # Creating files on client side
        count = 1
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python%d %s create_files "
                   "--base-file-name %d -f 25 %s"
                   % (sys.version_info.major, self.script_upload_path,
                      count, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            count = count + 10

        # Wait for IO to complete
        ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed to complete on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Rename files
        self.all_mounts_procs, self.io_validation_complete = [], False
        cmd = ("/usr/bin/env python%d %s mv -s FirstRename %s"
               % (sys.version_info.major, self.script_upload_path,
                  self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete
        ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts[0])
        self.assertTrue(ret, "IO failed to complete on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Get arequal before creating snapshot
        ret, result_before_snapshot = collect_mounts_arequal(self.mounts[0])
        self.assertTrue(ret, "Collecting arequal-checksum failed")

        # Create snapshot
        snapshot_name = ('entry_transaction_crash_consistency_rename-%s-%s'
                         % (self.volname, self.mount_type))
        ret, _, err = snap_create(self.mnode, self.volname, snapshot_name)
        self.assertEqual(ret, 0, err)
        g.log.info("Snapshot %s created successfully", snapshot_name)

        # Rename files
        self.all_mounts_procs, self.io_validation_complete = [], False
        cmd = ("/usr/bin/env python%d %s mv -s SecondRename %s"
               % (sys.version_info.major, self.script_upload_path,
                  self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete
        ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts[0])
        self.assertTrue(ret, "IO failed to complete on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Restore snapshot
        ret = snap_restore_complete(self.mnode, self.volname,
                                    snapshot_name)
        self.assertTrue(ret, 'Failed to restore snapshot %s'
                        % snapshot_name)
        g.log.info("Snapshot %s restored successfully", snapshot_name)

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Wait for volume graph to get loaded.
        sleep(10)

        # Get arequal after restoring snapshot
        ret, result_after_restoring = collect_mounts_arequal(self.mounts[0])
        self.assertTrue(ret, "Collecting arequal-checksum failed")

        # Checking arequal before creating snapshot
        # and after restoring snapshot
        self.assertEqual(result_before_snapshot, result_after_restoring,
                         'Checksums are not equal')
        g.log.info('Checksums are equal')
