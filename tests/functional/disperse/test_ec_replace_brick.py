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
Test Description:
    Tests replace brick on an EC volume
"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal,
                                 validate_io_procs)
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           wait_for_bricks_to_be_online,
                                           are_bricks_online)
from glustolibs.gluster.volume_libs import replace_brick_from_volume
from glustolibs.gluster.glusterfile import file_exists
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestEcBrickReplace(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [cls.script_upload_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_ec_replace_brick(self):
        """
        - Start resource consumption tool
        - Create directory dir1
        - Create 5 directory and 5 files in dir of mountpoint
        - Rename all files inside dir1 at mountpoint
        - Create softlink and hardlink of files in dir1 of mountpoint
        - Delete op for deleting all file in one of the dirs inside dir1
        - Change chmod, chown, chgrp
        - Create tiny, small, medium and large file
        - Get arequal before replacing brick
        - Replace brick
        - Get arequal after replacing brick
        - Compare Arequal's
        - Create IO's
        - Replace brick while IO's are going on
        - Validating IO's and waiting for it to complete
        """
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        # Starting resource consumption using top
        log_file_mem_monitor = '/var/log/glusterfs/mem_usage.log'
        cmd = ("for i in {1..20};do top -n 1 -b|egrep "
               "'RES|gluster' & free -h 2>&1 >> %s ;"
               "sleep 10;done" % (log_file_mem_monitor))
        g.log.info(cmd)
        cmd_list_procs = []
        for server in self.servers:
            proc = g.run_async(server, cmd)
            cmd_list_procs.append(proc)

        # Creating dir1
        ret = mkdir(self.mounts[0].client_system, "%s/dir1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, "Failed to create dir1")
        g.log.info("Directory dir1 on %s created successfully", self.mounts[0])

        # Create 5 dir and 5 files in each dir at mountpoint on dir1
        start, end = 1, 5
        for mount_obj in self.mounts:
            # Number of dir and files to be created.
            dir_range = ("%s..%s" % (str(start), str(end)))
            file_range = ("%s..%s" % (str(start), str(end)))
            # Create dir 1-5 at mountpoint.
            ret = mkdir(mount_obj.client_system, "%s/dir1/dir{%s}"
                        % (mount_obj.mountpoint, dir_range))
            self.assertTrue(ret, "Failed to create directory")
            g.log.info("Directory created successfully")

            # Create files inside each dir.
            cmd = ('touch %s/dir1/dir{%s}/file{%s};'
                   % (mount_obj.mountpoint, dir_range, file_range))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "File creation failed")
            g.log.info("File created successfull")

            # Increment counter so that at next client dir and files are made
            # with diff offset. Like at next client dir will be named
            # dir6, dir7...dir10. Same with files.
            start += 5
            end += 5

        # Rename all files inside dir1 at mountpoint on dir1
        cmd = ('cd %s/dir1/dir1/; '
               'for FILENAME in *;'
               'do mv $FILENAME Unix_$FILENAME; cd ~;'
               'done;'
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to rename file on "
                         "client")
        g.log.info("Successfully renamed file on client")

        # Truncate at any dir in mountpoint inside dir1
        # start is an offset to be added to dirname to act on
        # diff files at diff clients.
        start = 1
        for mount_obj in self.mounts:
            cmd = ('cd %s/dir1/dir%s/; '
                   'for FILENAME in *;'
                   'do echo > $FILENAME; cd ~;'
                   'done;'
                   % (mount_obj.mountpoint, str(start)))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Truncate failed")
            g.log.info("Truncate of files successfull")

        # Create softlink and hardlink of files in mountpoint. Start is an
        # offset to be added to dirname to act on diff files at diff clients.
        start = 1
        for mount_obj in self.mounts:
            cmd = ('cd %s/dir1/dir%s; '
                   'for FILENAME in *; '
                   'do ln -s $FILENAME softlink_$FILENAME; cd ~;'
                   'done;'
                   % (mount_obj.mountpoint, str(start)))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Creating Softlinks have failed")
            g.log.info("Softlink of files have been changed successfully")

            cmd = ('cd %s/dir1/dir%s; '
                   'for FILENAME in *; '
                   'do ln $FILENAME hardlink_$FILENAME; cd ~;'
                   'done;'
                   % (mount_obj.mountpoint, str(start + 1)))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Creating Hardlinks have failed")
            g.log.info("Hardlink of files have been changed successfully")
            start += 5

        # chmod, chown, chgrp inside dir1
        # start and end used as offset to access diff files
        # at diff clients.
        start, end = 2, 5
        for mount_obj in self.mounts:
            dir_file_range = '%s..%s' % (str(start), str(end))
            cmd = ('chmod 777 %s/dir1/dir{%s}/file{%s}'
                   % (mount_obj.mountpoint, dir_file_range, dir_file_range))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Changing mode of files has failed")
            g.log.info("Mode of files have been changed successfully")

            cmd = ('chown root %s/dir1/dir{%s}/file{%s}'
                   % (mount_obj.mountpoint, dir_file_range, dir_file_range))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Changing owner of files has failed")
            g.log.info("Owner of files have been changed successfully")

            cmd = ('chgrp root %s/dir1/dir{%s}/file{%s}'
                   % (mount_obj.mountpoint, dir_file_range, dir_file_range))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Changing group of files has failed")
            g.log.info("Group of files have been changed successfully")
            start += 5
            end += 5

        # Create tiny, small, medium and large file
        # at mountpoint. Offset to differ filenames
        # at diff clients.
        offset = 1
        for mount_obj in self.mounts:
            cmd = 'fallocate -l 100 tiny_file%s.txt' % str(offset)
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Fallocate for tiny files failed")
            g.log.info("Fallocate for tiny files successfully")

            cmd = 'fallocate -l 20M small_file%s.txt' % str(offset)
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Fallocate for small files failed")
            g.log.info("Fallocate for small files successfully")

            cmd = 'fallocate -l 200M medium_file%s.txt' % str(offset)
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Fallocate for medium files failed")
            g.log.info("Fallocate for medium files successfully")

            cmd = 'fallocate -l 1G large_file%s.txt' % str(offset)
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Fallocate for large files failed")
            g.log.info("Fallocate for large files successfully")
            offset += 1

        # Get arequal before replacing brick
        ret, result_before_replacing_brick = (
            collect_mounts_arequal(self.mounts[0]))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before replacing of brick '
                   'is successful')

        # Replacing a brick of random choice
        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        self.servers,
                                        self.all_servers_info)
        self.assertTrue(ret, "Unexpected:Replace brick is not successful")
        g.log.info("Expected : Replace brick is successful")

        # Wait for brick to come online
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Unexpected:Bricks are not online")
        g.log.info("Expected : Bricks are online")

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Unexpected:Heal has not yet completed')
        g.log.info('Heal has completed successfully')

        # Check if bricks are online
        all_bricks = get_all_bricks(self.mnode, self.volname)
        ret = are_bricks_online(self.mnode, self.volname, all_bricks)
        self.assertTrue(ret, 'Unexpected:All bricks are not online')
        g.log.info('All bricks are online')

        # Get areequal after replacing brick
        ret, result_after_replacing_brick = (
            collect_mounts_arequal(self.mounts[0]))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after replacing of brick '
                   'is successful')

        # Comparing arequals
        self.assertEqual(result_before_replacing_brick,
                         result_after_replacing_brick,
                         'Arequals are not equals before replacing '
                         'brick and after replacing brick')
        g.log.info('Arequals are equals before replacing brick '
                   'and after replacing brick')

        # Creating files on client side for dir1
        # Write IO
        all_mounts_procs, count = [], 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s/dir1" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count += 10

        # Replacing a brick while IO's are going on
        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        self.servers,
                                        self.all_servers_info)
        self.assertTrue(ret, "Unexpected:Replace brick is not successful")
        g.log.info("Expected : Replace brick is successful")

        # Wait for brick to come online
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Unexpected:Bricks are not online")
        g.log.info("Expected : Bricks are online")

        # Validating IO's and waiting to complete
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

        # Close connection and check file exist for memory log
        ret = file_exists(self.mnode,
                          '/var/log/glusterfs/mem_usage.log')
        self.assertTrue(ret, "Unexpected:Memory log file does "
                             "not exist")
        g.log.info("Memory log file exists")
        for proc in cmd_list_procs:
            ret, _, _ = proc.async_communicate()
            self.assertEqual(ret, 0, "Memory logging failed")
            g.log.info("Memory logging is successful")
