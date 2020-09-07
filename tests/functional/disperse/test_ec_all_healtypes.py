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
    Tests FOps and Data Deletion on a healthy EC volume
"""
from random import sample
from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, collect_mounts_arequal
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_online,
                                           wait_for_bricks_to_be_online,
                                           get_offline_bricks_list,
                                           bring_bricks_offline)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.heal_libs import monitor_heal_completion


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestEcAllHealTypes(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_ec_all_healtypes(self):
        """
        Test steps:
        - Create directory dir1
        - Create files inside dir1
        - Rename all file inside dir1
        - Create softlink and hardlink of files in mountpoint
        - Create tiny, small, medium nd large file
        - Get arequal of dir1
        - Create directory dir2
        - Creating files on dir2
        - Bring down other bricks to max redundancy
        - Create directory dir3
        - Start pumping IO to dir3
        - Validating IO's on dir2 and waiting to complete
        - Bring bricks online
        - Wait for bricks to come online
        - Check if bricks are online
        - Monitor heal completion
        - Get arequal of dir1
        - Compare arequal of dir1
        """

        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        # Get the bricks from the volume
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        mountpoint = self.mounts[0].mountpoint
        client = self.mounts[0].client_system

        # Creating dir1
        ret = mkdir(client, "%s/dir1"
                    % mountpoint)
        self.assertTrue(ret, "Failed to create dir1")
        g.log.info("Directory dir1 on %s created successfully", self.mounts[0])

        # Create files inside dir1
        cmd = ('touch %s/dir1/file{1..5};'
               % mountpoint)
        ret, _, _ = g.run(client, cmd)
        self.assertFalse(ret, "File creation failed")
        g.log.info("File created successfull")

        # Rename all files inside dir1
        cmd = ('cd %s/dir1/; '
               'for FILENAME in *;'
               'do mv $FILENAME Unix_$FILENAME; cd ~;'
               'done;'
               % mountpoint)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Failed to rename files on "
                         "client")
        g.log.info("Successfully renamed files on client")

        # Create softlink and hardlink of files in mountpoint
        cmd = ('cd %s/dir1/; '
               'for FILENAME in *; '
               'do ln -s $FILENAME softlink_$FILENAME; cd ~;'
               'done;'
               % mountpoint)
        ret, _, _ = g.run(client, cmd)
        self.assertFalse(ret, "Creating Softlinks have failed")
        g.log.info("Softlink of files have been changed successfully")

        cmd = ('cd %s/dir1/; '
               'for FILENAME in *; '
               'do ln $FILENAME hardlink_$FILENAME; cd ~;'
               'done;'
               % mountpoint)
        ret, _, _ = g.run(client, cmd)
        self.assertFalse(ret, "Creating Hardlinks have failed")
        g.log.info("Hardlink of files have been changed successfully")

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

        # Get arequal of dir1
        ret, result_before_brick_down = (
            collect_mounts_arequal(self.mounts[0], path='dir1/'))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal of dir1 '
                   'is successful')

        # Creating dir2
        ret = mkdir(self.mounts[0].client_system, "%s/dir2"
                    % mountpoint)
        self.assertTrue(ret, "Failed to create dir2")
        g.log.info("Directory dir2 on %s created successfully", self.mounts[0])

        # Creating files on dir2
        # Write IO
        all_mounts_procs, count = [], 1
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d --dir-depth 2 "
                   "--dir-length 10 --max-num-of-dirs 5 "
                   "--num-of-files 5 %s/dir2" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Bring down other bricks to max redundancy
        # Bringing bricks offline
        bricks_to_offline = sample(bricks_list, 2)
        ret = bring_bricks_offline(self.volname,
                                   bricks_to_offline)
        self.assertTrue(ret, 'Bricks not offline')
        g.log.info('Bricks are offline successfully')

        # Creating dir3
        ret = mkdir(self.mounts[0].client_system, "%s/dir3"
                    % mountpoint)
        self.assertTrue(ret, "Failed to create dir2")
        g.log.info("Directory dir2 on %s created successfully", self.mounts[0])

        # Start pumping IO to dir3
        cmd = ("cd %s/dir3; for i in `seq 1 100` ;"
               "do dd if=/dev/urandom of=file$i bs=1M "
               "count=5;done" % mountpoint)

        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Finished writing on files while a brick is DOWN')

        appendcmd = ("cd %s/dir3; for i in `seq 1 100` ;"
                     "do dd if=/dev/urandom of=file$i bs=1M "
                     "count=1 oflag=append conv=notrunc;done" % mountpoint)

        readcmd = ("cd %s/dir3; for i in `seq 1 100` ;"
                   "do dd if=file$i of=/dev/null bs=1M "
                   "count=5;done" % mountpoint)

        ret, _, err = g.run(self.mounts[0].client_system, appendcmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Finished append on files after redundant bricks offline')

        ret, _, err = g.run(self.mounts[0].client_system, readcmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Finished read on files after redundant bricks offline')

        # Validating IO's on dir2 and waiting to complete
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all IO's")

        # Bring bricks online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_offline)
        self.assertTrue(ret, 'Bricks not brought online')
        g.log.info('Bricks are online successfully')

        # Wait for brick to come online
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Bricks are not online")
        g.log.info("EXPECTED : Bricks are online")

        # Check if bricks are online
        ret = get_offline_bricks_list(self.mnode, self.volname)
        self.assertListEqual(ret, [], 'All bricks are not online')
        g.log.info('All bricks are online')

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')
        g.log.info('Heal has completed successfully')

        # Get arequal of dir1
        ret, result_after_brick_up = (
            collect_mounts_arequal(self.mounts[0], path='dir1/'))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal of dir1 '
                   'is successful')

        # Comparing arequals of dir1
        self.assertEqual(result_before_brick_down,
                         result_after_brick_up,
                         'Arequals are not equals before and after '
                         'bringing down redundant bricks')
        g.log.info('Arequals are equals before before and after '
                   'bringing down redundant bricks')
