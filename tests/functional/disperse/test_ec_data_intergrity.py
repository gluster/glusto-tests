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
    Tests Data Consistency and Intergrity
"""
from random import sample
from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs, collect_mounts_arequal
from glustolibs.gluster.brick_libs import (are_bricks_offline,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           wait_for_bricks_to_be_online,
                                           get_offline_bricks_list)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.volume_libs import get_subvols


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestEcDataIntegrity(GlusterBaseClass):

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

    def _bring_redundant_bricks_offline(self, mnode, volname):
        """
        Bring redundant bricks offline
        """
        brickset_to_offline = []
        # List two bricks in each subvol
        all_subvols_dict = get_subvols(mnode, volname)
        subvols = all_subvols_dict['volume_subvols']
        for subvol in subvols:
            self.assertTrue(subvol, "List is empty")
            brickset_to_offline.extend(sample(subvol, 2))

        # Bring two bricks of each subvol down
        ret = bring_bricks_offline(volname, brickset_to_offline)
        self.assertTrue(ret, "Bricks are still online")

        # Validating the bricks are offline
        ret = are_bricks_offline(mnode, volname,
                                 brickset_to_offline)
        self.assertTrue(ret, "Few of the bricks are still online in"
                             " {} ".format(brickset_to_offline))
        return brickset_to_offline

    def test_ec_data_integrity(self):
        """
        Test steps:
        - Create directory dir1
        - Create 5 dir and 5 files in each dir in directory 1
        - Rename all file inside dir1
        - Truncate at any dir in mountpoint inside dir1
        - Create softlink and hardlink of files in mountpoint
        - chmod, chown, chgrp inside dir1
        - Create tiny, small, medium nd large file
        - Creating files on client side for dir1
        - Validating IO's and waiting to complete
        - Get arequal of dir1
        - Bring redundant bricks offline
        - Get arequal of dir1 after 1st set of bricks down
        - Bring redundant bricks offline
        - Get arequal of dir1 after 2nd set of bricks down
        """

        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        brickset_to_offline = []

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

        # Create softlink and hardlink of files in mountpoint
        start = 1
        for mount_obj in self.mounts:
            for link_type, ln_mode in (('softlink', 'ln -s'),
                                       ('hardlink', 'ln')):
                cmd = ('cd %s/dir1/dir%s; '
                       'for FILENAME in *; '
                       'do %s $FILENAME %s_$FILENAME; cd ~;'
                       'done;'
                       % (mount_obj.mountpoint, str(start), ln_mode,
                          link_type))
                ret, _, _ = g.run(mount_obj.client_system, cmd)
                self.assertFalse(ret, "Creating %s have failed" % link_type)
                g.log.info("%s of files created successfully", link_type)
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
            for size, filename in (('100', 'tiny_file'), ('20M', 'small_file'),
                                   ('200M', 'medium_file'),
                                   ('1G', 'large_file')):
                cmd = 'fallocate -l {} {}{}.txt'.format(size, filename, offset)
                ret, _, _ = g.run(mount_obj.client_system, cmd)
                self.assertFalse(ret, "Fallocate for files failed")
                g.log.info("Fallocate for files successfully")
            offset += 1

        # Creating files on client side for dir1
        # Write IO
        all_mounts_procs, count = [], 1
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d --dir-depth 2 "
                   "--dir-length 10 --max-num-of-dirs 5 "
                   "--num-of-files 5 %s/dir1" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count += 10

        # Validating IO's and waiting to complete
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all IO's")

        # Get arequal of dir1
        ret, result_before_bricks_down = (
            collect_mounts_arequal(self.mounts[0], path='dir1/'))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal of dir1 '
                   'is successful')

        # Bring redundant bricks offline
        brickset_to_offline = self._bring_redundant_bricks_offline(
            self.mnode, self.volname)

        # Get arequal of dir1 after 1st set of bricks down
        ret, result_after_1st_brickset_down = (
            collect_mounts_arequal(self.mounts[0], path='dir1/'))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal of dir1 '
                   'is successful')

        # Bring bricks online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  brickset_to_offline)
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

        # Bring redundant bricks offline
        brickset_to_offline = self._bring_redundant_bricks_offline(
            self.mnode, self.volname)

        # Get arequal of dir1 after 2nd set of bricks down
        ret, result_after_2nd_brickset_down = (
            collect_mounts_arequal(self.mounts[0], path='dir1/'))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal of dir1 '
                   'is successful')

        # Comparing arequals
        self.assertEqual(result_before_bricks_down,
                         result_after_1st_brickset_down,
                         'Arequals are not equals before brickset '
                         'down and after 1st brickset down')
        g.log.info('Arequals are equals before brickset down '
                   'and after brickset down')

        self.assertEqual(result_after_2nd_brickset_down,
                         result_after_1st_brickset_down,
                         'Arequals are not equals before 2nd set '
                         'brick down and after 1st set brick down')
        g.log.info('Arequals are equals for 2nd brickset down '
                   'and 1st brickset down')
