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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfile import (get_file_stat, get_pathinfo,
                                            file_exists, create_link_file,
                                            get_md5sum, get_fattr)
from glustolibs.gluster.lib_utils import append_string_to_file


@runs_on([['distributed', 'distributed-arbiter',
           'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs']])
class TestFileCreation(GlusterBaseClass):
    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")
        self.client, self.m_point = (self.mounts[0].client_system,
                                     self.mounts[0].mountpoint)

    def tearDown(self):

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _create_file_using_touch(self, file_name):
        """Creates a regular empty file"""
        cmd = "touch {}/{}".format(self.m_point, file_name)
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, "Failed to create file {}".format(file_name))
        g.log.info("Successfully created file %s", file_name)

    def _check_file_stat_on_mountpoint(self, file_name, file_type):
        """Check the file-type on mountpoint"""
        file_stat = (get_file_stat(self.client, "{}/{}".format(
            self.m_point, file_name
        )))['filetype']
        self.assertEqual(file_stat, file_type,
                         "File is not a {}".format(file_type))
        g.log.info("File is %s", file_type)

    def _is_file_present_on_brick(self, file_name):
        """Check if file is created on the backend-bricks as per
        the value of trusted.glusterfs.pathinfo xattr"""
        brick_list = get_pathinfo(self.client, "{}/{}".format(
            self.m_point, file_name))
        self.assertNotEqual(
            brick_list, 0, "Failed to get bricklist for {}".format(file_name))

        for brick in brick_list['brickdir_paths']:
            host, path = brick.split(':')
            ret = file_exists(host, path)
            self.assertTrue(ret, "File {} is not present on {}".format(
                file_name, brick
            ))
            g.log.info("File %s is present on %s", file_name, brick)

    def _compare_file_permissions(self, file_name,
                                  file_info_mnt=None, file_info_brick=None):
        """Check if the file's permission are same on mountpoint and
        backend-bricks"""
        if (file_info_mnt is None and file_info_brick is None):
            file_info_mnt = (get_file_stat(self.client, "{}/{}".format(
                self.m_point, file_name
                )))['access']
            self.assertIsNotNone(
                file_info_mnt, "Failed to get access time for {}".format(
                    file_name))
            brick_list = get_pathinfo(self.client, "{}/{}".format(
                self.m_point, file_name))
            self.assertNotEqual(
                brick_list, 0, "Failed to get bricklist for {}".format(
                    file_name))
            file_info_brick = []
            for brick in brick_list['brickdir_paths']:
                host, path = brick.split(':')
                info_brick = (get_file_stat(host, path))['access']
                file_info_brick.append(info_brick)

        for info in file_info_brick:
            self.assertEqual(info, file_info_mnt,
                             "File details for {} are diffrent on"
                             " backend-brick".format(file_name))
            g.log.info("Details for file %s is correct"
                       " on backend-bricks", file_name)

    def _check_change_time_mnt(self, file_name):
        """Find out the modification time for file on mountpoint"""
        file_ctime_mnt = (get_file_stat(self.client, "{}/{}".format(
            self.m_point, file_name
        )))['epoch_ctime']
        return file_ctime_mnt

    def _check_change_time_brick(self, file_name):
        """Find out the modification time for file on backend-bricks"""
        brick_list = get_pathinfo(self.client, "{}/{}".format(
            self.m_point, file_name))
        self.assertNotEqual(brick_list, 0,
                            "Failed to get bricklist for {}".format(file_name))

        brick_mtime = []
        for brick in brick_list['brickdir_paths']:
            host, path = brick.split(':')
            cmd = "ls -lR {}".format(path)
            ret, _, _ = g.run(host, cmd)
            self.assertEqual(ret, 0, "Lookup failed on"
                             " brick:{}".format(path))
            file_ctime_brick = (get_file_stat(host, path))['epoch_ctime']
            brick_mtime.append(file_ctime_brick)
        return brick_mtime

    def _compare_file_perm_mnt(self, mtime_before, mtime_after,
                               file_name):
        """Compare the file permissions before and after appending data"""
        self.assertNotEqual(mtime_before, mtime_after, "Unexpected:"
                            "The ctime has not been changed")
        g.log.info("The modification time for %s has been"
                   " changed as expected", file_name)

    def _collect_and_compare_file_info_on_mnt(
            self, link_file_name, values, expected=True):
        """Collect the files's permissions on mountpoint and compare"""
        stat_test_file = get_file_stat(
            self.client, "{}/test_file".format(self.m_point))
        self.assertIsNotNone(stat_test_file, "Failed to get stat of test_file")
        stat_link_file = get_file_stat(
            self.client, "{}/{}".format(self.m_point, link_file_name))
        self.assertIsNotNone(stat_link_file, "Failed to get stat of {}".format(
            link_file_name))

        for key in values:
            if expected is True:
                self.assertEqual(stat_test_file[key], stat_link_file[key],
                                 "The {} is not same for test_file"
                                 " and {}".format(key, link_file_name))
                g.log.info("The %s for test_file and %s is same on mountpoint",
                           key, link_file_name)
            else:
                self.assertNotEqual(stat_test_file[key], stat_link_file[key],
                                    "Unexpected : The {} is same for test_file"
                                    " and {}".format(key, link_file_name))
                g.log.info("The %s for test_file and %s is different"
                           " on mountpoint", key, link_file_name)

    def _compare_file_md5sum_on_mnt(self, link_file_name):
        """Collect and compare the md5sum for file on mountpoint"""
        md5sum_test_file, _ = (get_md5sum(
            self.client, "{}/test_file".format(self.m_point))).split()
        self.assertIsNotNone(
            md5sum_test_file, "Failed to get md5sum for test_file")

        md5sum_link_file, _ = get_md5sum(
            self.client, "{}/{}".format(self.m_point, link_file_name)).split()
        self.assertIsNotNone(md5sum_link_file, "Failed to get"
                             " md5sum for {}".format(link_file_name))
        self.assertEqual(md5sum_test_file, md5sum_link_file,
                         "The md5sum for test_file and {} is"
                         " not same".format(link_file_name))
        g.log.info("The md5sum is same for test_file and %s"
                   " on mountpoint", link_file_name)

    def _compare_file_md5sum_on_bricks(self, link_file_name):
        """Collect and compare md5sum for file on backend-bricks"""
        brick_list_test_file = get_pathinfo(self.client, "{}/test_file".format(
            self.m_point))
        md5sum_list_test_file = []
        for brick in brick_list_test_file['brickdir_paths']:
            host, path = brick.split(':')
            md5sum_test_file, _ = (get_md5sum(host, path)).split()
            md5sum_list_test_file.append(md5sum_test_file)

        brick_list_link_file = get_pathinfo(self.client, "{}/{}".format(
            self.m_point, link_file_name))
        md5sum_list_link_file = []
        for brick in brick_list_link_file['brickdir_paths']:
            md5sum_link_file, _ = (get_md5sum(host, path)).split()
            md5sum_list_link_file.append(md5sum_link_file)

        self.assertEqual(md5sum_test_file, md5sum_link_file,
                         "The md5sum for test_file and {} is"
                         " not same on brick {}".format(link_file_name, brick))
        g.log.info("The md5sum for test_file and %s is same"
                   " on backend brick %s", link_file_name, brick)

    def _compare_gfid_xattr_on_files(self, link_file_name, expected=True):
        """Collect and compare the value of trusted.gfid xattr for file
        on backend-bricks"""
        brick_list_test_file = get_pathinfo(self.client, "{}/test_file".format(
            self.m_point))
        xattr_list_test_file = []
        for brick in brick_list_test_file['brickdir_paths']:
            host, path = brick.split(':')
            xattr_test_file = get_fattr(host, path, "trusted.gfid")
            xattr_list_test_file.append(xattr_test_file)

        brick_list_link_file = get_pathinfo(self.client, "{}/{}".format(
            self.m_point, link_file_name))
        xattr_list_link_file = []
        for brick in brick_list_link_file['brickdir_paths']:
            host, path = brick.split(':')
            xattr_link_file = get_fattr(host, path, "trusted.gfid")
            xattr_list_link_file.append(xattr_link_file)

        if expected is True:
            self.assertEqual(xattr_list_test_file, xattr_list_link_file,
                             "Unexpected: The xattr trusted.gfid is not same "
                             "for test_file and {}".format(link_file_name))
            g.log.info("The xattr trusted.gfid is same for test_file"
                       " and %s", link_file_name)
        else:
            self.assertNotEqual(xattr_list_test_file, xattr_list_link_file,
                                "Unexpected: The xattr trusted.gfid is same "
                                "for test_file and {}".format(link_file_name))
            g.log.info("The xattr trusted.gfid is not same for test_file"
                       " and %s", link_file_name)

    def test_special_file_creation(self):
        """
        Description : check creation of different types of files.

        Steps:
        1) From mount point, Create a regular file
        eg:
        touch f1
        - From mount point, create character, block device and pipe files
        mknod c
        mknod b
        mkfifo
        2) Stat on the files created in Step-2 from mount point
        3) Verify that file is stored on only one bricks which is mentioned in
           trusted.glusterfs.pathinfo xattr
           On mount point -
           " getfattr -n trusted.glusterfs.pathinfo
           On all bricks
           " ls / "
        4) Verify that file permissions are same on mount point and sub-volumes
           " stat "
        5) Append some data to the file.
        6) List content of file to verify that data has been appended.
           " cat "
        7) Verify that file change time and size has been updated
           accordingly(from mount point and sub-volume)
           " stat / "
        """
        # pylint: disable=too-many-statements
        # pylint: disable=too-many-locals
        # Create a regular file
        self._create_file_using_touch("regfile")

        # Create a character and block file
        for (file_name, parameter) in [
                ("blockfile", "b"), ("charfile", "c")]:
            cmd = "mknod {}/{} {} 1 5".format(self.m_point, file_name,
                                              parameter)
            ret, _, _ = g.run(self.client, cmd)
            self.assertEqual(
                ret, 0, "Failed to create {} file".format(file_name))
            g.log.info("%s file created successfully", file_name)

        # Create a pipe file
        cmd = "mkfifo {}/pipefile".format(self.m_point)
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, "Failed to create pipe file")
        g.log.info("Pipe file is created successfully")

        # Stat all the files created on mount-point
        for (file_name, check_string) in [
                ("regfile", "regular empty file"),
                ("charfile", "character special file"),
                ("blockfile", "block special file"),
                ("pipefile", "fifo")]:
            self._check_file_stat_on_mountpoint(file_name, check_string)

        # Verify files are stored on backend bricks as per
        # the trusted.glusterfs.pathinfo
        file_types = ["regfile", "charfile", "blockfile", "pipefile"]

        for file_name in file_types:
            self._is_file_present_on_brick(file_name)

        # Verify that the file permissions are same on
        # mount-point and bricks
        for file_name in file_types:
            self._compare_file_permissions(file_name)

        # Note the modification time on mount and bricks
        # for all files. Also it should be same on mnt and bricks
        reg_mnt_ctime_1 = self._check_change_time_mnt("regfile")
        char_mnt_ctime_1 = self._check_change_time_mnt("charfile")
        block_mnt_ctime_1 = self._check_change_time_mnt("blockfile")
        fifo_mnt_ctime_1 = self._check_change_time_mnt("pipefile")

        reg_brick_ctime_1 = self._check_change_time_brick("regfile")
        char_brick_ctime_1 = self._check_change_time_brick("charfile")
        block_brick_ctime_1 = self._check_change_time_brick("blockfile")
        fifo_brick_ctime_1 = self._check_change_time_brick("pipefile")

        for (file_name, mnt_ctime, brick_ctime) in [
                ("regfile", reg_mnt_ctime_1, reg_brick_ctime_1),
                ("charfile", char_mnt_ctime_1, char_brick_ctime_1),
                ("blockfile", block_mnt_ctime_1, block_brick_ctime_1),
                ("pipefile", fifo_mnt_ctime_1, fifo_brick_ctime_1)]:
            self._compare_file_permissions(
                file_name, mnt_ctime, brick_ctime)

        # Append some data to the files
        for (file_name, data_str) in [
                ("regfile", "regular"),
                ("charfile", "character special"),
                ("blockfile", "block special")]:
            ret = append_string_to_file(
                self.client, "{}/{}".format(self.m_point, file_name),
                "Welcome! This is a {} file".format(data_str))
            self.assertTrue(
                ret, "Failed to append data to {}".format(file_name))
            g.log.info(
                "Successfully appended data to %s", file_name)

        # Check if the data has been appended
        check = "Welcome! This is a regular file"
        cmd = "cat {}/{}".format(self.m_point, "regfile")
        ret, out, _ = g.run(self.client, cmd)
        self.assertEqual(out.strip(), check, "No data present at regfile")

        # Append data to pipefile and check if it has been appended
        g.run_async(self.client, "echo 'Hello' > {}/{} ".format(
            self.m_point, "pipefile"))
        ret, out, _ = g.run(
            self.client, "cat < {}/{}".format(self.m_point, "pipefile"))
        self.assertEqual(
            ret, 0, "Unable to fetch datat on other terimnal")
        self.assertEqual(
            "Hello", out.split('\n')[0],
            "Hello not recieved on the second terimnal")

        # Lookup on mount-point
        cmd = "ls -lR {}".format(self.m_point)
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, "Lookup on mountpoint failed")

        # Collect ctime on mount point after appending data
        reg_mnt_ctime_2 = self._check_change_time_mnt("regfile")

        # After appending data the ctime for file should change
        self.assertNotEqual(reg_mnt_ctime_1, reg_mnt_ctime_2, "Unexpected:"
                            "The ctime has not been changed")
        g.log.info("The modification time for regfile has been"
                   " changed as expected")

        # Collect the ctime on bricks
        reg_brick_ctime_2 = self._check_change_time_brick("regfile")

        # Check if the ctime has changed on bricks as per mount
        self._compare_file_permissions(
            "regfile", reg_mnt_ctime_2, reg_brick_ctime_2)

    def test_hard_link_file(self):
        """
        Description: link file create, validate and access file
                     using it

        Steps:
        1) From mount point, create a regular file
        2) Verify that file is stored on only on bricks which is
           mentioned in trusted.glusterfs.pathinfo xattr
        3) From mount point create hard-link file for the created file
        4) From mount point stat on the hard-link file and original file;
           file inode, permission, size should be same
        5) From mount point, verify that file contents are same
           "md5sum"
        6) Verify "trusted.gfid" extended attribute of the file
           on sub-vol
        7) From sub-volume stat on the hard-link file and original file;
           file inode, permission, size should be same
        8) From sub-volume verify that content of file are same
        """
        # Create a regular file
        self._create_file_using_touch("test_file")

        # Check file is create on bricks as per trusted.glusterfs.pathinfo
        self._is_file_present_on_brick("test_file")

        # Create a hard-link file for the test_file
        ret = create_link_file(
            self.client, "{}/test_file".format(self.m_point),
            "{}/hardlink_file".format(self.m_point))
        self.assertTrue(ret, "Failed to create hard link file for"
                             " test_file")
        g.log.info("Successfully created hardlink_file")

        # On mountpoint perform stat on original and hard-link file
        values = ["inode", "access", "size"]
        self._collect_and_compare_file_info_on_mnt(
            "hardlink_file", values, expected=True)

        # Check the md5sum on original and hard-link file on mountpoint
        self._compare_file_md5sum_on_mnt("hardlink_file")

        # Compare the value of trusted.gfid for test_file and hard-link file
        # on backend-bricks
        self._compare_gfid_xattr_on_files("hardlink_file")

        # On backend bricks perform stat on original and hard-link file
        values = ["inode", "access", "size"]
        self._collect_and_compare_file_info_on_mnt("hardlink_file", values)

        # On backend bricks check the md5sum
        self._compare_file_md5sum_on_bricks("hardlink_file")

    def test_symlink_file(self):
        """
        Description: Create symbolic link file, validate and access file
                     using it

        Steps:
        1) From mount point, create a regular file
        2) Verify that file is stored on only on bricks which is
           mentioned in trusted.glusterfs.pathinfo xattr
        3) From mount point create symbolic link file for the created file
        4) From mount point stat on the symbolic link file and original file;
           file inode should be different
        5) From mount point, verify that file contents are same
           "md5sum"
        6) Verify "trusted.gfid" extended attribute of the file
           on sub-vol
        7) Verify readlink on symbolic link from mount point
           "readlink "
        8) From sub-volume verify that content of file are same
        """
        # Create a regular file on mountpoint
        self._create_file_using_touch("test_file")

        # Check file is create on bricks as per trusted.glusterfs.pathinfo
        self._is_file_present_on_brick("test_file")

        # Create a symbolic-link file for the test_file
        ret = create_link_file(
            self.client, "{}/test_file".format(self.m_point),
            "{}/softlink_file".format(self.m_point), soft=True)
        self.assertTrue(ret, "Failed to create symbolic link file for"
                             " test_file")
        g.log.info("Successfully created softlink_file")

        # On mountpoint perform stat on original and symbolic-link file
        # The value of inode should be different
        values = ["inode"]
        self._collect_and_compare_file_info_on_mnt(
            "softlink_file", values, expected=False)

        # Check the md5sum on original and symbolic-link file on mountpoint
        self._compare_file_md5sum_on_mnt("softlink_file")

        # Compare the value of trusted.gfid for test_file and
        # symbolic-link file on backend-bricks
        self._compare_gfid_xattr_on_files("softlink_file")

        # Verify readlink on symbolic-link from mount point
        cmd = "readlink {}/softlink_file".format(self.m_point)
        ret, out, _ = g.run(self.client, cmd)
        self.assertEqual(
            out.strip(), "{}/test_file".format(self.m_point),
            "Symbolic link points to incorrect file")
        g.log.info("Symbolic link points to correct file")

        # Check the md5sum on original and symbolic-link file on backend bricks
        self._compare_file_md5sum_on_bricks("softlink_file")
