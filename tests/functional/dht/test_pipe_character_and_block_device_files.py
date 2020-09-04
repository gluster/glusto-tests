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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from socket import gethostbyname

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterdir import get_dir_contents
from glustolibs.gluster.glusterfile import (
    get_file_stat, get_fattr, set_fattr, delete_fattr, get_pathinfo,
    file_exists)


@runs_on([['distributed-replicated', 'distributed-arbiter', 'distributed'],
          ['glusterfs']])
class TestPipeCharacterAndBlockDeviceFiles(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Changing dist_count to 5
        self.volume['voltype']['dist_count'] = 5

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume % s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def _create_character_and_block_device_files(self):
        """Create character and block device files"""
        self.list_of_device_files, self.file_names = [], []
        for ftype, filename in (('b', 'blockfile'), ('c', 'Characterfile')):

            # Create files using mknod
            cmd = ("cd {}; mknod {} {} 1 5".format(
                self.mounts[0].mountpoint, filename, ftype))
            ret, _, _ = g.run(self.clients[0], cmd)
            self.assertEqual(
                ret, 0, 'Failed to create %s file' % filename)

            # Add file names and file path to lists
            self.file_names.append(filename)
            self.list_of_device_files.append('{}/{}'.format(
                self.mounts[0].mountpoint, filename))

        # Create file type list for the I/O
        self.filetype_list = ["block special file", "character special file"]

    def _create_pipe_file(self):
        """Create pipe files"""

        # Create pipe files using mkfifo
        cmd = "cd {}; mkfifo {}".format(self.mounts[0].mountpoint, 'fifo')
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, 'Failed to create %s file' % 'fifo')

        # Populate variables with fifo file details
        self.list_of_device_files = [
            '{}/{}'.format(self.mounts[0].mountpoint, 'fifo')]
        self.file_names = ['fifo']
        self.filetype_list = ['fifo']

    def _set_xattr_trusted_foo(self, xattr_val):
        """Sets xattr trusted.foo on all the files"""
        for fname in self.list_of_device_files:
            ret = set_fattr(self.clients[0], fname, 'trusted.foo',
                            xattr_val)
            self.assertTrue(ret, "Unable to create custom xattr "
                            "for file {}".format(fname))

    def _delete_xattr_trusted_foo(self):
        """Removes xattr trusted.foo from all the files."""
        for fname in self.list_of_device_files:
            ret = delete_fattr(self.clients[0], fname, 'trusted.foo')
            self.assertTrue(ret, "Unable to remove custom xattr for "
                            "file {}".format(fname))

    def _check_custom_xattr_trusted_foo(self, xattr_val, visible=True):
        """Check custom xttar from mount point and on bricks."""
        # Check custom xattr from mount point
        for fname in self.list_of_device_files:
            ret = get_fattr(self.clients[0], fname, 'trusted.foo',
                            encode='text')
            if visible:
                self.assertEqual(ret, xattr_val,
                                 "Custom xattr not found from mount.")
            else:
                self.assertIsNone(ret, "Custom attribute visible at mount "
                                  "point even after deletion")

        # Check custom xattr on bricks
        for brick in get_all_bricks(self.mnode, self.volname):
            node, brick_path = brick.split(':')
            files_on_bricks = get_dir_contents(node, brick_path)
            files = [
                fname for fname in self.file_names
                if fname in files_on_bricks]
            for fname in files:
                ret = get_fattr(node, "{}/{}".format(brick_path, fname),
                                'trusted.foo', encode='text')
                if visible:
                    self.assertEqual(ret, xattr_val,
                                     "Custom xattr not visible on bricks")
                else:
                    self.assertIsNone(ret, "Custom attribute visible on "
                                      "brick even after deletion")

    def _check_if_files_are_stored_only_on_expected_bricks(self):
        """Check if files are stored only on expected bricks"""
        for fname in self.list_of_device_files:
            # Fetch trusted.glusterfs.pathinfo and check if file is present on
            # brick or not
            ret = get_pathinfo(self.clients[0], fname)
            self.assertIsNotNone(ret, "Unable to get "
                                 "trusted.glusterfs.pathinfo  of file %s"
                                 % fname)
            present_brick_list = []
            for brick_path in ret['brickdir_paths']:
                node, path = brick_path.split(":")
                ret = file_exists(node, path)
                self.assertTrue(ret, "Unable to find file {} on brick {}"
                                .format(fname, path))
                brick_text = brick_path.split('/')[:-1]
                if brick_text[0][0:2].isdigit():
                    brick_text[0] = gethostbyname(brick_text[0][:-1]) + ":"
                present_brick_list.append('/'.join(brick_text))

            # Check on other bricks where file doesn't exist
            brick_list = get_all_bricks(self.mnode, self.volname)
            other_bricks = [
                brk for brk in brick_list if brk not in present_brick_list]
            for brick in other_bricks:
                node, path = brick.split(':')
                ret = file_exists(node, "{}/{}".format(path,
                                                       fname.split('/')[-1]))
                self.assertFalse(ret, "Unexpected: Able to find file {} on "
                                 "brick {}".format(fname, path))

    def _check_filetype_of_files_from_mountpoint(self):
        """Check filetype of files from mountpoint"""
        for filetype in self.filetype_list:
            # Check if filetype is as expected
            ret = get_file_stat(self.clients[0], self.list_of_device_files[
                self.filetype_list.index(filetype)])
            self.assertEqual(ret['filetype'], filetype,
                             "File type not reflecting properly for %s"
                             % filetype)

    def _compare_stat_output_from_mout_point_and_bricks(self):
        """Compare stat output from mountpoint and bricks"""
        for fname in self.list_of_device_files:
            # Fetch stat output from mount point
            mountpoint_stat = get_file_stat(self.clients[0], fname)
            bricks = get_pathinfo(self.clients[0], fname)

            # Fetch stat output from bricks
            for brick_path in bricks['brickdir_paths']:
                node, path = brick_path.split(":")
                brick_stat = get_file_stat(node, path)
                for key in ("filetype", "access", "size", "username",
                            "groupname", "uid", "gid", "epoch_atime",
                            "epoch_mtime", "epoch_ctime"):
                    self.assertEqual(mountpoint_stat[key], brick_stat[key],
                                     "Difference observed between stat output "
                                     "of mountpoint and bricks for file %s"
                                     % fname)

    def test_character_and_block_device_file_creation(self):
        """
        Test case:
        1. Create distributed volume with 5 sub-volumes, start amd mount it.
        2. Create character and block device files.
        3. Check filetype of files from mount point.
        4. Verify that the files are stored on only the bricks which is
           mentioned in trusted.glusterfs.pathinfo xattr.
        5. Verify stat output from mount point and bricks.
        """
        # Create Character and block device files
        self._create_character_and_block_device_files()

        # Check filetype of files from mount point
        self._check_filetype_of_files_from_mountpoint()

        # Verify that the files are stored on only the bricks which is
        # mentioned in trusted.glusterfs.pathinfo xattr
        self._check_if_files_are_stored_only_on_expected_bricks()

        # Verify stat output from mount point and bricks
        self._compare_stat_output_from_mout_point_and_bricks()

    def test_character_and_block_device_file_removal_using_rm(self):
        """
        Test case:
        1. Create distributed volume with 5 sub-volumes, start and mount it.
        2. Create character and block device files.
        3. Check filetype of files from mount point.
        4. Verify that the files are stored on only one bricks which is
           mentioned in trusted.glusterfs.pathinfo xattr.
        5. Delete the files.
        6. Verify if the files are delete from all the bricks
        """
        # Create Character and block device files
        self._create_character_and_block_device_files()

        # Check filetype of files from mount point
        self._check_filetype_of_files_from_mountpoint()

        # Verify that the files are stored on only the bricks which is
        # mentioned in trusted.glusterfs.pathinfo xattr
        self._check_if_files_are_stored_only_on_expected_bricks()

        # Delete both the character and block device files
        for fname in self.list_of_device_files:
            ret, _, _ = g.run(self.clients[0], 'rm -rf {}'.format(fname))
            self.assertEqual(
                ret, 0, 'Failed to remove {} file'.format(fname))

        # Verify if the files are deleted from all bricks or not
        for brick in get_all_bricks(self.mnode, self.volname):
            node, path = brick.split(':')
            for fname in self.file_names:
                ret = file_exists(node, "{}/{}".format(path, fname))
                self.assertFalse(ret, "Unexpected: Able to find file {} on "
                                 " brick {} even after deleting".format(fname,
                                                                        path))

    def test_character_and_block_device_file_with_custom_xattrs(self):
        """
        Test case:
        1. Create distributed volume with 5 sub-volumes, start and mount it.
        2. Create character and block device files.
        3. Check filetype of files from mount point.
        4. Set a custom xattr for files.
        5. Verify that xattr for files is displayed on mount point and bricks.
        6. Modify custom xattr value and verify that xattr for files
           is displayed on mount point and bricks.
        7. Remove the xattr and verify that custom xattr is not displayed.
        8. Verify that mount point and brick shows pathinfo xattr properly.
        """
        # Create Character and block device files
        self._create_character_and_block_device_files()

        # Check filetype of files from mount point
        self._check_filetype_of_files_from_mountpoint()

        # Set a custom xattr for files
        self._set_xattr_trusted_foo("bar1")

        # Verify that xattr for files is displayed on mount point and bricks
        self._check_custom_xattr_trusted_foo("bar1")

        # Modify custom xattr value
        self._set_xattr_trusted_foo("bar2")

        # Verify that xattr for files is displayed on mount point and bricks
        self._check_custom_xattr_trusted_foo("bar2")

        # Remove the xattr
        self._delete_xattr_trusted_foo()

        # Verify that custom xattr is not displayed
        self._check_custom_xattr_trusted_foo("bar2", visible=False)

        # Verify that mount point shows pathinfo xattr properly
        self._check_if_files_are_stored_only_on_expected_bricks()

    def test_pipe_file_create(self):
        """
        Test case:
        1. Create distributed volume with 5 sub-volumes, start and mount it.
        2. Create a pipe file.
        3. Check filetype of files from mount point.
        4. Verify that the files are stored on only the bricks which is
           mentioned in trusted.glusterfs.pathinfo xattr.
        5. Verify stat output from mount point and bricks.
        6. Write data to fifo file and read data from fifo file
           from the other instance of the same client.
        """
        # Create a pipe file
        self._create_pipe_file()

        # Check filetype of files from mount point
        self._check_filetype_of_files_from_mountpoint()

        # Verify that the files are stored on only the bricks which is
        # mentioned in trusted.glusterfs.pathinfo xattr
        self._check_if_files_are_stored_only_on_expected_bricks()

        # Verify stat output from mount point and bricks
        self._compare_stat_output_from_mout_point_and_bricks()

        # Write data to fifo file and read data from fifo file
        # from the other instance of the same client.
        g.run_async(self.clients[0], "echo 'Hello' > {} ".format(
            self.list_of_device_files[0]))
        ret, out, _ = g.run(
            self.clients[0], "cat < {}".format(self.list_of_device_files[0]))
        self.assertEqual(
            ret, 0, "Unable to fetch datat on other terimnal")
        self.assertEqual(
            "Hello", out.split('\n')[0],
            "Hello not recieved on the second terimnal")
