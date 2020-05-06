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

# pylint: disable=too-many-locals
# pylint: disable=too-many-branches,too-many-statements,too-many-function-args

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.glusterfile import (get_fattr, set_fattr,
                                            create_link_file,
                                            delete_fattr)
from glustolibs.gluster.glusterdir import get_dir_contents
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.lib_utils import (append_string_to_file)
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
import glustolibs.gluster.constants as k


@runs_on([['distributed', 'distributed-replicated',
           'distributed-dispersed', 'distributed-arbiter'],
          ['glusterfs']])
class TestDhtCustomXattrClass(GlusterBaseClass):

    def check_custom_xattr_visible(self, xattr_val):
        """
        Check custom xttar from mount point and on bricks.
        """
        # Check custom xattr from mount point
        for mount_object in self.mounts:
            for fname in self.files_and_soft_links:
                attr_val = get_fattr(mount_object.client_system,
                                     fname, 'user.foo')
                self.assertEqual(attr_val, xattr_val,
                                 "Custom xattr not found from mount.")
        g.log.info("Custom xattr found on mount point.")

        # Check custom xattr on bricks
        for brick in get_all_bricks(self.mnode, self.volname):
            node, brick_path = brick.split(':')
            files_on_bricks = get_dir_contents(node, brick_path)
            files = [
                fname.split('/')[3] for fname in self.list_of_files
                if fname.split('/')[3] in files_on_bricks]
            for fname in files:
                attr_val = get_fattr(node,
                                     "{}/{}".format(brick_path, fname),
                                     'user.foo')
                self.assertEqual(attr_val, xattr_val,
                                 "Custom xattr not visible on bricks")
        g.log.info("Custom xattr found on bricks.")

    def delete_xattr_user_foo(self, list_of_files):
        """
        Removes xattr user.foo from all the files.
        """
        for fname in list_of_files:
            ret = delete_fattr(self.client_node, fname, 'user.foo')
            self.assertTrue(ret, "Unable to remove custom xattr for "
                            "file {}".format(fname))
        g.log.info("Successfully removed custom xattr for each file.")

    def set_xattr_user_foo(self, list_of_files, xattr_val):
        """
        sets xattr user.foo on all the files.
        """
        for fname in list_of_files:
            ret = set_fattr(self.client_node, fname, 'user.foo',
                            xattr_val)
            self.assertTrue(ret, "Unable to create custom xattr "
                            "for file {}".format(fname))
        g.log.info("Successfully created a custom xattr for all files.")

    def check_for_trusted_glusterfs_pathinfo(self, list_of_files):
        """
        Check if trusted.glusterfs.pathinfo is visible.
        """
        for fname in list_of_files:
            ret = get_fattr(self.client_node, fname,
                            'trusted.glusterfs.pathinfo')
            self.assertIsNotNone(ret, "pathinfo not visible")
        g.log.info("Mount point shows pathinfo xattr for "
                   "all files")

    def check_mount_point_and_bricks_for_xattr(self, list_of_all_files):
        """
        Check xattr on mount point and bricks.
        """
        # Check if xattr is visable from mount point
        for mount_object in self.mounts:
            for fname in list_of_all_files:
                ret = get_fattr(mount_object.client_system,
                                fname, 'user.foo')
                self.assertIsNone(ret,
                                  "Custom attribute visible at mount "
                                  "point even after deletion")

        # Check if xattr is visable from bricks
        for brick in get_all_bricks(self.mnode, self.volname):
            node, brick_path = brick.split(':')
            files_on_bricks = get_dir_contents(node, brick_path)
            files = [
                fname.split('/')[3] for fname in self.list_of_files
                if fname.split('/')[3] in files_on_bricks]
            for fname in files:
                ret = get_fattr(node, "{}/{}".format(brick_path, fname),
                                'user.foo')
                self.assertIsNone(ret,
                                  "Custom attribute visible on "
                                  "brick even after deletion")

        g.log.info("Custom xattr for file is not visible on "
                   "mount point and bricks")

    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("volume clean up failed")
        g.log.info("Successful in cleaning up Volume %s", self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_dht_custom_xattr(self):
        """
        Test case:
        1.Create a gluster volume and start it.
        2.Create file and link files.
        3.Create a custom xattr for file.
        4.Verify that xattr for file is displayed on
          mount point and bricks
        5.Modify custom xattr value and verify that xattr
          for file is displayed on mount point and bricks
        6.Verify that custom xattr is not displayed
          once you remove it
        7.Create a custom xattr for symbolic link.
        8.Verify that xattr for symbolic link
          is displayed on mount point and sub-volume
        9.Modify custom xattr value and verify that
          xattr for symbolic link is displayed on
          mount point and bricks
        10.Verify that custom xattr is not
           displayed once you remove it.
        """
        # Initializing variables
        mount_point = self.mounts[0].mountpoint
        self.client_node = self.mounts[0].client_system
        self.list_of_files, list_of_softlinks = [], []
        list_of_hardlinks = []

        for number in range(1, 3):

            # Create regular files
            fname = '{0}/regular_file_{1}'.format(mount_point,
                                                  str(number))
            ret = append_string_to_file(self.client_node, fname,
                                        'Sample content for file.')
            self.assertTrue(ret, "Unable to create regular file "
                            "{}".format(fname))
            self.list_of_files.append(fname)

            # Create hard link for file
            hardlink = '{0}/link_file_{1}'.format(mount_point,
                                                  str(number))
            ret = create_link_file(self.client_node, fname, hardlink)
            self.assertTrue(ret, "Unable to create hard link file "
                            "{}".format(hardlink))
            list_of_hardlinks.append(hardlink)

            # Create soft link for file
            softlink = '{0}/symlink_file_{1}'.format(mount_point,
                                                     str(number))
            ret = create_link_file(self.client_node, fname, softlink,
                                   soft=True)
            self.assertTrue(ret, "Unable to create symlink file "
                            "{}".format(softlink))
            list_of_softlinks.append(softlink)

        self.files_and_soft_links = self.list_of_files + list_of_softlinks

        # Check if files are created on the right subvol
        ret = validate_files_in_dir(
            self.client_node, mount_point, file_type=k.FILETYPE_FILES,
            test_type=k.TEST_FILE_EXISTS_ON_HASHED_BRICKS)
        self.assertTrue(ret, "Files not created on correct sub-vols")
        g.log.info("Files are on correct sub-vols according to "
                   "the hash value")

        # Set custom xattr on all the regular files
        self.set_xattr_user_foo(self.list_of_files, 'bar2')

        # Check if custom xattr is set to all the regular files
        self.check_custom_xattr_visible('bar2')

        # Change the custom xattr on all the regular files
        self.set_xattr_user_foo(self.list_of_files, 'ABC')

        # Check if xattr is set to all the regular files
        self.check_custom_xattr_visible('ABC')

        # Delete Custom xattr from all regular files
        self.delete_xattr_user_foo(self.list_of_files)

        # Check mount point and brick for the xattr
        list_of_all_files = list_of_hardlinks + self.files_and_soft_links
        self.check_mount_point_and_bricks_for_xattr(list_of_all_files)

        # Check if pathinfo xattr is visible
        self.check_for_trusted_glusterfs_pathinfo(self.list_of_files)

        # Set custom xattr on all the regular files
        self.set_xattr_user_foo(list_of_softlinks, 'bar2')

        # Check if custom xattr is set to all the regular files
        self.check_custom_xattr_visible('bar2')

        # Change the custom xattr on all the regular files
        self.set_xattr_user_foo(list_of_softlinks, 'ABC')

        # Check if xattr is set to all the regular files
        self.check_custom_xattr_visible('ABC')

        # Delete Custom xattr from all regular files
        self.delete_xattr_user_foo(list_of_softlinks)

        # Check mount point and brick for the xattr
        self.check_mount_point_and_bricks_for_xattr(list_of_all_files)

        # Check if pathinfo xattr is visible
        self.check_for_trusted_glusterfs_pathinfo(list_of_softlinks)
