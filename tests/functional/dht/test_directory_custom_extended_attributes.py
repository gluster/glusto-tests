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
Distributed Hash Table (DHT) Tests
Test cases in this module tests
Custom extended attribute validation

"""
from glusto.core import Glusto as g

from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.constants import FILETYPE_DIR, FILETYPE_LINK
from glustolibs.gluster.constants import \
    TEST_FILE_EXISTS_ON_HASHED_BRICKS as FILE_ON_HASHED_BRICKS
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import (delete_fattr, file_exists,
                                            get_fattr, get_fattr_list,
                                            set_fattr)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestDirectoryCustomExtendedAttributes(GlusterBaseClass):
    """
    TestDirectoryCustomExtendedAttributes contains tests
    which verifies Directory - custom extended attribute
    validation getfattr, setfattr.
    """

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError("Failed to Setup and Mount Volume")

    def test_directory_custom_extended_attr(self):
        """Test - set custom xattr to directory and link to directory
        """
        # pylint: disable = too-many-statements
        dir_prefix = '{root}/folder_{client_index}'

        for mount_index, mount_point in enumerate(self.mounts):
            folder_name = dir_prefix.format(
                root=mount_point.mountpoint,
                client_index=mount_index
            )

            # Create a directory from mount point
            g.log.info('Creating directory : %s:%s',
                       mount_point.mountpoint, folder_name)
            ret = mkdir(mount_point.client_system, folder_name)
            self.assertTrue(ret,
                            'Failed to create directory %s on mount point %s'
                            % (folder_name, mount_point.mountpoint))

            ret = file_exists(mount_point.client_system,
                              folder_name)
            self.assertTrue(ret,
                            'Created Directory %s does not exists on mount '
                            'point %s' %
                            (folder_name, mount_point.mountpoint))
            g.log.info('Created directory %s:%s',
                       mount_point.mountpoint, folder_name)

            # Verify that hash layout values are set on each
            # bricks for the dir
            g.log.debug("Verifying hash layout values")
            ret = validate_files_in_dir(mount_point.client_system,
                                        mount_point.mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS,
                                        file_type=FILETYPE_DIR)
            self.assertTrue(ret, "Expected - Directory is stored "
                                 "on hashed bricks")
            g.log.info("Hash layout values are set on each bricks")

            # Verify that mount point should not display
            # xattr : trusted.gfid and dht
            g.log.debug("Loading extra attributes")
            ret = get_fattr_list(mount_point.client_system, folder_name)

            self.assertTrue('trusted.gfid' not in ret,
                            "Extended attribute trusted.gfid is presented on "
                            "mount point %s and folder %s"
                            % (mount_point.mountpoint, folder_name))
            self.assertTrue('trusted.glusterfs.dht' not in ret,
                            "Extended attribute trusted.glusterfs.dht is "
                            "presented on mount point %s and folder %s"
                            % (mount_point.mountpoint, folder_name))

            g.log.info('Extended attributes trusted.gfid and '
                       'trusted.glusterfs.dht does not exists on '
                       'mount point %s:%s ',
                       mount_point.mountpoint, folder_name)

            # Verify that mount point shows pathinfo xattr
            g.log.debug("Check for xattr trusted.glusterfs.pathinfo on %s:%s",
                        mount_point, folder_name)
            ret = get_fattr(mount_point.client_system,
                            mount_point.mountpoint,
                            'trusted.glusterfs.pathinfo')
            self.assertIsNotNone(ret,
                                 "trusted.glusterfs.pathinfo is not "
                                 "presented on %s:%s" %
                                 (mount_point.mountpoint, folder_name))
            g.log.info('pathinfo xattr is displayed on mount point %s and '
                       'dir %s', mount_point.mountpoint, folder_name)

            # Create a custom xattr for dir
            g.log.info("Set attribute user.foo to %s", folder_name)
            ret = set_fattr(mount_point.client_system,
                            folder_name, 'user.foo', 'bar2')
            self.assertTrue(ret, "Setup custom attribute on %s:%s failed" %
                            (mount_point.client_system, folder_name))

            g.log.info('Set custom attribute is set on %s:%s',
                       mount_point.client_system, folder_name)
            # Verify that custom xattr for directory is displayed
            # on mount point and bricks
            g.log.debug('Check xarttr user.foo on %s:%s',
                        mount_point.client_system, folder_name)
            ret = get_fattr(mount_point.client_system, folder_name,
                            'user.foo')
            self.assertEqual(ret, 'bar2',
                             "Xattr attribute user.foo is not presented on "
                             "mount point %s and directory %s" %
                             (mount_point.client_system, folder_name))

            g.log.info('Custom xattr user.foo is presented on mount point'
                       ' %s:%s ', mount_point.client_system, folder_name)

            for brick in get_all_bricks(self.mnode, self.volname):
                brick_server, brick_dir = brick.split(':')
                brick_path = dir_prefix.format(root=brick_dir,
                                               client_index=mount_index)

                ret = get_fattr(brick_server, brick_path, 'user.foo')

                g.log.debug('Check custom xattr for directory on brick %s:%s',
                            brick_server, brick_path)
                self.assertEqual('bar2', ret,
                                 "Expected: user.foo should be on brick %s\n"
                                 "Actual: Value of attribute foo.bar %s" %
                                 (brick_path, ret))
                g.log.info('Custom xattr is presented on brick %s',
                           brick_path)

            # Delete custom attribute
            ret = delete_fattr(mount_point.client_system, folder_name,
                               'user.foo')
            self.assertTrue(ret, "Failed to delete custom attribute")

            g.log.info('Removed custom attribute from directory %s:%s',
                       mount_point.client_system, folder_name)
            # Verify that custom xattr is not displayed after delete
            # on mount point and on the bricks

            g.log.debug('Looking if custom extra attribute user.foo is '
                        'presented on mount or on bricks after deletion')
            self.assertIsNone(get_fattr(mount_point.client_system,
                                        folder_name, 'user.foo'),
                              "Xattr user.foo is presented on mount point"
                              " %s:%s after deletion" %
                              (mount_point.mountpoint, folder_name))

            g.log.info("Xattr user.foo is not presented after deletion"
                       " on mount point %s:%s",
                       mount_point.mountpoint, folder_name)

            for brick in get_all_bricks(self.mnode, self.volname):
                brick_server, brick_dir = brick.split(':')
                brick_path = dir_prefix.format(root=brick_dir,
                                               client_index=mount_index)
                self.assertIsNone(get_fattr(brick_server, brick_path,
                                            'user.foo'),
                                  "Deleted xattr user.foo is presented on "
                                  "brick %s:%s" % (brick, brick_path))
                g.log.info('Custom attribute is not presented after delete '
                           'from directory on brick %s:%s', brick, brick_path)

        # Repeat all of the steps for link of created directory
        for mount_index, mount_point in enumerate(self.mounts):
            linked_folder_name = dir_prefix.format(
                root=mount_point.mountpoint,
                client_index="%s_linked" % mount_index
            )
            folder_name = dir_prefix.format(
                root=mount_point.mountpoint,
                client_index=mount_index
            )
            # Create link to created dir
            command = 'ln -s {src} {dst}'.format(dst=linked_folder_name,
                                                 src=folder_name)
            ret, _, _ = g.run(mount_point.client_system, command)
            self.assertEqual(0, ret,
                             'Failed to create link %s to directory %s' % (
                                 linked_folder_name, folder_name))
            self.assertTrue(file_exists(mount_point.client_system,
                                        linked_folder_name),
                            'Link does not exists on %s:%s' %
                            (mount_point.client_system, linked_folder_name))
            g.log.info('Create link %s to directory %s', linked_folder_name,
                       folder_name)

            # Verify that hash layout values are set on each
            # bricks for the link to dir
            g.log.debug("Verifying hash layout values")
            ret = validate_files_in_dir(mount_point.client_system,
                                        mount_point.mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS,
                                        file_type=FILETYPE_LINK)
            self.assertTrue(ret, "Expected - Link to directory is stored "
                                 "on hashed bricks")
            g.log.info("Hash layout values are set on each bricks")

            # Verify that mount point should not display xattr :
            # trusted.gfid and dht
            g.log.debug("Loading extra attributes")
            ret = get_fattr_list(mount_point.client_system, linked_folder_name)

            self.assertTrue('trusted.gfid' not in ret,
                            "Extended attribute trudted.gfid is presented on "
                            "mount point %s and folder %s"
                            % (mount_point.mountpoint, linked_folder_name))

            self.assertTrue('trusted.glusterfs.dht' not in ret,
                            "Extended attribute trusted.glusterfs.dht is "
                            "presented on mount point %s and folder %s"
                            % (mount_point.mountpoint, linked_folder_name))

            g.log.info('Extended attributes trusted.gfid and '
                       'trusted.glusterfs.dht does not exists on '
                       'mount point %s:%s ',
                       mount_point.mountpoint, linked_folder_name)

            # Verify that mount point shows pathinfo xattr
            g.log.debug("Check if pathinfo is presented on %s:%s",
                        mount_point.client_system, linked_folder_name)
            self.assertIsNotNone(get_fattr(mount_point.client_system,
                                           mount_point.mountpoint,
                                           'trusted.glusterfs.pathinfo'),
                                 "pathinfo is not displayed on mountpoint "
                                 "%s:%s" % (mount_point.client_system,
                                            linked_folder_name))
            g.log.info('pathinfo value is displayed on mount point %s:%s',
                       mount_point.client_system, linked_folder_name)

            # Set custom Attribute to link
            g.log.debug("Set custom xattribute user.foo to %s:%s",
                        mount_point.client_system, linked_folder_name)
            self.assertTrue(set_fattr(mount_point.client_system,
                                      linked_folder_name, 'user.foo', 'bar2'))
            g.log.info('Successful in set custom attribute to %s:%s',
                       mount_point.client_system, linked_folder_name)

            # Verify that custom xattr for directory is displayed
            # on mount point and bricks
            g.log.debug('Check mountpoint and bricks for custom xattribute')
            self.assertEqual('bar2', get_fattr(mount_point.client_system,
                                               linked_folder_name,
                                               'user.foo'),
                             'Custom xattribute is not presented on '
                             'mount point %s:%s' %
                             (mount_point.client_system, linked_folder_name))
            g.log.info("Custom xattribute is presented on mount point %s:%s",
                       mount_point.client_system, linked_folder_name)
            for brick in get_all_bricks(self.mnode, self.volname):
                brick_server, brick_dir = brick.split(':')
                brick_path = dir_prefix. \
                    format(root=brick_dir,
                           client_index="%s_linked" % mount_index)
                cmd = '[ -f %s ] && echo "yes" || echo "no"' % brick_path
                # Check if link exists
                _, ret, _ = g.run(brick_server, cmd)
                if 'no' in ret:
                    g.log.info("Link %s:%s does not exists",
                               brick_server, brick_path)
                    continue

                self.assertEqual(get_fattr(brick_server, brick_path,
                                           'user.foo'), 'bar2',
                                 "Actual: custom attribute not "
                                 "found on brick %s:%s" % (
                                     brick_server, brick_path))
                g.log.info('Custom xattr for link found on brick %s:%s',
                           brick, brick_path)

            # Delete custom attribute
            g.log.debug('Removing customer attribute on mount point %s:%s',
                        mount_point.client_system, linked_folder_name)
            self.assertTrue(delete_fattr(mount_point.client_system,
                                         linked_folder_name, 'user.foo'),
                            'Fail on delete xattr user.foo')
            g.log.info('Deleted custom xattr from link %s:%s',
                       mount_point.client_system, linked_folder_name)

            # Verify that custom xattr is not displayed after delete
            # on mount point and on the bricks
            g.log.debug("Check if custom xattr is presented on %s:%s "
                        "after deletion", mount_point.client_system,
                        linked_folder_name)
            self.assertIsNone(get_fattr(mount_point.client_system,
                                        linked_folder_name, 'user.foo'),
                              "Expected: xattr user.foo to be not presented on"
                              " %s:%s" % (mount_point.client_system,
                                          linked_folder_name))
            g.log.info("Custom xattr user.foo is not presented on %s:%s",
                       mount_point.client_system, linked_folder_name)
            for brick in get_all_bricks(self.mnode, self.volname):
                brick_server, brick_dir = brick.split(':')
                brick_path = dir_prefix. \
                    format(root=brick_dir,
                           client_index="%s_linked" % mount_index)
                cmd = '[ -f %s ] && echo "yes" || echo "no"' % brick_path
                # Check if link exists
                _, ret, _ = g.run(brick_server, cmd)
                if 'no' in ret:
                    g.log.info("Link %s:%s does not exists",
                               brick_server, brick_path)
                    continue

                self.assertIsNone(get_fattr(brick_server, brick_path,
                                            'user.foo'),
                                  "Extended custom attribute is presented on "
                                  "%s:%s after deletion" % (brick_server,
                                                            brick_path))
                g.log.info('Custom attribute is not presented after delete '
                           'from link on brick %s:%s', brick_server,
                           brick_path)

        g.log.info('Directory - custom extended attribute validation getfattr,'
                   ' setfattr is successful')

    def tearDown(self):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
