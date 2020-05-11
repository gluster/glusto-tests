#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable=too-many-statements, undefined-loop-variable
# pylint: disable=too-many-branches,too-many-locals,pointless-string-statement

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
import glustolibs.gluster.constants as k
from glustolibs.gluster.glusterfile import (get_fattr, get_pathinfo,
                                            get_fattr_list)
from glustolibs.gluster.glusterdir import mkdir

"""
Description: tests to check the dht layouts of files and directories,
             along with their symlinks.
"""


@runs_on([['replicated',
           'distributed',
           'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestDhtClass(GlusterBaseClass):

    """
    Description: tests to check the dht layouts of files and directories,
                 along with their symlinks.
    """
    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_create_directory(self):

        g.log.info("creating multiple,multilevel directories")
        m_point = self.mounts[0].mountpoint
        command = 'mkdir -p ' + m_point + '/root_dir/test_dir{1..3}'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "directory creation failed on %s"
                         % self.mounts[0].mountpoint)
        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "ls failed on parent directory:root_dir")
        g.log.info("ls on parent directory: successful")

        g.log.info("creating files at different directory levels inside %s",
                   self.mounts[0].mountpoint)
        command = 'touch ' + m_point + \
            '/root_dir/test_file{1..5} ' + m_point + \
            '/root_dir/test_dir{1..3}/test_file{1..5}'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "File creation: failed")
        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "can't list the created directories")
        list_of_files_and_dirs = out.split('\n')
        flag = True
        for x_count in range(3):
            dir_name = 'test_dir%d' % (x_count+1)
            if dir_name not in list_of_files_and_dirs:
                flag = False
        for x_count in range(5):
            file_name = 'test_file%d' % (x_count+1)
            if file_name not in list_of_files_and_dirs:
                flag = False
        self.assertTrue(flag, "ls command didn't list all the "
                        "directories and files")
        g.log.info("creation of files at multiple levels successful")

        g.log.info("creating a list of all directories")
        command = 'cd ' + m_point + ';find root_dir -type d -print'
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "creation of directory list failed")
        list_of_all_dirs = out.split('\n')
        del list_of_all_dirs[-1]

        g.log.info("verifying that all the directories are present on "
                   "every brick and the layout ranges are correct")
        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/root_dir',
                                     test_type=k.TEST_LAYOUT_IS_COMPLETE)
        self.assertTrue(flag, "Layout has some holes or overlaps")
        g.log.info("Layout is completely set")

        g.log.info("Checking if gfid xattr of directories is displayed and"
                   "is same on all the bricks on the server node")
        brick_list = get_all_bricks(self.mnode, self.volname)
        for direc in list_of_all_dirs:
            list_of_gfid = []
            for brick in brick_list:
                # the partition function returns a tuple having 3 elements.
                # the host address, the character passed i.e. ':'
                # , and the brick path
                brick_tuple = brick.partition(':')
                brick_path = brick_tuple[2]
                gfid = get_fattr(brick_tuple[0], brick_path + '/' + direc,
                                 'trusted.gfid')
                list_of_gfid.append(gfid)
            flag = True
            for x_count in range(len(list_of_gfid) - 1):
                if list_of_gfid[x_count] != list_of_gfid[x_count + 1]:
                    flag = False
            self.assertTrue(flag, ("the gfid for the directory %s is not "
                                   "same on all the bricks", direc))
        g.log.info("the gfid for each directory is the same on all the "
                   "bricks")

        g.log.info("Verify that for all directories mount point "
                   "should not display xattr")
        for direc in list_of_all_dirs:
            list_of_xattrs = get_fattr_list(self.mounts[0].client_system,
                                            self.mounts[0].mountpoint
                                            + '/' + direc)
            if 'security.selinux' in list_of_xattrs:
                del list_of_xattrs['security.selinux']
            self.assertFalse(list_of_xattrs, "one or more xattr being "
                                             "displayed on mount point")
        g.log.info("Verified : mount point not displaying important "
                   "xattrs")

        g.log.info("Verifying that for all directories only mount point "
                   "shows pathinfo xattr")
        for direc in list_of_all_dirs:
            fattr = get_fattr(self.mounts[0].client_system,
                              self.mounts[0].mountpoint+'/'+direc,
                              'trusted.glusterfs.pathinfo')
            self.assertTrue(fattr, ("pathinfo not displayed for the "
                                    "directory %s on mount point", direc))
        brick_list = get_all_bricks(self.mnode, self.volname)
        for direc in list_of_all_dirs:
            for brick in brick_list:
                host = brick.partition(':')[0]
                brick_path = brick.partition(':')[2]
                fattr = get_fattr(host, brick_path + '/' + direc,
                                  'trusted.glusterfs.pathinfo')
                self.assertIsNone(fattr, "subvolume displaying pathinfo")
        g.log.info("Verified: only mount point showing pathinfo "
                   "for all the directories")

    def test_create_link_for_directory(self):

        g.log.info("creating a directory at mount point")
        m_point = self.mounts[0].mountpoint
        test_dir_path = 'test_dir'
        fqpath = m_point + '/' + test_dir_path
        flag = mkdir(self.clients[0], fqpath, True)
        self.assertTrue(flag, "failed to create a directory")
        fqpath = m_point + '/' + test_dir_path + '/dir{1..3}'
        flag = mkdir(self.clients[0], fqpath, True)
        self.assertTrue(flag, "failed to create sub directories")
        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/test_dir',
                                     test_type=k.TEST_LAYOUT_IS_COMPLETE)
        self.assertTrue(flag, "layout of test directory is complete")
        g.log.info("directory created successfully")

        g.log.info("creating a symlink for test_dir")
        sym_link_path = m_point + '/' + 'test_sym_link'
        command = 'ln -s ' + m_point + '/test_dir ' + sym_link_path
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "failed to create symlink for test_dir")

        command = 'stat ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "stat command didn't return the details "
                                 "correctly")
        flag = False
        g.log.info("checking if the link is symbolic")
        if 'symbolic link' in out:
            flag = True
        self.assertTrue(flag, "the type of the link is not symbolic")
        g.log.info("the link is symbolic")
        g.log.info("checking if the sym link points to right directory")
        index_start = out.find('->') + 6
        index_end = out.find("\n") - 3
        dir_pointed = out[index_start:index_end]
        flag = False
        if dir_pointed == m_point + '/' + test_dir_path:
            flag = True
        self.assertTrue(flag, "sym link does not point to correct "
                              "location")
        g.log.info("sym link points to right directory")
        g.log.info("The details of the symlink are correct")

        g.log.info("verifying that inode number of the test_dir "
                   "and its sym link are different")
        command = 'ls -id ' + m_point + '/' + \
            test_dir_path + ' ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "inode numbers not retrieved by the "
                                 "ls command")
        list_of_inode_numbers = out.split('\n')
        flag = True
        if (list_of_inode_numbers[0].split(' ')[0] ==
                list_of_inode_numbers[1].split(' ')[0]):
            flag = False
        self.assertTrue(flag, "the inode numbers of the dir and sym link "
                              "are same")
        g.log.info("verified: inode numbers of the test_dir "
                   "and its sym link are different")

        g.log.info("listing the contents of the test_dir from its sym "
                   "link")
        command = 'ls ' + sym_link_path
        ret, out1, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "failed to list the contents using the "
                                 "sym link")
        command = 'ls ' + m_point + '/' + test_dir_path
        ret, out2, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "failed to list the contents of the "
                                 "test_dir using ls command")
        flag = False
        if out1 == out2:
            flag = True
        self.assertTrue(flag, "the contents listed using the sym link "
                              "are not the same")
        g.log.info("the contents listed using the symlink are"
                   " the same as that of the test_dir")

        g.log.info("verifying that mount point doesn't display important "
                   "xattrs using the symlink")
        command = 'getfattr -d -m . -e hex ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "failed to retrieve xattrs")
        list_xattrs = ['trusted.gfid', 'trusted.glusterfs.dht']
        flag = True
        for xattr in list_xattrs:
            if xattr in out:
                flag = False
        self.assertTrue(flag, "important xattrs are being compromised"
                              " using the symlink at the mount point")
        g.log.info("verified: mount point doesn't display important "
                   "xattrs using the symlink")

        g.log.info("verifying that mount point shows path info xattr for the"
                   " test_dir and sym link and is same for both")
        path_info_1 = get_pathinfo(self.mounts[0].client_system,
                                   m_point + '/' + test_dir_path)
        path_info_2 = get_pathinfo(self.mounts[0].client_system,
                                   sym_link_path)
        if path_info_1 == path_info_2:
            flag = True
        self.assertTrue(flag, "pathinfos for test_dir and its sym link "
                              "are not same")
        g.log.info("pathinfos for test_dir and its sym link are same")

        g.log.info("verifying readlink on sym link at mount point returns "
                   "the name of the directory")
        command = 'readlink ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "readlink command returned an error")
        flag = False
        if out.rstrip() == m_point + '/' + test_dir_path:
            flag = True
        self.assertTrue(flag, "readlink did not return the path of the "
                              "test_dir")
        g.log.info("readlink successfully returned the path of the test_dir")
