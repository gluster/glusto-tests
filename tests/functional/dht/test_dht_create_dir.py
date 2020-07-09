#  Copyright (C) 2017-2020 Red Hat, Inc. <http://www.redhat.com>
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

from re import search
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


@runs_on([['distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'replicated',
           'arbiter', 'distributed-arbiter'],
          ['glusterfs']])
class TestDhtClass(GlusterBaseClass):

    """
    Description: tests to check the dht layouts of files and directories,
                 along with their symlinks.
    """
    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_create_directory(self):

        m_point = self.mounts[0].mountpoint
        command = 'mkdir -p ' + m_point + '/root_dir/test_dir{1..3}'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "directory creation failed on %s"
                         % self.mounts[0].mountpoint)
        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "ls failed on parent directory:root_dir")
        g.log.info("ls on parent directory: successful")

        command = 'touch ' + m_point + \
            '/root_dir/test_file{1..5} ' + m_point + \
            '/root_dir/test_dir{1..3}/test_file{1..5}'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "File creation: failed")
        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to list the created directories")
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
        g.log.info("Creation of files at multiple levels successful")

        command = 'cd ' + m_point + ';find root_dir -type d -print'
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Creation of directory list failed")
        list_of_all_dirs = out.split('\n')
        del list_of_all_dirs[-1]

        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/root_dir',
                                     test_type=k.TEST_LAYOUT_IS_COMPLETE)
        self.assertTrue(flag, "Layout has some holes or overlaps")
        g.log.info("Layout is completely set")

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
            self.assertTrue(flag, ("The gfid for the directory %s is not "
                                   "same on all the bricks", direc))
        g.log.info("The gfid for each directory is the same on all the "
                   "bricks")

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

        for direc in list_of_all_dirs:
            fattr = get_fattr(self.mounts[0].client_system,
                              self.mounts[0].mountpoint+'/'+direc,
                              'trusted.glusterfs.pathinfo')
            self.assertTrue(fattr, ("Pathinfo not displayed for the "
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

        m_point = self.mounts[0].mountpoint
        fqpath_for_test_dir = m_point + '/test_dir'
        flag = mkdir(self.clients[0], fqpath_for_test_dir, True)
        self.assertTrue(flag, "Failed to create a directory")
        fqpath = m_point + '/test_dir/dir{1..3}'
        flag = mkdir(self.clients[0], fqpath, True)
        self.assertTrue(flag, "Failed to create sub directories")
        flag = validate_files_in_dir(self.clients[0],
                                     fqpath_for_test_dir,
                                     test_type=k.TEST_LAYOUT_IS_COMPLETE)
        self.assertTrue(flag, "Layout of test directory is not complete")
        g.log.info("Layout for directory is complete")

        sym_link_path = m_point + '/' + 'test_sym_link'
        command = 'ln -s ' + fqpath_for_test_dir + ' ' + sym_link_path
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to create symlink for test_dir")

        command = 'stat ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Stat command didn't return the details "
                                 "correctly")
        flag = False
        if 'symbolic link' in out:
            flag = True
        self.assertTrue(flag, "The type of the link is not symbolic")
        g.log.info("The link is symbolic")
        flag = False
        if search(fqpath_for_test_dir, out):
            flag = True
        self.assertTrue(flag, "sym link does not point to correct "
                              "location")
        g.log.info("sym link points to right directory")
        g.log.info("The details of the symlink are correct")

        command = 'ls -id ' + fqpath_for_test_dir + ' ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Inode numbers not retrieved by the "
                                 "ls command")
        list_of_inode_numbers = out.split('\n')
        if (list_of_inode_numbers[0].split(' ')[0] ==
                list_of_inode_numbers[1].split(' ')[0]):
            flag = False
        self.assertTrue(flag, "The inode numbers of the dir and sym link "
                              "are same")
        g.log.info("Verified: inode numbers of the test_dir "
                   "and its sym link are different")

        command = 'ls ' + sym_link_path
        ret, out1, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to list the contents using the "
                                 "sym link")
        command = 'ls ' + fqpath_for_test_dir
        ret, out2, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to list the contents of the "
                                 "test_dir using ls command")
        flag = False
        if out1 == out2:
            flag = True
        self.assertTrue(flag, "The contents listed using the sym link "
                              "are not the same")
        g.log.info("The contents listed using the symlink are"
                   " the same as that of the test_dir")

        command = 'getfattr -d -m . -e hex ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "failed to retrieve xattrs")
        list_xattrs = ['trusted.gfid', 'trusted.glusterfs.dht']
        for xattr in list_xattrs:
            if xattr in out:
                flag = False
        self.assertTrue(flag, "Important xattrs are being compromised"
                              " using the symlink at the mount point")
        g.log.info("Verified: mount point doesn't display important "
                   "xattrs using the symlink")

        path_info_1 = get_pathinfo(self.mounts[0].client_system,
                                   fqpath_for_test_dir)
        path_info_2 = get_pathinfo(self.mounts[0].client_system,
                                   sym_link_path)
        if path_info_1 == path_info_2:
            flag = True
        self.assertTrue(flag, "Pathinfos for test_dir and its sym link "
                              "are not same")
        g.log.info("Pathinfos for test_dir and its sym link are same")

        command = 'readlink ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "readlink command returned an error")
        flag = False
        if out.rstrip() == fqpath_for_test_dir:
            flag = True
        self.assertTrue(flag, "readlink did not return the path of the "
                              "test_dir")
        g.log.info("readlink successfully returned the path of the test_dir")
