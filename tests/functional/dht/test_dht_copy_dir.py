#  Copyright (C) 2020  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable=pointless-string-statement,too-many-locals
# pylint: disable=too-many-branches,too-many-statements,too-many-function-args

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
import glustolibs.gluster.constants as const
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'],
          ['glusterfs']])
class DhtCopyTest(GlusterBaseClass):

    destination_exists = False

    def copy_dir(self):
        """
        Description:
        This test creates a parent directory and subdirectories
        at mount point. After that it creates a copy of parent
        directory at mount point, first when destination
        directory is not there, and second sub-test creates a
        copy after creating destination directory for copying.
        In the first test, contents will be copied from one
        directory to another but in the second test case, entire
        directory will be copied to another directory along with
        the contents.Then it checks for correctness of layout
        and content of source and copied directory at all
        sub-vols.
        """

        g.log.info("creating multiple,multilevel directories")
        m_point = self.mounts[0].mountpoint
        fqpath = m_point + '/root_dir/test_dir{1..3}'
        client_ip = self.clients[0]
        flag = mkdir(client_ip, fqpath, True)
        self.assertTrue(flag, "Directory creation: failed")

        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "can't list the created directories")

        list_of_created_dirs = out.split('\n')
        flag = True
        for x_count in range(3):
            dir_name = 'test_dir%d' % (x_count + 1)
            if dir_name not in list_of_created_dirs:
                flag = False
        self.assertTrue(flag, "ls command didn't list all the directories")
        g.log.info("creation of multiple,multilevel directories created")

        g.log.info("creating files at different directory levels")
        command = 'touch ' + m_point + '/root_dir/test_file{1..5}'
        ret, _, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "files not created")

        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "can't list the created directories")

        list_of_files_and_dirs = out.split('\n')
        flag = True
        for x_count in range(3):
            dir_name = 'test_dir%d' % (x_count + 1)
            if dir_name not in list_of_files_and_dirs:
                flag = False
        for x_count in range(5):
            file_name = 'test_file%d' % (x_count + 1)
            if file_name not in list_of_files_and_dirs:
                flag = False
        self.assertTrue(
            flag, "ls command didn't list all the directories and files")
        g.log.info("creation of files at multiple levels successful")

        if not self.destination_exists:
            destination_dir = 'root_dir_1'
        else:
            fqpath = m_point + '/new_dir'
            flag = mkdir(client_ip, fqpath, True)
            self.assertTrue(flag, "new_dir not created")
            destination_dir = 'new_dir/root_dir'

        g.log.info("performing layout checks for root_dir")
        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/root_dir',
                                     const.TEST_FILE_EXISTS_ON_HASHED_BRICKS)
        self.assertTrue(flag, "root directory not present on every brick")

        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/root_dir',
                                     test_type=(
                                         const.TEST_LAYOUT_IS_COMPLETE))
        self.assertTrue(flag, "layout of every directory is complete")
        g.log.info("every directory is present on every brick and layout "
                   "of each brick is correct")

        g.log.info("copying root_dir at the mount point")
        command = "cp -r " + m_point + '/root_dir ' + m_point \
            + '/' + destination_dir
        ret, out, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "directory was not copied")

        g.log.info("performing layout checks for copied directory")

        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/' + destination_dir,
                                     const.TEST_FILE_EXISTS_ON_HASHED_BRICKS)
        self.assertTrue(flag, "directories not present on every brick")

        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/' + destination_dir,
                                     test_type=(
                                         const.TEST_LAYOUT_IS_COMPLETE))
        self.assertTrue(flag, "layout of every directory is complete")
        g.log.info("verified: layouts correct")

        g.log.info("listing the copied directory")
        command = 'ls -A1 ' + m_point + '/' + destination_dir
        ret, out, _ = g.run(client_ip, command)
        self.assertIsNotNone(out, "copied directory not listed")

        g.log.info("copied directory listed")
        command = 'ls -A1 ' + m_point + '/root_dir'
        ret, out1, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "details of root_dir not listed")

        command = 'ls -A1 ' + m_point + '/' + destination_dir
        ret, out2, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "details of copied dir not listed")
        self.assertEqual(out1, out2,
                         "contents and attributes of original and "
                         "copied directory not same")
        g.log.info("the contents and attributes of copied directory "
                   "are same")

        g.log.info("listing the copied directory on all the subvolumes")
        brick_list = get_all_bricks(self.mnode, self.volname)
        for brick in brick_list:

            brick_tuple = brick.partition(':')
            brick_path = brick_tuple[2]
            host_addr = brick_tuple[0]

            command = 'ls -A1 ' + brick_path + '/' + destination_dir
            ret, out, _ = g.run(host_addr, command)
            self.assertIsNotNone(out,
                                 ("copied directory not listed on brick "
                                  "%s", brick))

            g.log.info("copied directory listed on brick %s", brick)
            command = 'ls -l --time-style=\'+\' ' + brick_path \
                + '/root_dir/' + ' | grep ^d'
            ret, out1, _ = g.run(host_addr, command)
            self.assertEqual(ret, 0, "details of root_dir not listed on "
                                     "brick %s" % brick)

            command = 'ls -l --time-style=\'+\' ' + brick_path + '/' \
                + destination_dir + '| grep ^d'
            ret, out2, _ = g.run(host_addr, command)
            self.assertEqual(ret, 0, "details of copied dir not listed on "
                                     "brick %s" % brick)
            self.assertEqual(out1, out2,
                             "contents and attributes of original and "
                             "copied directory not same on brick "
                             "%s" % brick)
            g.log.info("the contents and attributes of copied directory "
                       "are same on brick %s", brick)
        g.log.info("the copied directory is present on all the subvolumes")

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

    @classmethod
    def tearDownClass(cls):

        # Cleanup Volume
        g.log.info("Starting to clean up Volume %s", cls.volname)
        ret = cls.unmount_volume_and_cleanup_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to create volume")
        g.log.info("Successful in cleaning up Volume %s", cls.volname)

        cls.get_super_method(cls, 'tearDownClass')()

    def test_copy_directory(self):

        # Checking when destination directory for copying directory doesn't
        # exist
        self.destination_exists = False
        self.copy_dir()

        # Checking by creating destination directory first and then copying
        # created directory
        self.destination_exists = True
        self.copy_dir()
