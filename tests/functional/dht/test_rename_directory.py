#  Copyright (C) 2017-2018 Red Hat, Inc. <http://www.redhat.com>
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
Description:
    Test cases in this module tests DHT Rename directory
"""

from glusto.core import Glusto as g

from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.constants import \
    TEST_FILE_EXISTS_ON_HASHED_BRICKS as FILE_ON_HASHED_BRICKS
from glustolibs.gluster.constants import \
    TEST_LAYOUT_IS_COMPLETE as LAYOUT_IS_COMPLETE
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import file_exists, move_file


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class TestDHTRenameDirectory(GlusterBaseClass):
    """DHT Tests - rename directory
    Scenarios:
    1 - Rename directory when destination is not presented
    2 - Rename directory when destination is presented
    """

    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError("Failed to Setup and Mount Volume")

        self.files = (
            'a.txt',
            'b.txt',
            'sub_folder/c.txt',
            'sub_folder/d.txt'
        )

    @classmethod
    def create_files(cls, host, root, files, content):
        """This method is responsible to create file structure by given
        sequence with the same content for all of the files
        :param host: Remote host
        :param root: root file directory
        :param files: Sequence of files
        :param content: Content for all of the files
        :type host: str
        :type root: str
        :type files: list or tuple or set
        :type content: str
        :rtype host: str
        :return: True on success / False on error
        :rtype: boolean
        """
        for item in files:
            dir_name = root
            file_name = item
            if item.find('/') != -1:
                folders_tree = "/".join(item.split('/')[:-1])
                file_name = item[-1]
                dir_name = '{root}/{folders_tree}'.format(
                    root=root, folders_tree=folders_tree)
                mkdir(host, dir_name, parents=True)
            cmd = 'echo "{content}" > {root}/{file}'.format(root=dir_name,
                                                            file=file_name,
                                                            content=content)
            ret, _, _ = g.run(host, cmd)
            if ret != 0:
                g.log.error('Error on file creation %s', cmd)
                return False
        return True

    def test_rename_directory_no_destination_folder(self):
        """Test rename directory with no destination folder
        """
        dirs = {
            'initial': '{root}/folder_{client_index}',
            'new_folder': '{root}/folder_renamed{client_index}'
        }

        for mount_index, mount_obj in enumerate(self.mounts):
            client_host = mount_obj.client_system
            mountpoint = mount_obj.mountpoint

            initial_folder = dirs['initial'].format(
                root=mount_obj.mountpoint,
                client_index=mount_index
            )

            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=LAYOUT_IS_COMPLETE,
                                        file_type=FILETYPE_DIRS)
            self.assertTrue(ret, "Expected - Layout is complete")
            g.log.info('Layout is complete')

            # Create source folder on mount point
            self.assertTrue(mkdir(client_host, initial_folder),
                            'Failed creating source directory')
            self.assertTrue(file_exists(client_host, initial_folder))
            g.log.info('Created source directory %s on mount point %s',
                       initial_folder, mountpoint)
            # Create files and directories
            ret = self.create_files(client_host, initial_folder, self.files,
                                    content='Textual content')

            self.assertTrue(ret, 'Unable to create files on mount point')
            g.log.info('Files and directories are created')

            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS)
            self.assertTrue(ret, "Expected - Files and dirs are stored "
                                 "on hashed bricks")
            g.log.info('Files and dirs are stored on hashed bricks')

            new_folder_name = dirs['new_folder'].format(
                root=mountpoint,
                client_index=mount_index
            )
            # Check if destination dir does not exist
            self.assertFalse(file_exists(client_host, new_folder_name),
                             'Expected New folder name should not exists')
            # Rename source folder
            ret = move_file(client_host, initial_folder,
                            new_folder_name)
            self.assertTrue(ret, "Rename direcoty failed")
            g.log.info('Renamed directory %s to %s', initial_folder,
                       new_folder_name)

            # Old dir does not exists and destination is presented
            self.assertFalse(file_exists(client_host, initial_folder),
                             '%s should be not listed' % initial_folder)
            g.log.info('The old directory %s does not exists on mount point',
                       initial_folder)
            self.assertTrue(file_exists(client_host, new_folder_name),
                            'Destination dir does not exists %s' %
                            new_folder_name)
            g.log.info('The new folder is presented %s', new_folder_name)

            # Check bricks for source and destination directories
            for brick_item in get_all_bricks(self.mnode, self.volname):
                brick_host, brick_dir = brick_item.split(':')

                initial_folder = dirs['initial'].format(
                    root=brick_dir,
                    client_index=mount_index
                )
                new_folder_name = dirs['new_folder'].format(
                    root=brick_dir,
                    client_index=mount_index
                )

                self.assertFalse(file_exists(brick_host, initial_folder),
                                 "Expected folder %s to be not presented" %
                                 initial_folder)
                self.assertTrue(file_exists(brick_host, new_folder_name),
                                'Expected folder %s to be presented' %
                                new_folder_name)

                g.log.info('The old directory %s does not exists and directory'
                           ' %s is presented' %
                           (initial_folder, new_folder_name))
        g.log.info('Rename directory when destination directory '
                   'does not exists is successful')

    def test_rename_directory_with_dest_folder(self):
        """Test rename directory with presented destination folder
        """
        dirs = {
            'initial_folder': '{root}/folder_{client_index}/',
            'new_folder': '{root}/new_folder_{client_index}/'
        }

        for mount_index, mount_obj in enumerate(self.mounts):
            client_host = mount_obj.client_system
            mountpoint = mount_obj.mountpoint

            initial_folder = dirs['initial_folder'].format(
                root=mount_obj.mountpoint,
                client_index=mount_index
            )

            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=LAYOUT_IS_COMPLETE,
                                        file_type=FILETYPE_DIRS)
            self.assertTrue(ret, "Expected - Layout is complete")
            g.log.info('Layout is complete')

            # Create a folder on mount point
            self.assertTrue(mkdir(client_host, initial_folder, parents=True),
                            'Failed creating source directory')
            self.assertTrue(file_exists(client_host, initial_folder))
            g.log.info('Created source directory %s on mount point %s',
                       initial_folder, mountpoint)

            new_folder_name = dirs['new_folder'].format(
                root=mountpoint,
                client_index=mount_index
            )
            # Create destination directory
            self.assertTrue(mkdir(client_host, new_folder_name, parents=True),
                            'Failed creating destination directory')
            self.assertTrue(file_exists(client_host, new_folder_name))
            g.log.info('Created destination directory %s on mount point %s',
                       new_folder_name, mountpoint)

            # Create files and directories
            ret = self.create_files(client_host, initial_folder, self.files,
                                    content='Textual content')
            self.assertTrue(ret, 'Unable to create files on mount point')
            g.log.info('Files and directories are created')

            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS)
            self.assertTrue(ret, "Expected - Files and dirs are stored "
                                 "on hashed bricks")
            g.log.info('Files and dirs are stored on hashed bricks')
            # Rename source folder to destination
            ret = move_file(client_host, initial_folder,
                            new_folder_name)
            self.assertTrue(ret, "Rename folder failed")
            g.log.info('Renamed folder %s to %s', initial_folder,
                       new_folder_name)

            # Old dir does not exists and destination is presented
            self.assertFalse(file_exists(client_host, initial_folder),
                             '%s should be not listed' % initial_folder)
            g.log.info('The old directory %s does not exists on mount point',
                       initial_folder)
            self.assertTrue(file_exists(client_host, new_folder_name),
                            'Renamed directory does not exists %s' %
                            new_folder_name)
            g.log.info('The new folder exists %s', new_folder_name)

            # Check bricks for source and destination directories
            for brick_item in get_all_bricks(self.mnode, self.volname):
                brick_host, brick_dir = brick_item.split(':')

                initial_folder = dirs['initial_folder'].format(
                    root=brick_dir,
                    client_index=mount_index
                )
                new_folder_name = dirs['new_folder'].format(
                    root=brick_dir,
                    client_index=mount_index
                )

                self.assertFalse(file_exists(brick_host, initial_folder),
                                 "Expected folder %s to be not presented" %
                                 initial_folder)
                self.assertTrue(file_exists(brick_host, new_folder_name),
                                'Expected folder %s to be presented' %
                                new_folder_name)

                g.log.info('The old directory %s does not exists and directory'
                           ' %s is presented' %
                           (initial_folder, new_folder_name))
        g.log.info('Rename directory when destination directory '
                   'exists is successful')

    def tearDown(self):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")
        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDownClass.im_func(self)
