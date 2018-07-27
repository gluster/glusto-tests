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
#  Test - Distribution based on hash value

from random import choice, randint
from uuid import uuid4

from glusto.core import Glusto as g

from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.constants import \
    TEST_FILE_EXISTS_ON_HASHED_BRICKS as FILE_ON_HASHED_BRICKS
from glustolibs.gluster.constants import TEST_LAYOUT_IS_COMPLETE
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir, rmdir
from glustolibs.gluster.glusterfile import (file_exists, get_fattr,
                                            get_fattr_list)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestDHTHashValue(GlusterBaseClass):
    def setUp(self):
        GlusterBaseClass.setUp.im_func(self)
        # Setup Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)

        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError("Failed to Setup and Mount Volume")

        self.temp_folder = '/tmp/%s' % uuid4()

    @classmethod
    def create_files(cls, host, root, files, content):
        """This method is responsible to create file structure by given
        sequence with the same content for all of the files
        Args:
            host (str): Remote host
            root (str): root file directory
            files (list|tuple): Sequence of file paths
            content (str): Textual file content for each of files.
        Returns:
            bool: True on success, False on error
        """
        for item in files:
            dir_name = root
            file_name = item
            if item.find('/') != -1:
                segments = item.split('/')
                folders_tree = "/".join(segments[:-1])
                file_name = segments[-1]
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

    def test_distribution_hash_value(self):
        """Test case tests DHT of files and directories based on hash value
        """
        # pylint: disable=too-many-locals
        for client_index, mount_obj in enumerate(self.mounts):
            client_host = mount_obj.client_system
            mountpoint = mount_obj.mountpoint

            # Create directory for initial data
            g.log.debug("Creating temporary folder on client's machine %s:%s",
                        client_host, self.temp_folder)
            if not mkdir(client_host, self.temp_folder):
                g.log.error("Failed create temporary directory "
                            "on client machine %s:%s",
                            client_host, self.temp_folder)
                raise ExecutionError("Failed create temporary directory "
                                     "on client machine %s:%s" %
                                     (client_host, self.temp_folder))
            g.log.info('Created temporary directory on client machine %s:%s',
                       client_host, self.temp_folder)
            # Prepare a set of data
            files = ["{prefix}{file_name}_{client_index}".
                     format(file_name=file_name,
                            client_index=client_index,
                            prefix='' if randint(1, 6) % 2
                            else choice('ABCD') + '/')
                     for file_name in map(chr, range(97, 123))]
            ret = self.create_files(client_host, self.temp_folder,
                                    files,
                                    "Lorem Ipsum is simply dummy text of the "
                                    "printing and typesetting industry.")
            self.assertTrue(ret, "Failed creating a set of files and dirs "
                                 "on %s:%s" % (client_host, self.temp_folder))
            g.log.info('Created data set on client machine on folder %s:%s',
                       client_host, self.temp_folder)

            # Copy prepared data to mount point
            cmd = ('cp -vr {source}/* {destination}'.format(
                source=self.temp_folder,
                destination=mountpoint))
            ret, _, _ = g.run(client_host, cmd)
            self.assertEqual(ret, 0, "Copy data to mount point %s:%s Failed")
            g.log.info('Copied prepared data to mount point %s:%s',
                       client_host, mountpoint)

            # Verify that hash layout values are set on each
            # bricks for the dir
            g.log.debug("Verifying DHT layout")
            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=TEST_LAYOUT_IS_COMPLETE)
            self.assertTrue(ret, "TEST_LAYOUT_IS_COMPLETE: FAILED")
            g.log.info("TEST_LAYOUT_IS_COMPLETE: PASS on %s:%s ",
                       client_host, mountpoint)

            g.log.debug("Verifying files and directories")
            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS,
                                        file_type=FILETYPE_DIRS)
            self.assertTrue(ret, "TEST_FILE_EXISTS_ON_HASHED_BRICKS: FAILED")
            g.log.info("TEST_FILE_EXISTS_ON_HASHED_BRICKS: PASS")

            # Verify "trusted.gfid" extended attribute of the
            # directory/file on all the bricks
            gfids = dict()
            g.log.debug("Check if trusted.gfid is presented on the bricks")
            for brick_item in get_all_bricks(self.mnode, self.volname):
                brick_host, brick_dir = brick_item.split(':')

                for target_destination in files:
                    if not file_exists(brick_host, '{brick_dir}/{dest}'.
                                       format(brick_dir=brick_dir,
                                              dest=target_destination)):
                        continue
                    ret = get_fattr(brick_host, '%s/%s' %
                                    (brick_dir, target_destination),
                                    'trusted.gfid')
                    self.assertIsNotNone(ret,
                                         "trusted.gfid is not presented "
                                         "on %s/%s" % (brick_dir,
                                                       target_destination))
                    g.log.info("Verified trusted.gfid on brick %s:%s",
                               brick_item, target_destination)
                    gfids.setdefault(target_destination, []).append(ret)

            g.log.debug('Check if trusted.gfid is same on all the bricks')
            self.assertTrue(all([False if len(set(gfids[k])) > 1 else True
                                 for k in gfids]),
                            "trusted.gfid should be same on all the bricks")
            g.log.info('trusted.gfid is same on all the bricks')
            # Verify that mount point shows pathinfo xattr.
            g.log.debug("Check if pathinfo is presented on mount point "
                        "%s:%s", client_host, mountpoint)
            ret = get_fattr(client_host, mountpoint,
                            'trusted.glusterfs.pathinfo')
            self.assertIsNotNone(ret, "pathinfo is not presented on mount "
                                      "point %s:%s" % (client_host,
                                                       mountpoint))

            g.log.info('trusted.glusterfs.pathinfo is presented on mount'
                       ' point %s:%s', client_host, mountpoint)

            # Mount point should not display xattr:
            # trusted.gfid and trusted.glusterfs.dht
            g.log.debug("Check if trusted.gfid and trusted.glusterfs.dht are "
                        "not presented on mount point %s:%s", client_host,
                        mountpoint)
            attributes = get_fattr_list(client_host, mountpoint)
            self.assertFalse('trusted.gfid' in attributes,
                             "Expected: Mount point shouldn't display xattr:"
                             "{xattr}. Actual: xattrs {xattr} is "
                             "presented on mount point".
                             format(xattr='trusted.gfid'))
            self.assertFalse('trusted.glusterfs.dht' in attributes,
                             "Expected: Mount point shouldn't display xattr:"
                             "{xattr}. Actual: xattrs {xattr} is "
                             "presented on mount point".
                             format(xattr='trusted.glusterfs.dht'))

            g.log.info("trusted.gfid and trusted.glusterfs.dht are not "
                       "presented on mount point %s:%s", client_host,
                       mountpoint)
        g.log.info('Files and dirs are stored on bricks based on hash value')

    def tearDown(self):
        g.log.info("Tear down")

        for mount_point in self.mounts:
            g.log.debug('Removing temporary folder %s',
                        self.temp_folder)
            rmdir(mount_point.client_system, self.temp_folder,
                  force=True)

        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError('Failed to unmount and clean volumes')
        GlusterBaseClass.tearDown.im_func(self)
