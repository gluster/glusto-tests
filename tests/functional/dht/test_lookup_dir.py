#  Copyright (C) 2018-2020 Red Hat, Inc. <http://www.redhat.com>
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
    Test lookup directory with subvol down
"""

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (
    bring_bricks_offline,
    bring_bricks_online)
from glustolibs.gluster.brickdir import BrickDir
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.constants import \
    TEST_LAYOUT_IS_COMPLETE as LAYOUT_IS_COMPLETE
from glustolibs.gluster.dht_test_utils import validate_files_in_dir,\
     find_hashed_subvol, find_nonhashed_subvol


@runs_on([['distributed-replicated', 'distributed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'samba']])
class TestLookupDir(GlusterBaseClass):
    '''
    test case: (directory lookup)
    case -1:
        - bring down a subvol
        - create a directory so that it does not hash to down subvol
        - make sure stat is successful on the dir

    case -2:
        - create directory
        - bring down hashed subvol
        - make sure stat is successful on the dir

    case -3:
        - create dir
        - bringdown unhashed subvol
        - make sure stat is successful on the dir
    '''
    # Create Volume and mount according to config file
    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError("Failed to Setup and Mount Volume")

    def mkdir_post_hashdown(self, subvols, parent_dir):
        '''
        case -1:
        - bring down a subvol
        - create a directory so that it does not hash to down subvol
        - make sure stat is successful on the dir
        '''
        # pylint: disable=protected-access
        # pylint: disable=pointless-string-statement
        # Find a non hashed subvolume(or brick)
        nonhashed_subvol, count = find_nonhashed_subvol(subvols, "/", "parent")
        if nonhashed_subvol is None:
            g.log.error('Error in finding nonhashed subvol for parent')
            return False

        # bring nonhashed_subbvol offline
        ret = bring_bricks_offline(self.volname, subvols[count])
        if ret == 0:
            g.log.error('Error in bringing down subvolume %s',
                        subvols[count])
            return False

        g.log.info('target subvol %s is offline', subvols[count])

        # create parent dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        if ret != 0:
            g.log.error('mkdir failed for %s err: %s', parent_dir, err)
            return False
        g.log.info("mkdir of parent directory %s successful", parent_dir)

        # this confirms both layout and stat of the directory
        ret = validate_files_in_dir(self.clients[0],
                                    self.mounts[0].mountpoint + '/parent_dir',
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "Layout is not complete")
        g.log.info('Layout is complete')

        # bring up the subvol
        ret = bring_bricks_online(self.mnode, self.volname, subvols[count],
                                  bring_bricks_online_methods=None)
        if ret == 0:
            g.log.error("Error in bringing back subvol online")
            return False

        g.log.info('Subvol is back online')

        # delete parent_dir
        ret, _, err = g.run(self.clients[0], ("rmdir %s" % parent_dir))
        if ret != 0:
            g.log.error('rmdir failed for %s err: %s', parent_dir, err)
        g.log.info("rmdir of directory %s successful", parent_dir)

        return True

    def mkdir_before_hashdown(self, subvols, parent_dir):
        '''
        case -2:
            - create directory
            - bring down hashed subvol
            - make sure stat is successful on the dir
        '''
        # pylint: disable=protected-access
        # pylint: disable=pointless-string-statement
        # create parent dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        if ret != 0:
            g.log.error('mkdir failed for %s err: %s', parent_dir, err)
            return False
        g.log.info("mkdir of parent directory %s successful", parent_dir)

        # find hashed subvol
        hashed_subvol, count = find_hashed_subvol(subvols, "/", "parent")
        if hashed_subvol is None:
            g.log.error('Error in finding hash value')
            return False

        g.log.info("hashed subvol %s", hashed_subvol._host)

        # bring hashed_subvol offline
        ret = bring_bricks_offline(self.volname, subvols[count])
        if ret == 0:
            g.log.error('Error in bringing down subvolume %s', subvols[count])
            return False
        g.log.info('target subvol %s is offline', subvols[count])

        # this confirms both layout and stat of the directory
        ret = validate_files_in_dir(self.clients[0],
                                    self.mounts[0].mountpoint + '/parent_dir',
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "Layout is not complete")
        g.log.info('Layout is complete')

        # bring up the subvol
        ret = bring_bricks_online(self.mnode, self.volname, subvols[count],
                                  bring_bricks_online_methods=None)
        if ret == 0:
            g.log.error("Error in bringing back subvol online")
            return False
        g.log.info('Subvol is back online')

        # delete parent_dir
        ret, _, err = g.run(self.clients[0], ("rmdir %s" % parent_dir))
        if ret == 0:
            g.log.error('rmdir failed for %s err: %s', parent_dir, err)
        g.log.info("rmdir of directory %s successful", parent_dir)
        return True

    def mkdir_nonhashed_down(self, subvols, parent_dir):
        '''
        case -3:
            - create dir
            - bringdown a non-hashed subvol
            - make sure stat is successful on the dir
        '''
        # pylint: disable=protected-access
        # pylint: disable=pointless-string-statement
        # create parent dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        if ret != 0:
            g.log.error('mkdir failed for %s err: %s', parent_dir, err)
            return False

        g.log.info("mkdir of parent directory %s successful", parent_dir)

        # Find a non hashed subvolume(or brick)
        nonhashed_subvol, count = find_nonhashed_subvol(subvols, "/", "parent")
        if nonhashed_subvol is None:
            g.log.error('Error in finding hash value')
            return False

        # bring nonhashed_subbvol offline
        ret = bring_bricks_offline(self.volname, subvols[count])
        if ret == 0:
            g.log.error('Error in bringing down subvolume %s', subvols[count])
            return False
        g.log.info('target subvol %s is offline', subvols[count])

        # this confirms both layout and stat of the directory
        ret = validate_files_in_dir(self.clients[0],
                                    self.mounts[0].mountpoint + '/parent_dir',
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "Expected - Layout is complete")
        g.log.info('Layout is complete')

        # bring up the subvol
        ret = bring_bricks_online(self.mnode, self.volname, subvols[count],
                                  bring_bricks_online_methods=None)
        if ret == 0:
            g.log.error("Error in bringing back subvol online")
            return False
        g.log.info('Subvol is back online')

        # delete parent_dir
        ret, _, err = g.run(self.clients[0], ("rmdir %s" % parent_dir))
        if ret != 0:
            g.log.error('rmdir failed for %s err: %s', parent_dir, err)
            return False
        g.log.info("rmdir of directory %s successful", parent_dir)
        return True

    def test_lookup_dir(self):
        '''
        Test directory lookup.
        '''
        # pylint: disable=too-many-locals
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # directory that needs to be created
        parent_dir = mountpoint + '/parent'

        # calculate hash for name "parent"
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']

        # This populates one brick from one subvolume
        secondary_bricks = []
        for subvol in subvols:
            secondary_bricks.append(subvol[0])

        for subvol in secondary_bricks:
            g.log.debug("secondary bricks %s", subvol)

        brickobject = []
        for item in secondary_bricks:
            temp = BrickDir(item)
            brickobject.append(temp)

        ret = self.mkdir_post_hashdown(subvols, parent_dir)
        self.assertTrue(ret, 'mkdir_post_hashdown failed')

        ret = self.mkdir_before_hashdown(subvols, parent_dir)
        self.assertTrue(ret, 'mkdir_before_hashdown failed')

        ret = self.mkdir_nonhashed_down(subvols, parent_dir)
        self.assertTrue(ret, 'mkdir_nonhashed_down failed')

    def tearDown(self):

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
