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
    Test cases in this module tests directory healing
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online, get_all_bricks)
from glustolibs.gluster.brickdir import check_hashrange
from glustolibs.gluster.glusterfile import get_file_stat, calculate_hash


@runs_on([['distributed', 'distributed-replicated',
           'distributed-dispersed', 'distributed-arbiter'],
          ['glusterfs', 'nfs']])
class TestDirHeal(GlusterBaseClass):
    '''
    test case: (directory healing)
    - bring down a subvol
    - create a directory so that it does not hash to down subvol
    - bring up the subvol
    - Check if directory is healed
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

    def test_directory_heal(self):
        '''
        Test directory healing.
        '''
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements
        # pylint: disable=protected-access

        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # directory that needs to be created
        parent_dir = mountpoint + '/parent'
        target_dir = mountpoint + '/parent/child'

        # create parent dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        g.log.info("mkdir of parent directory %s successful", parent_dir)
        self.assertEqual(ret, 0, ('mkdir failed for %s err: %s', parent_dir,
                                  err))
        g.log.info("mkdir of parent successful")

        # find non-hashed subvol for child
        hashed, non_hashed = [], []
        hash_num = calculate_hash(self.mnode, "child")
        bricklist = get_all_bricks(self.mnode, self.volname)
        for brick in bricklist:
            ret = check_hashrange(brick + "/parent")
            hash_range_low = ret[0]
            hash_range_high = ret[1]
            if hash_range_low <= hash_num <= hash_range_high:
                hashed.append(brick)

        non_hashed = [brick for brick in bricklist if brick not in hashed]
        g.log.info("Non-hashed bricks are: %s", non_hashed)

        # bring non_hashed offline
        for brick in non_hashed:
            ret = bring_bricks_offline(self.volname, brick)
            self.assertTrue(ret, ('Error in bringing down brick %s',
                                  brick))
            g.log.info('Non-hashed brick %s is offline', brick)

        # create child directory
        runc = ("mkdir %s" % target_dir)
        ret, _, _ = g.run(self.clients[0], runc)
        self.assertEqual(ret, 0, ('failed to create dir %s' % target_dir))
        g.log.info('mkdir successful %s', target_dir)

        # Check that the dir is not created on the down brick
        for brick in non_hashed:
            non_hashed_host, dir_path = brick.split(":")
            brickpath = ("%s/parent/child" % dir_path)
            ret, _, _ = g.run(non_hashed_host, ("stat %s" % brickpath))
            self.assertEqual(ret, 1, ("Expected %s to be not present on %s" %
                                      (brickpath, non_hashed_host)))
            g.log.info("Stat of %s failed as expected", brickpath)

        # bring up the subvol
        ret = bring_bricks_online(
            self.mnode, self.volname, non_hashed,
            bring_bricks_online_methods='volume_start_force')
        self.assertTrue(ret, "Error in bringing back subvol online")
        g.log.info("Subvol is back online")

        runc = ("ls %s" % target_dir)
        ret, _, _ = g.run(self.clients[0], runc)
        self.assertEqual(ret, 0, ("Lookup on %s failed", target_dir))
        g.log.info("Lookup is successful on %s", target_dir)

        # check if the directory is created on non_hashed
        for brick in non_hashed:
            non_hashed_host, dir_path = brick.split(":")
            absolutedirpath = ("%s/parent/child" % dir_path)
            ret = get_file_stat(non_hashed_host, absolutedirpath)
            self.assertIsNotNone(ret, "Directory is not present on non_hashed")
            g.log.info("Directory is created on non_hashed subvol")

        # check if directory is healed => i.e. layout is zeroed out
        for brick in non_hashed:
            brick_path = ("%s/parent/child" % brick)
            ret = check_hashrange(brick_path)
            hash_range_low = ret[0]
            hash_range_high = ret[1]
            if not hash_range_low and not hash_range_high:
                g.log.info("Directory healing successful")
            else:
                g.log.error("Directory is not healed")

    @classmethod
    def tearDownClass(cls):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        cls.get_super_method(cls, 'tearDownClass')()
