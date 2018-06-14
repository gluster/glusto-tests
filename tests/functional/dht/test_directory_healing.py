#  Copyright (C) 2018-2019 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.gluster.brick_libs import bring_bricks_online
from glustolibs.gluster.brickdir import BrickDir
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.dht_test_utils import find_nonhashed_subvol


@runs_on([['distributed-replicated', 'distributed', 'distributed-dispersed'],
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
        GlusterBaseClass.setUp.im_func(self)

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
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']

        non_hashed, count = find_nonhashed_subvol(subvols, "parent", "child")
        self.assertIsNotNone(non_hashed, "could not find non_hashed subvol")

        g.log.info("non_hashed subvol %s", non_hashed._host)

        # bring non_hashed offline
        ret = bring_bricks_offline(self.volname, subvols[count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[count]))
        g.log.info('target subvol %s is offline', subvols[count])

        # create child directory
        runc = ("mkdir %s" % target_dir)
        ret, _, _ = g.run(self.clients[0], runc)
        self.assertEqual(ret, 0, ('failed to create dir %s' % target_dir))
        g.log.info('mkdir successful %s', target_dir)

        # Check that the dir is not created on the down brick
        brickpath = ("%s/child" % non_hashed._path)

        ret, _, _ = g.run(non_hashed._host, ("stat %s" % brickpath))
        self.assertEqual(ret, 1, ("Expected %s to be not present on %s" %
                                  (brickpath, non_hashed._host)))
        g.log.info("stat of %s failed as expected", brickpath)

        # bring up the subvol
        ret = bring_bricks_online(self.mnode, self.volname, subvols[count],
                                  bring_bricks_online_methods=None)
        self.assertTrue(ret, "Error in bringing back subvol online")
        g.log.info('Subvol is back online')

        runc = ("ls %s" % target_dir)
        ret, _, _ = g.run(self.clients[0], runc)
        self.assertEqual(ret, 0, ("lookup on %s failed", target_dir))
        g.log.info("lookup is successful on %s", target_dir)

        # check if the directory is created on non_hashed
        absolutedirpath = ("%s/child" % non_hashed._path)

        # check if directory is healed => i.e. layout is zeroed out
        temp = BrickDir(absolutedirpath)

        if temp is None:
            self.assertIsNot(temp, None, 'temp is None')

        ret = temp.has_zero_hashrange()
        self.assertTrue(ret, ("hash range is not there %s", ret))
        g.log.info("directory healing successful")

    @classmethod
    def tearDownClass(cls):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDownClass.im_func(cls)
