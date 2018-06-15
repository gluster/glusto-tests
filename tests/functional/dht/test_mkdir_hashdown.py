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
    Test cases in this module tests mkdir operation with subvol down
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import bring_bricks_offline, \
     bring_bricks_online, are_bricks_offline, are_bricks_online
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.glusterfile import file_exists
from glustolibs.gluster.dht_test_utils import find_hashed_subvol
from glustolibs.gluster.dht_test_utils import create_brickobjectlist


@runs_on([['distributed-replicated', 'distributed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class TestMkdirHashdown(GlusterBaseClass):
    '''
    test case:
        - bring down a subvol
        - create a directory so that it hashes the down subvol
        - make sure mkdir does not succeed
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

    def test_mkdir_with_subvol_down(self):
        '''
        Test mkdir hashed to a down subvol
        '''
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-branches
        # pylint: disable=too-many-statements
        # pylint: disable=W0212
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # directory that needs to be created
        parent_dir = mountpoint + '/parent'
        child_dir = mountpoint + '/parent/child'

        # get hashed subvol for name "parent"
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        hashed, count = find_hashed_subvol(subvols, "/", "parent")
        self.assertIsNotNone(hashed, "Could not find hashed subvol")

        # bring target_brick offline
        bring_bricks_offline(self.volname, subvols[count])
        ret = are_bricks_offline(self.mnode, self.volname, subvols[count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[count]))
        g.log.info('target subvol is offline')

        # create parent dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        self.assertNotEqual(ret, 0, ('Expected mkdir of %s to fail with %s',
                                     parent_dir, err))
        g.log.info('mkdir of dir %s failed as expected', parent_dir)

        # check that parent_dir does not exist on any bricks and client
        brickobject = create_brickobjectlist(subvols, "/")
        for brickdir in brickobject:
            adp = "%s/parent" % brickdir.path
            bpath = adp.split(":")
            self.assertTrue((file_exists(brickdir._host, bpath[1])) == 0,
                            ('Expected dir %s not to exist on servers',
                             parent_dir))

        for client in self.clients:
            self.assertTrue((file_exists(client, parent_dir)) == 0,
                            ('Expected dir %s not to exist on clients',
                             parent_dir))

        g.log.info('dir %s does not exist on mount as expected', parent_dir)

        # Bring up the subvols and create parent directory
        bring_bricks_online(self.mnode, self.volname, subvols[count],
                            bring_bricks_online_methods=None)
        ret = are_bricks_online(self.mnode, self.volname, subvols[count])
        self.assertTrue(ret, ("Error in bringing back subvol %s online",
                              subvols[count]))
        g.log.info('Subvol is back online')

        ret, _, _ = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        self.assertEqual(ret, 0, ('Expected mkdir of %s to succeed',
                                  parent_dir))
        g.log.info('mkdir of dir %s successful', parent_dir)

        # get hash subvol for name "child"
        hashed, count = find_hashed_subvol(subvols, "parent", "child")
        self.assertIsNotNone(hashed, "Could not find hashed subvol")

        # bring target_brick offline
        bring_bricks_offline(self.volname, subvols[count])
        ret = are_bricks_offline(self.mnode, self.volname, subvols[count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[count]))
        g.log.info('target subvol is offline')

        # create child dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % child_dir))
        self.assertNotEqual(ret, 0, ('Expected mkdir of %s to fail with %s',
                                     child_dir, err))
        g.log.info('mkdir of dir %s failed', child_dir)

        # check if child_dir exists on any bricks
        for brickdir in brickobject:
            adp = "%s/parent/child" % brickdir.path
            bpath = adp.split(":")
            self.assertTrue((file_exists(brickdir._host, bpath[1])) == 0,
                            ('Expected dir %s not to exist on servers',
                             child_dir))
        for client in self.clients:
            self.assertTrue((file_exists(client, child_dir)) == 0)

        g.log.info('dir %s does not exist on mount as expected', child_dir)

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
