#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from random import choice

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (setup_volume, cleanup_volume,)
from glustolibs.gluster.volume_ops import (get_gluster_state, get_volume_list)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterfile import check_if_pattern_in_file


@runs_on([['distributed', 'replicated',
           'distributed-replicated',
           'dispersed', 'distributed-dispersed',
           'arbiter', 'distributed-arbiter'], []])
class TestGetStateOnBrickUnmount(GlusterBaseClass):
    """
    Tests to verify 'gluster get state' command on unmounting the brick from
    an online volume
    """

    @classmethod
    def setUpClass(cls):

        cls.get_super_method(cls, 'setUpClass')()

        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Failed to validate peers are in connected")
        g.log.info("Successfully validated peers are in connected state")

    def tearDown(self):

        # Mount the bricks which are unmounted as part of test
        if getattr(self, 'umount_host', None) and getattr(self, 'umount_brick',
                                                          None):
            ret, _, _ = g.run(self.umount_host, 'mount -a')
            if ret:
                raise ExecutionError("Not able to mount unmounted brick on "
                                     "{}".format(self.umount_host))

        vol_list = get_volume_list(self.mnode)
        if vol_list:
            for volume in vol_list:
                ret = cleanup_volume(self.mnode, volume)
                if ret:
                    g.log.info("Volume deleted successfully %s", volume)
                else:
                    raise ExecutionError(
                        "Not able to delete volume {}".format(volume))

        self.get_super_method(self, 'tearDown')()

    def test_get_state_on_brick_unmount(self):
        """
        Steps:
        1. Form a gluster cluster by peer probing and create a volume
        2. Unmount the brick using which the volume is created
        3. Run 'gluster get-state' and validate absence of error 'Failed to get
           daemon state. Check glusterd log file for more details'
        4. Create another volume and start it using different bricks which are
           not used to create above volume
        5. Run 'gluster get-state' and validate the absence of above error.
        """
        # Setup Volume
        ret = setup_volume(mnode=self.mnode,
                           all_servers_info=self.all_servers_info,
                           volume_config=self.volume, create_only=True)
        self.assertTrue(ret, "Failed to setup volume {}".format(self.volname))
        g.log.info("Successful in setting up volume %s", self.volname)

        # Select one of the bricks in the volume to unmount
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, ("Not able to get list of bricks "
                                          "of volume %s", self.volname))

        select_brick = choice(brick_list)
        self.umount_host, self.umount_brick = (
            select_brick[0:select_brick.rfind('/')].split(':'))

        # Verify mount entry in /etc/fstab
        ret = check_if_pattern_in_file(self.umount_host,
                                       self.umount_brick, '/etc/fstab')
        self.assertEqual(ret, 0, "Fail: Brick mount entry is not"
                         " found in /etc/fstab of {}".format(self.umount_host))

        # Unmount the selected brick
        cmd = 'umount {}'.format(self.umount_brick)
        ret, _, _ = g.run(self.umount_host, cmd)
        self.assertEqual(0, ret, "Fail: Not able to unmount {} on "
                         "{}".format(self.umount_brick, self.umount_host))

        # Run 'gluster get-state' and verify absence of any error
        ret = get_gluster_state(self.mnode)
        self.assertIsNotNone(ret, "Fail: 'gluster get-state' didn't dump the "
                             "state of glusterd when {} unmounted from "
                             "{}".format(self.umount_brick, self.umount_host))

        # Create another volume
        self.volume['name'] = 'second_volume'
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, 'Failed to create and start volume')
        g.log.info('Second volume created and started successfully')

        # Run 'gluster get-state' and verify absence of any error after
        # creation of second-volume
        ret = get_gluster_state(self.mnode)
        self.assertIsNotNone(ret, "Fail: 'gluster get-state' didn't dump the "
                             "state of glusterd ")
