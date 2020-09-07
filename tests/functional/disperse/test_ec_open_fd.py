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
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Test Description:
    Tests open FD heal for EC volume
"""

import os
from random import choice
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.brick_libs import (bring_bricks_online,
                                           bring_bricks_offline,
                                           validate_xattr_on_all_bricks)
from glustolibs.gluster.heal_ops import disable_heal
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import (get_subvols,
                                            log_volume_info_and_status)
from glustolibs.gluster.glusterfile import check_if_pattern_in_file
from glustolibs.io.utils import open_file_fd


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestEcOpenFd(GlusterBaseClass):

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

    def test_ec_open_fd(self):
        """
        Test Steps:
        - disable server side heal
        - Create a file
        - Set volume option to implement open FD on file
        - Bring a brick down,say b1
        - Open FD on file
        - Bring brick b1 up
        - write to open FD file
        - Monitor heal
        - Check xattr , ec.version and ec.size of file
        - Check stat of file
        """

        # pylint: disable=too-many-branches,too-many-statements,too-many-locals

        mountpoint = self.mounts[0].mountpoint

        # Disable server side heal
        ret = disable_heal(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to disable server side heal"))
        g.log.info("Successfully disabled server side heal")

        # Log Volume Info and Status after disabling server side heal
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed "
                              "on volume %s", self.volname))

        # Create a file
        cmd = ("cd %s; touch 'file_openfd';" % mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Finished creating a file while all the bricks are UP')

        # Set volume options
        ret = set_volume_options(self.mnode, self.volname,
                                 {"performance.read-after-open": "yes"})
        self.assertTrue(ret, 'Failed to set volume {}'
                        ' options'.format(self.volname))
        g.log.info('Successfully set %s volume options', self.volname,)

        # Bringing brick b1 offline
        sub_vols = get_subvols(self.mnode, self.volname)
        subvols_list = sub_vols['volume_subvols']
        bricks_list1 = subvols_list[0]
        brick_b1_down = choice(bricks_list1)
        ret = bring_bricks_offline(self.volname,
                                   brick_b1_down)
        self.assertTrue(ret, 'Brick %s is not offline' % brick_b1_down)
        g.log.info('Brick %s is offline successfully', brick_b1_down)

        node = self.mounts[0].client_system
        # Open FD
        proc = open_file_fd(mountpoint, time=100,
                            client=node)

        # Bring brick b1 online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [brick_b1_down],
                                  'glusterd_restart')
        self.assertTrue(ret, 'Brick {} is not brought '
                        'online'.format(brick_b1_down))
        g.log.info('Brick %s is online successfully', brick_b1_down)

        # Validate peers are connected
        ret = self.validate_peers_are_connected()
        self.assertTrue(ret, "Peers are not in connected state after bringing"
                        " an offline brick to online via `glusterd restart`")
        g.log.info("Successfully validated peers are in connected state")

        # Check if write to FD is successful
        g.log.info('Open FD on file successful')
        ret, _, _ = proc.async_communicate()
        self.assertEqual(ret, 0, "Write to FD is successful")

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')
        g.log.info('Heal has completed successfully')

        file_openfd = os.path.join(mountpoint, 'file_openfd')

        # Check if data exists on file
        ret = check_if_pattern_in_file(node, 'xyz', file_openfd)
        self.assertEqual(ret, 0, 'xyz does not exists in file')
        g.log.info('xyz exists in file')

        file_fd = 'file_openfd'

        # Check if EC version is same on all bricks which are up
        ret = validate_xattr_on_all_bricks(bricks_list1, file_fd,
                                           'trusted.ec.version')
        self.assertTrue(ret, "Healing not completed and EC version is "
                        "not updated")
        g.log.info("Healing is completed and EC version is updated")

        # Check if EC size is same on all bricks which are up
        ret = validate_xattr_on_all_bricks(bricks_list1, file_fd,
                                           'trusted.ec.size')
        self.assertTrue(ret, "Healing not completed and EC size is "
                        "not updated")
        g.log.info("Healing is completed and EC size is updated")

        # Check stat of file
        cmd = "cd %s; du -kh file_openfd" % mountpoint
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, err)
        g.log.info('File %s is accessible', file_fd)
