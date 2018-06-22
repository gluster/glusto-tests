#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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

""" Description:
      Volume start when one of the brick is absent
"""

import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           volume_status)
from glustolibs.gluster.lib_utils import form_bricks_list


@runs_on([['distributed', 'replicated', 'distributed-replicated'],
          ['glusterfs']])
class TestVolumeStatusWithAbsentBricks(GlusterBaseClass):

    def tearDown(self):
        """
        tearDown for every test
        """
        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s"
                                 % self.volname)
        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_volume_absent_bricks(self):
        '''
        -> Create Volume
        -> Remove any one Brick directory
        -> Start Volume
        -> Check the gluster volume status
        '''
        num_of_bricks = 0
        replica = True

        if self.volume_type == 'distributed':
            num_of_bricks = 3
            replica = False

        elif self.volume_type == 'replicated':
            num_of_bricks = 3

        elif self.volume_type == 'distributed-replicated':
            num_of_bricks = 6

        # Forming brick list
        brick_list = form_bricks_list(self.mnode, self.volname, num_of_bricks,
                                      self.servers, self.all_servers_info)
        if replica:
            # Creating Volume
            ret, _, _ = volume_create(self.mnode, self.volname, brick_list,
                                      replica_count=3)
            self.assertEqual(ret, 0, "Volume creation failed for %s"
                             % self.volname)
            g.log.info("volume created successfully %s", self.volname)
        else:
            # Creating Volume
            ret, _, _ = volume_create(self.mnode, self.volname, brick_list)
            self.assertEqual(ret, 0, "Volume creation failed for %s"
                             % self.volname)
            g.log.info("volume created successfully %s", self.volname)

        # Command for removing brick directory
        random_brick = random.choice(brick_list)
        node, brick_path = random_brick.split(r':')
        cmd = 'rm -rf ' + brick_path

        # Removing brick directory of one node
        ret, _, _ = g.run(node, cmd)
        self.assertEqual(ret, 0, "Failed to remove brick dir")
        g.log.info("Brick directory removed successfully")

        # Starting volume
        ret, _, err = volume_start(self.mnode, self.volname)
        self.assertNotEqual(ret, 0, "Unexpected: Volume started successfully "
                                    "even though brick directory is removed "
                                    "for %s" % self.volname)
        g.log.info("Expected: Failed to start volume %s", self.volname)

        # Checking volume start failed message
        msg = "Failed to find brick directory"
        self.assertIn(msg, err, "Expected message is %s but volume start "
                                "command failed with this "
                                "message %s" % (msg, err))
        g.log.info("Volume start failed with correct error message %s", err)

        # Checking Volume status
        ret, _, err = volume_status(self.mnode, self.volname)
        self.assertNotEqual(ret, 0, "Success in getting volume status, volume "
                                    "status should fail when volume is in "
                                    "not started state ")
        g.log.info("Failed to get volume status which is expected")

        # Checking volume status message
        msg = ' '.join(['Volume', self.volname, 'is not started'])
        self.assertIn(msg, err, 'Incorrect error message for gluster vol '
                                'status')
        g.log.info("Correct error message for volume status")
