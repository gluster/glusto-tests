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

""" Description:
      Volume start when one of the brick is absent
"""

import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_start, volume_status)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.volume_libs import cleanup_volume


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'], ['glusterfs']])
class TestVolumeStatusWithAbsentBricks(GlusterBaseClass):
    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume(False, True)
        if ret:
            g.log.info("Volme created successfully : %s", self.volname)
        else:
            raise ExecutionError("Volume creation failed: %s" % self.volname)

    def tearDown(self):
        # Stopping the volume and Cleaning up the volume
        ret = cleanup_volume(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_volume_absent_bricks(self):
        """
        Test Case:
        1) Create Volume
        2) Remove any one Brick directory
        3) Start Volume and compare the failure message
        4) Check the gluster volume status nad compare the status message
        """
        # Fetching the brick list
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Failed to get the bricks in"
                             " the volume")

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
