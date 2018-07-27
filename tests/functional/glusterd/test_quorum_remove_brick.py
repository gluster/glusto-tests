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
      Test remove-brick operation when quorum not met
"""

from time import sleep
import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.gluster_init import (stop_glusterd, start_glusterd,
                                             is_glusterd_running)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.volume_libs import form_bricks_list_to_remove_brick


@runs_on([['distributed', 'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestServerQuorumNotMet(GlusterBaseClass):
    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Creating Volume
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """
        ret = is_glusterd_running(self.random_server)
        if ret:
            ret = start_glusterd(self.random_server)
            if not ret:
                raise ExecutionError("Failed to start glusterd on %s"
                                     % self.random_server)
        g.log.info("Glusterd started successfully on %s", self.random_server)

        # checking for peer status from every node
        count = 0
        while count < 80:
            ret = self.validate_peers_are_connected()
            if ret:
                break
            sleep(2)
            count += 1

        if not ret:
            raise ExecutionError("Servers are not in peer probed state")
        g.log.info("All peers are in connected state")

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s"
                                 % self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_quorum_remove_brick(self):
        '''
        -> Create volume
        -> Enabling server quorum
        -> Set server quorum ratio to 95%
        -> Stop the glusterd on any one of the node
        -> Perform remove brick operation
        -> start glusterd
        -> Check gluster vol info, bricks should be same before and after
        performing remove brick operation.
        '''
        # Enabling server quorum
        ret = set_volume_options(self.mnode, self.volname,
                                 {'cluster.server-quorum-type': 'server'})
        self.assertTrue(ret, "Failed to set server quorum for volume %s"
                        % self.volname)
        g.log.info("Able to set server quorum successfully for %s",
                   self.volname)

        # Setting server quorum ratio in percentage
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.server-quorum-ratio': '95%'})
        self.assertTrue(ret, "Failed to set server quorum ratio for %s"
                        % self.servers)
        g.log.info("Able to set server quorum ratio successfully for %s",
                   self.servers)

        # Getting brick list from volume
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Failed to get brick list of %s"
                             % self.volname)
        g.log.info("Successful in getting brick list of %s", self.volname)

        # Stopping glusterd
        self.random_server = random.choice(self.servers[1:])
        ret = stop_glusterd(self.random_server)
        self.assertTrue(ret, "Failed to stop glusterd on %s"
                        % self.random_server)
        g.log.info("Glusterd stopped successfully on %s", self.random_server)

        # Forming brick list for performing remove brick operation
        remove_brick_list = form_bricks_list_to_remove_brick(self.mnode,
                                                             self.volname)
        self.assertIsNotNone(remove_brick_list, "Failed to get brick list for "
                                                "performing remove brick "
                                                "operation")
        g.log.info("Successful in getting brick list for performing remove "
                   "brick operation")

        # Performing remove brick operation
        ret, _, err = remove_brick(self.mnode, self.volname,
                                   remove_brick_list, 'force')
        self.assertNotEqual(ret, 0, "Remove brick should fail when quorum is "
                                    "in not met condition, but brick removed "
                                    "successfully for %s" % self.volname)
        g.log.info("Failed to remove brick when quorum is in not met condition"
                   " as expected for %s", self.volname)

        # Expected error message for remove brick operation
        msg = ("volume remove-brick commit force: failed: "
               "Quorum not met. Volume operation not allowed")

        # Checking error message for remove brick operation
        self.assertIn(msg, err, "Error message is not correct for "
                                "remove brick operation when quorum not met")
        g.log.info("Error message is correct for remove brick operation when "
                   "quorum not met")

        # Starting glusterd
        ret = start_glusterd(self.random_server)
        self.assertTrue(ret, "Failed to start glusterd on %s"
                        % self.random_server)
        g.log.info("Glusted started successfully on %s", self.random_server)

        # Checking glusterd status
        count = 0
        while count < 60:
            ret = is_glusterd_running(self.random_server)
            if not ret:
                break
            sleep(2)
            count += 1
        self.assertEqual(ret, 0, "Glusterd is not running on %s"
                         % self.random_server)
        g.log.info("Glusterd is running on %s", self.random_server)

        # Getting brick list of volume after performing remove brick operation
        new_brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(new_brick_list, "Failed to get brick list of %s"
                             % self.volname)
        g.log.info("Successful in getting brick list of %s", self.volname)

        # Comparing bricks info before and after performing
        # remove brick operation
        self.assertListEqual(brick_list, new_brick_list,
                             "Bricks are not same before and after performing"
                             " remove brick operation")
        g.log.info("Bricks are same before and after "
                   "performing remove brick operation")
