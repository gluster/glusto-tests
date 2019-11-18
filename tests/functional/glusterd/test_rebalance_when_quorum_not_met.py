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
      Test rebalance operation when quorum not met
"""
from time import sleep
import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.gluster_init import (stop_glusterd, start_glusterd,
                                             is_glusterd_running)
from glustolibs.gluster.rebalance_ops import rebalance_start
from glustolibs.gluster.volume_ops import (volume_status,
                                           volume_stop, volume_start)


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

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s"
                                 % self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_rebalance_quorum(self):
        '''
        -> Create volume
        -> Stop the volume
        -> Enabling serve quorum
        -> start the volume
        -> Set server quorum ratio to 95%
        -> Stop the glusterd of any one of the node
        -> Perform rebalance operation operation
        -> Check gluster volume status
        -> start glusterd
        '''
        # Stop the Volume
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to stop the volume %s" % self.volname)
        g.log.info("Volume stopped successfully %s", self.volname)

        # Enabling server quorum
        ret = set_volume_options(self.mnode, self.volname,
                                 {'cluster.server-quorum-type': 'server'})
        self.assertTrue(ret, "Failed to set quorum type for volume %s"
                        % self.volname)
        g.log.info("Able to set quorum type successfully for %s", self.volname)

        # Start the volume
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start the volume %s"
                         % self.volname)
        g.log.info("Volume started successfully %s", self.volname)

        # Setting Quorum ratio in percentage
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.server-quorum-ratio': '95%'})
        self.assertTrue(ret, "Failed to set server quorum ratio on %s"
                        % self.servers)
        g.log.info("Able to set server quorum ratio successfully on %s",
                   self.servers)

        # Stopping glusterd
        self.random_server = random.choice(self.servers[1:])
        ret = stop_glusterd(self.random_server)
        self.assertTrue(ret, "Failed to stop glusterd on %s"
                        % self.random_server)
        g.log.info("Glusterd stopped successfully on %s", self.random_server)

        msg = ("volume rebalance: " + self.volname + ": failed: Quorum not "
                                                     "met. Volume operation "
                                                     "not allowed")

        # Start Rebalance
        ret, _, err = rebalance_start(self.mnode, self.volname)
        self.assertNotEqual(ret, 0, "Unexpected: Rebalance should fail when "
                                    "quorum is in not met condition but "
                                    "Rebalance succeeded %s" % self.volname)
        g.log.info("Expected: Rebalance failed when quorum is in not met "
                   "condition %s", self.volname)

        # Checking Rebalance failed message
        self.assertIn(msg, err, "Error message is not correct for rebalance "
                                "operation when quorum not met")
        g.log.info("Error message is correct for rebalance operation "
                   "when quorum not met")

        # Volume Status
        ret, out, _ = volume_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to get volume status for %s"
                         % self.volname)
        g.log.info("Successful in getting volume status for %s", self.volname)

        # Checking volume status message
        self.assertNotIn('rebalance', out, "Unexpected: Found rebalance task "
                                           "in vol status of %s"
                         % self.volname)
        g.log.info("Expected: Not Found rebalance task in vol status of %s",
                   self.volname)

        # Starting glusterd
        ret = start_glusterd(self.random_server)
        self.assertTrue(ret, "Failed to start glusterd on %s"
                        % self.random_server)
        g.log.info("Glusted started successfully on %s", self.random_server)
