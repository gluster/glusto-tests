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
      Test brick status when quorum is not met
"""

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.gluster_init import (stop_glusterd, start_glusterd,
                                             is_glusterd_running)
from glustolibs.gluster.brick_libs import (are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.volume_ops import get_volume_status


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestBrickStatusWhenQuorumNotMet(GlusterBaseClass):

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
        g.log.info("Volme created successfully : %s", self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """
        ret = is_glusterd_running(self.servers)
        if ret:
            ret = start_glusterd(self.servers)
            if not ret:
                raise ExecutionError("Glusterd not started on some of "
                                     "the servers")
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
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_remove_brick_status(self):
        '''
        -> Create volume
        -> Enable server quorum on volume
        -> Stop glusterd on all nodes except first node
        -> Verify brick status of nodes where glusterd is running with
        default quorum ratio(51%)
        -> Change the cluster.server-quorum-ratio from default to 95%
        -> Start glusterd on all servers except last node
        -> Verify the brick status again
        '''

        # Enabling server quorum
        ret = set_volume_options(self.mnode, self.volname,
                                 {'cluster.server-quorum-type': 'server'})
        self.assertTrue(ret, "Failed to set server quorum on volume %s"
                        % self.volname)
        g.log.info("Able to set server quorum on volume %s successfully",
                   self.volname)

        # Getting brick list
        brick_list = get_all_bricks(self.mnode, self.volname)

        # Stopping glusterd on remaining servers except first node
        ret = stop_glusterd(self.servers[1:])
        self.assertTrue(ret, "Failed to stop gluterd on some of the servers "
                             "%s" % self.servers[1:])
        g.log.info("Glusterd stopped successfully on servers %s",
                   self.servers[1:])

        # Checking brick status for glusterd running nodes with
        # default quorum ratio(51%)
        ret = are_bricks_offline(self.mnode, self.volname, brick_list[0:1])
        self.assertTrue(ret, "Bricks are online when quorum is in not "
                             "met condition for %s" % self.volname)
        g.log.info("Bricks are offline when quorum is in not met "
                   "condition for %s", self.volname)

        # Setting quorum ratio to 95%
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.server-quorum-ratio': '95%'})
        self.assertTrue(ret, "Failed to set quorum ratio to 95 percentage on "
                             "servers %s" % self.servers)
        g.log.info("Able to set server quorum ratio to 95 percentage "
                   "on servers %s", self.servers)

        # Starting glusterd on remaining servers except last node
        ret = start_glusterd(self.servers[1:5])
        self.assertTrue(ret, "Failed to start glusterd on some of the servers"
                             " %s" % self.servers[1:5])
        g.log.info("Glusterd started successfully on all servers except "
                   "last node %s", self.servers[1:5])

        # Verfiying node count in volume status after glusterd
        # started on servers, Its not possible to check the brick status
        # immediately after glusterd start, thats why verifying that all
        # glusterd started nodes available in gluster volume status or not
        count = 0
        while count < 50:
            vol_status = get_volume_status(self.mnode, self.volname)
            servers_count = len(vol_status[self.volname].keys())
            if servers_count == 5:
                break
            sleep(2)
            count += 1

        # Checking brick status with quorum ratio(95%)
        ret = are_bricks_offline(self.mnode, self.volname, brick_list[0:5])
        self.assertTrue(ret, "Bricks are online when quorum is in not "
                             "met condition for %s" % self.volname)
        g.log.info("Bricks are offline when quorum is in not met "
                   "condition for %s", self.volname)
