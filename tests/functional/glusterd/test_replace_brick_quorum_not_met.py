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

# pylint: disable= too-many-statements
""" Description:
    Test replace brick when quorum not met
"""

import random
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.volume_ops import (set_volume_options, volume_start,
                                           volume_create, get_volume_status)
from glustolibs.gluster.gluster_init import (stop_glusterd,
                                             is_glusterd_running,
                                             start_glusterd)
from glustolibs.gluster.brick_libs import (are_bricks_offline,
                                           are_bricks_online, get_all_bricks)
from glustolibs.gluster.brick_ops import replace_brick


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestReplaceBrickWhenQuorumNotMet(GlusterBaseClass):

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

        # Setting Quorum ratio to 51%
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.server-quorum-ratio': '51%'})
        if not ret:
            raise ExecutionError("Failed to set server quorum ratio on %s"
                                 % self.servers)
        g.log.info("Able to set server quorum ratio successfully on %s",
                   self.servers)

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Removing brick directories
        if not self.replace_brick_failed:
            for brick in self.brick_list:
                node, brick_path = brick.split(r':')
                cmd = "rm -rf " + brick_path
                ret, _, _ = g.run(node, cmd)
                if ret:
                    raise ExecutionError("Failed to delete the brick "
                                         "dir's of deleted volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_replace_brick_quorum(self):

        '''
        -> Create volume
        -> Set quorum type
        -> Set quorum ratio to 95%
        -> Start the volume
        -> Stop the glusterd on one node
        -> Now quorum is in not met condition
        -> Check all bricks went to offline or not
        -> Perform replace brick operation
        -> Start glusterd on same node which is already stopped
        -> Check all bricks are in online or not
        -> Verify in vol info that old brick not replaced with new brick
        '''

        # Forming brick list, 6 bricks for creating volume, 7th brick for
        # performing replace brick operation
        self.brick_list = form_bricks_list(self.mnode, self.volname, 7,
                                           self.servers, self.all_servers_info)

        # Create Volume
        ret, _, _ = volume_create(self.mnode, self.volname,
                                  self.brick_list[0:6], replica_count=3)
        self.assertEqual(ret, 0, "Failed to create volume %s" % self.volname)
        g.log.info("Volume created successfully %s", self.volname)

        # Enabling server quorum
        ret = set_volume_options(self.mnode, self.volname,
                                 {'cluster.server-quorum-type': 'server'})
        self.assertTrue(ret, "Failed to set server quorum on volume %s"
                        % self.volname)
        g.log.info("Able to set server quorum successfully on volume %s",
                   self.volname)

        # Setting Quorum ratio in percentage
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.server-quorum-ratio': '95%'})
        self.assertTrue(ret, "Failed to set server quorum ratio on %s"
                        % self.servers)
        g.log.info("Able to set server quorum ratio successfully on %s",
                   self.servers)

        # Start the volume
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start volume %s" % self.volname)
        g.log.info("Volume started successfully %s", self.volname)

        # Stop glusterd on one of the node
        random_server = random.choice(self.servers[1:])
        ret = stop_glusterd(random_server)
        self.assertTrue(ret, "Failed to stop glusterd for %s"
                        % random_server)
        g.log.info("Glusterd stopped successfully on server %s",
                   random_server)

        # Checking whether glusterd is running or not
        ret = is_glusterd_running(random_server)
        self.assertEqual(ret, 1, "Glusterd is still running on the node %s "
                                 "where glusterd stopped"
                         % random_server)
        g.log.info("Glusterd is not running on the server %s",
                   random_server)

        # Verifying node count in volume status after glusterd stopped
        # on one of the server, Its not possible to check the brick status
        # immediately in volume status after glusterd stop
        count = 0
        while count < 100:
            vol_status = get_volume_status(self.mnode, self.volname)
            servers_count = len(vol_status[self.volname].keys())
            if servers_count == 5:
                break
            sleep(2)
            count += 1

        # creating brick list from volume status
        offline_bricks = []
        vol_status = get_volume_status(self.mnode, self.volname)
        for node in vol_status[self.volname]:
            for brick_path in vol_status[self.volname][node]:
                if brick_path != 'Self-heal Daemon':
                    offline_bricks.append(':'.join([node, brick_path]))

        # Checking bricks are offline or not with quorum ratio(95%)
        ret = are_bricks_offline(self.mnode, self.volname, offline_bricks)
        self.assertTrue(ret, "Bricks are online when quorum is in not met "
                             "condition for %s" % self.volname)
        g.log.info("Bricks are offline when quorum is in not met "
                   "condition for %s", self.volname)

        # Getting random brick from offline brick list
        self.random_brick = random.choice(offline_bricks)

        # Performing replace brick commit force when quorum not met
        self.replace_brick_failed = False
        ret, _, _ = replace_brick(self.mnode, self.volname, self.random_brick,
                                  self.brick_list[6])
        self.assertNotEqual(ret, 0, "Replace brick should fail when quorum is "
                                    "in not met condition but replace brick "
                                    "success on %s" % self.volname)
        g.log.info("Failed to replace brick when quorum is in not met "
                   "condition %s", self.volname)
        self.replace_brick_failed = True

        # Start glusterd on one of the node
        ret = start_glusterd(random_server)
        self.assertTrue(ret, "Failed to start glusterd on server %s"
                        % random_server)
        g.log.info("Glusterd started successfully on server %s",
                   random_server)

        # Verifying node count in volume status after glusterd started
        # on one of the servers, Its not possible to check the brick status
        # immediately in volume status after glusterd start
        count = 0
        while count < 100:
            vol_status = get_volume_status(self.mnode, self.volname)
            servers_count = len(vol_status[self.volname].keys())
            if servers_count == 6:
                break
            sleep(2)
            count += 1

        # Checking bricks are online or not
        count = 0
        while count < 100:
            ret = are_bricks_online(self.mnode, self.volname,
                                    self.brick_list[0:6])
            if ret:
                break
            sleep(2)
            count += 1
        self.assertTrue(ret, "All bricks are not online for %s"
                        % self.volname)
        g.log.info("All bricks are online for volume %s", self.volname)

        # Comparing brick lists of before and after performing replace brick
        # operation
        after_brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertListEqual(after_brick_list, self.brick_list[0:6],
                             "Bricks are not same before and after performing "
                             "replace brick operation for volume %s"
                             % self.volname)
        g.log.info("Bricks are same before and after performing replace "
                   "brick operation for volume %s", self.volname)
