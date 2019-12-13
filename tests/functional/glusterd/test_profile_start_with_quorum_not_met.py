#  Copyright (C) 2017-2019  Red Hat, Inc. <http://www.redhat.com>
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

from random import choice
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.gluster_init import start_glusterd, stop_glusterd
from glustolibs.gluster.profile_ops import profile_start, profile_stop


@runs_on([['distributed', 'replicated', 'dispersed',
           'distributed-replicated', 'distributed-dispersed'], ['glusterfs']])
class TestProfileStartWithQuorumNotMet(GlusterBaseClass):

    def setUp(self):

        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % self.volname)
        g.log.info("Volme created successfully : %s", self.volname)

    def tearDown(self):

        # Setting Quorum ratio to 51%
        self.quorum_perecent = {'cluster.server-quorum-ratio': '51%'}
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        if not ret:
            raise ExecutionError("gluster volume set all cluster."
                                 "server-quorum-ratio percentage "
                                 "Failed :%s" % self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio 51 "
                   "percentage enabled successfully on :%s", self.servers)

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_profile_start_with_quorum_not_met(self):
        # pylint: disable=too-many-statements
        """
        1. Create a volume
        2. Set the quorum type to server and ratio to 90
        3. Stop glusterd randomly on one of the node
        4. Start profile on the volume
        5. Start glusterd on the node where it is stopped
        6. Start profile on the volume
        7. Stop profile on the volume where it is started
        """

        # Enabling server quorum
        self.quorum_options = {'cluster.server-quorum-type': 'server'}
        ret = set_volume_options(self.mnode, self.volname, self.quorum_options)
        self.assertTrue(ret, "gluster volume set %s cluster.server-quorum-type"
                             " server Failed" % self.volname)
        g.log.info("gluster volume set %s cluster.server-quorum-type server "
                   "enabled successfully", self.volname)

        # Setting Quorum ratio to 90%
        self.quorum_perecent = {'cluster.server-quorum-ratio': '90%'}
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertTrue(ret, "gluster volume set all cluster.server-quorum-rat"
                             "io percentage Failed :%s" % self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio 90 "
                   "percentage enabled successfully on :%s", self.servers)

        # Stop glusterd on one of the node randomly
        self.node_on_glusterd_to_stop = choice(self.servers)
        ret = stop_glusterd(self.node_on_glusterd_to_stop)
        self.assertTrue(ret, "glusterd stop on the node failed")
        g.log.info("glusterd stop on the node: % "
                   "succeeded", self.node_on_glusterd_to_stop)

        # checking whether peers are connected or not
        count = 0
        while count < 5:
            ret = self.validate_peers_are_connected()
            if not ret:
                break
            sleep(2)
            count += 1
        self.assertFalse(ret, "Peers are in connected state even after "
                              "stopping glusterd on one node")

        # Starting volume profile when quorum is not met
        self.new_servers = self.servers[:]
        self.new_servers.remove(self.node_on_glusterd_to_stop)
        ret, _, _ = profile_start(choice(self.new_servers),
                                  self.volname)
        self.assertNotEqual(ret, 0, "Expected: Should not be able to start "
                                    "volume profile. Acutal: Able to start "
                                    "the volume profile start")
        g.log.info("gluster vol profile start is failed as expected")

        # Start glusterd on the node where it is stopped
        ret = start_glusterd(self.node_on_glusterd_to_stop)
        self.assertTrue(ret, "glusterd start on the node failed")
        g.log.info("glusterd start succeeded")

        # checking whether peers are connected or not
        count = 0
        while count < 5:
            ret = self.validate_peers_are_connected()
            if ret:
                break
            sleep(5)
            count += 1
        self.assertTrue(ret, "Peer are not in connected state ")

        # Starting profile when volume quorum is met
        ret, _, _ = profile_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Expected: Should be able to start the"
                                 "volume profile start. Acutal: Not able"
                                 " to start the volume profile start")
        g.log.info("gluster vol profile start is successful")

        # Stop the profile
        ret, _, _ = profile_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Expected: Should be able to stop the "
                                 "profile stop. Acutal: Not able to stop"
                                 " the profile stop")
        g.log.info("gluster volume profile stop is successful")
