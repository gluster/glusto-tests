#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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
Test Cases in this module related to Glusterd peer detach.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.peer_ops import peer_detach
from glustolibs.gluster.peer_ops import peer_probe_servers
from glustolibs.gluster.lib_utils import is_core_file_created


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class PeerDetachVerification(GlusterBaseClass):
    """
    Test that peer detach works as expected
    """
    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)

        # checking for peer status from every node
        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peer probe failed ")
        else:
            g.log.info("All server peers are already in connected state "
                       "%s:", cls.servers)

    @classmethod
    def tearDownClass(cls):
        # stopping the volume and Cleaning up the volume
        ret = cls.cleanup_volume()
        if ret:
            g.log.info("Volume deleted successfully : %s", cls.volname)
        else:
            raise ExecutionError("Failed Cleanup the Volume %s" % cls.volname)

    def test_peer_detach_host(self):
        # peer Detaching specified server from cluster
        # peer Detaching detached server again
        # peer Detaching invalid host
        # peer Detaching Non exist host
        # peer Checking Core file created or not
        # Peer detach one node which contains the bricks of volume created
        # Peer detach force a node which is hosting bricks of a volume

        # Timestamp of current test case of start time
        ret, test_timestamp, _ = g.run_local('date +%s')
        test_timestamp = test_timestamp.strip()

        # Assigning non existing host to variable
        self.non_exist_host = '256.256.256.256'

        # Assigning invalid ip to variable
        self.invalid_ip = '10.11.a'

        # Peer detach to specified server
        g.log.info("Start detach specified server :%s", self.servers[1])
        ret, _, _ = peer_detach(self.mnode, self.servers[1])
        self.assertEqual(ret, 0, "Failed to detach server :%s"
                         % self.servers[1])

        # Detached server detaching again, Expected to fail detach
        g.log.info("Start detached server detaching "
                   "again : %s", self.servers[1])
        ret, _, _ = peer_detach(self.mnode, self.servers[1])
        self.assertNotEqual(ret, 0, "Detach server should "
                                    "fail :%s" % self.servers[1])

        # Probing detached server
        g.log.info("Start probing detached server : %s", self.servers[1])
        ret = peer_probe_servers(self.mnode, self.servers[1])
        self.assertTrue(ret, "Peer probe failed from %s to other "
                        "server : %s" % (self.mnode, self.servers[1]))

        # Detach invalid host
        g.log.info("Start detaching invalid host :%s ", self.invalid_ip)
        ret, _, _ = peer_detach(self.mnode, self.invalid_ip)
        self.assertNotEqual(ret, 0, "Detach invalid host should "
                                    "fail :%s" % self.invalid_ip)

        # Detach non exist host
        g.log.info("Start detaching non exist host : %s", self.non_exist_host)
        ret, _, _ = peer_detach(self.mnode, self.non_exist_host)
        self.assertNotEqual(ret, 0, "Detach non existing host "
                                    "should fail :%s" % self.non_exist_host)

        # Chekcing core. file created or not in "/", "/tmp", "/log/var/core
        # directory
        ret = is_core_file_created(self.servers, test_timestamp)
        self.assertTrue(ret, "glusterd service should not crash")
        g.log.info("No core file found, glusterd service running "
                   "successfully")

        # Creating Volume
        g.log.info("Started creating volume: %s", self.volname)
        ret = self.setup_volume()
        self.assertTrue(ret, "Volume creation failed: %s" % self.volname)

        # Peer detach one node which contains the bricks of the volume created
        g.log.info("Start detaching server %s which is hosting "
                   "bricks of a volume", self.servers[1])
        ret, _, err = peer_detach(self.mnode, self.servers[1])
        self.assertNotEqual(ret, 0, "detach server should fail: %s"
                            % self.servers[1])
        msg = ('peer detach: failed: Brick(s) with the peer ' +
               self.servers[1] + ' ' + 'exist in cluster')
        if msg not in err:
            msg = ('peer detach: failed: Peer ' + self.servers[1] +
                   ' hosts one or more bricks. ' +
                   'If the peer is in not recoverable ' +
                   'state then use either ' +
                   'replace-brick or remove-brick command ' +
                   'with force to remove ' +
                   'all bricks from the peer and ' +
                   'attempt the peer detach again.')
        self.assertIn(msg, err, "Peer detach not failed with "
                                "proper error message")

        #  Peer detach force a node which is hosting bricks of a volume
        g.log.info("start detaching server %s with force option "
                   "which is hosting bricks of a volume", self.servers[1])
        ret, _, err = peer_detach(self.mnode, self.servers[1], force=True)
        self.assertNotEqual(ret, 0, "detach server should fail with force "
                                    "option : %s" % self.servers[1])
        msg = ('peer detach: failed: Brick(s) with the peer ' +
               self.servers[1] + ' ' + 'exist in cluster')
        if msg not in err:
            msg = ('peer detach: failed: Peer ' + self.servers[1] +
                   ' hosts one or more bricks. ' +
                   'If the peer is in not recoverable ' +
                   'state then use either ' +
                   'replace-brick or remove-brick command ' +
                   'with force to remove ' +
                   'all bricks from the peer and ' +
                   'attempt the peer detach again.')
        self.assertIn(msg, err, "Peer detach not failed with "
                                "proper error message")
