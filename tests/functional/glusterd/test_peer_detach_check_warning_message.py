#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.peer_ops import (peer_detach, peer_probe,
                                         is_peer_connected)


class TestPeerDetachWarningMessage(GlusterBaseClass):

    def tearDown(self):

        # Peer probe node which was detached
        ret, _, _ = peer_probe(self.mnode, self.servers[1])
        if ret:
            raise ExecutionError("Failed to detach %s" % self.servers[1])
        g.log.info("Peer detach successful %s", self.servers[1])

        self.get_super_method(self, 'tearDown')()

    def test_peer_detach_check_warning_message(self):
        # pylint: disable=too-many-statements
        """
        Test Case:
        1) Create a cluster.
        2) Peer detach a node but don't press y.
        3) Check the warning message.
        4) Check peer status.
           (Node shouldn't be detached!)
        5) Peer detach a node now press y.
        6) Check peer status.
           (Node should be detached!)
        """

        # Peer detach one node
        ret, msg, _ = g.run(self.mnode, "gluster peer detach %s"
                            % self.servers[1])
        self.assertEqual(ret, 0, "ERROR: Peer detach successful %s"
                         % self.servers[1])
        g.log.info("EXPECTED: Failed to detach %s", self.servers[1])

        # Checking warning message
        expected_msg = ' '.join([
            'All clients mounted through the peer which is getting',
            'detached need to be remounted using one of the other',
            'active peers in the trusted storage pool to ensure',
            'client gets notification on any changes done on the',
            'gluster configuration and if the same has been done',
            'do you want to proceed'
            ])
        self.assertIn(expected_msg, msg.split('?')[0],
                      "Incorrect warning message for peer detach.")
        g.log.info("Correct warning message for peer detach.")

        # Checking if peer is connected
        ret = is_peer_connected(self.mnode, self.servers[1])
        self.assertTrue(ret, "Peer is not in connected state.")
        g.log.info("Peers is in connected state.")

        # Peer detach one node
        ret, _, _ = peer_detach(self.mnode, self.servers[1])
        self.assertEqual(ret, 0, "Failed to detach %s" % self.servers[1])
        g.log.info("Peer detach successful %s", self.servers[1])

        # Checking if peer is connected
        ret = is_peer_connected(self.mnode, self.servers[1])
        self.assertFalse(ret, "Peer is in connected state.")
        g.log.info("Peer is not in connected state.")
