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
        No Errors should generate in glusterd.log while detaching
        node from gluster
"""

import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.peer_ops import (peer_detach_servers,
                                         peer_probe_servers)


class GlusterdLogsWhilePeerDetach(GlusterBaseClass):

    def tearDown(self):
        """
        tearDown for every test
        """
        # Peer probe detached server
        ret = peer_probe_servers(self.mnode, self.random_server)
        if not ret:
            raise ExecutionError(ret, "Failed to probe detached server")
        g.log.info("peer probe is successful for %s", self.random_server)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_logs_while_peer_detach(self):
        '''
        -> Detach the node from peer
        -> Check that any error messages related to peer detach
        in glusterd log file
        -> No errors should be there in glusterd log file
        '''

        # Getting timestamp
        _, timestamp, _ = g.run_local('date +%s')
        timestamp = timestamp.strip()

        #  glusterd logs
        ret, _, _ = g.run(self.mnode,
                          'cp /var/log/glusterfs/glusterd.log '
                          '/var/log/glusterfs/glusterd_%s.log' % timestamp)
        if ret:
            raise ExecutionError("Failed to copy glusterd logs")

        # Clearing the existing glusterd log file
        ret, _, _ = g.run(self.mnode, 'echo > /var/log/glusterfs/glusterd.log')
        if ret:
            raise ExecutionError("Failed to clear glusterd.log file on %s"
                                 % self.mnode)

        # Performing peer detach
        self.random_server = random.choice(self.servers[1:])
        ret = peer_detach_servers(self.mnode, self.random_server)
        self.assertTrue(ret, "Failed to detach peer %s"
                        % self.random_server)
        g.log.info("Peer detach successful for %s", self.random_server)

        # Searching for error message in log
        ret, out, _ = g.run(
            self.mnode,
            "grep ' E ' /var/log/glusterfs/glusterd.log | wc -l")
        self.assertEqual(ret, 0, "Failed to get error message count in "
                                 "glusterd log file")
        g.log.info("Successful getting error message count in log file")

        self.assertEqual(int(out), 0, "Found Error messages in glusterd log "
                                      "file after peer detach")
        g.log.info("No error messages found in gluterd log file after peer "
                   "detach")
