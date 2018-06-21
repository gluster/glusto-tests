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
      Test peer probe while snapd is running.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.snap_ops import snap_create
from glustolibs.gluster.uss_ops import enable_uss, is_snapd_running
from glustolibs.gluster.peer_ops import (peer_probe_servers, peer_detach,
                                         nodes_from_pool_list)


@runs_on([['distributed', 'replicated'], ['glusterfs']])
class TestPeerProbeWhileSnapdRunning(GlusterBaseClass):
    def tearDown(self):
        """
        tearDown for every test
        """
        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)

        # Checking peers are in connected state or not
        ret = self.validate_peers_are_connected()
        if not ret:
            # Peer probe detached servers
            pool = nodes_from_pool_list(self.mnode)
            for node in pool:
                peer_detach(self.mnode, node)
            ret = peer_probe_servers(self.mnode, self.servers)
            if not ret:
                raise ExecutionError("Failed to probe detached servers %s"
                                     % self.servers)
        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_peer_probe_snapd_running(self):

        '''
        -> Create Volume
        -> Create snap for that volume
        -> Enable uss
        -> Check snapd running or not
        -> Probe a new node while snapd is running
        '''

        # Performing node detach, Here detached node considering as extra
        # server
        extra_node = self.servers[-1]
        ret, _, _ = peer_detach(self.mnode, extra_node)
        self.assertEqual(ret, 0, "Peer detach failed for %s"
                         % extra_node)
        g.log.info("Peer detach success for %s", extra_node)

        # Removing detached node from 'self.servers' list, it's because of
        # 'self.setup_volume' function checking peer status of 'self.servers'
        # list before creating volume
        self.servers.remove(extra_node)

        # Creating volume
        ret = self.setup_volume()
        self.assertTrue(ret, "Failed Create volume %s" % self.volname)
        g.log.info("Volume created successfully %s", self.volname)

        # Adding node back into self.servers list
        self.servers.append(extra_node)

        # creating Snap
        ret, _, _ = snap_create(self.mnode, self.volname, 'snap1')
        self.assertEqual(ret, 0, "Snap creation failed for volume %s"
                         % self.volname)
        g.log.info("Snap created successfully for volume %s", self.volname)

        # Enabling Snapd(USS)
        ret, _, _ = enable_uss(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable USS for volume %s"
                         % self.volname)
        g.log.info("USS Enabled successfully on volume %s", self.volname)

        # Checking snapd running or not
        ret = is_snapd_running(self.mnode, self.volname)
        self.assertTrue(ret, "Snapd not runnig for volume %s" % self.volname)
        g.log.info("snapd running for volume %s", self.volname)

        # Probing new node
        ret = peer_probe_servers(self.mnode, extra_node)
        self.assertTrue(ret, "Peer Probe failed for new server %s"
                        % extra_node)
        g.log.info("Peer Probe success for new server %s", extra_node)
