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
      Test peer probe after setting global options to
      the volume, peer probe has to be successful.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.peer_ops import peer_probe_servers, peer_detach


@runs_on([['distributed', 'replicated'], ['glusterfs']])
class TestPeerProbeAfterSettingGlobalOptions(GlusterBaseClass):

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
        if self.detach_peer:
            ret = peer_probe_servers(self.mnode, self.servers[5])
            if not ret:
                raise ExecutionError("Peer probe failed for detached "
                                     "server %s" % self.servers[5])

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_peer_probe_global_options(self):
        '''
        -> Set global options and other volume specific options on the volume
        -> gluster volume set VOL nfs.rpc-auth-allow 1.1.1.1
        -> gluster volume set VOL nfs.addr-namelookup on
        -> gluster volume set VOL cluster.server-quorum-type server
        -> gluster volume set VOL network.ping-timeout 20
        -> gluster volume set VOL nfs.port 2049
        -> gluster volume set VOL performance.nfs.write-behind on
        -> Peer probe for a new node
        '''

        # Performing peer detach, considering last node of cluster as a
        # extra server
        ret, _, _ = peer_detach(self.mnode, self.servers[5])
        self.assertEqual(ret, 0, "Peer detach failed for server %s"
                         % self.servers[5])
        g.log.info("Peer detach success for server %s", self.servers[5])
        self.detach_peer = True

        # Performing gluster volume set volname nfs.rpc-auth-allow
        ret = set_volume_options(self.mnode, self.volname,
                                 {'nfs.rpc-auth-allow': '1.1.1.1'})
        self.assertTrue(ret, "gluster volume set %s nfs.rpc-auth-allow"
                             " failed" % self.volname)
        g.log.info("gluster volume set %s nfs.rpc-auth-allow executed"
                   " successfully", self.volname)

        # Performing gluster volume set volname nfs.addr-namelookup
        ret = set_volume_options(self.mnode, self.volname,
                                 {'nfs.addr-namelookup': 'on'})
        self.assertTrue(ret, "gluster volume set %s nfs.addr-namelookup"
                             " failed" % self.volname)
        g.log.info("gluster volume set %s nfs.addr-namelookup executed"
                   " successfully", self.volname)

        # Performing gluster volume set volname cluster.server-quorum-type
        ret = set_volume_options(self.mnode, self.volname,
                                 {'cluster.server-quorum-type': 'server'})
        self.assertTrue(ret, "gluster volume set %s cluster.server-quorum-type"
                             " failed" % self.volname)
        g.log.info("gluster volume set %s cluster.server-quorum-type executed"
                   " successfully", self.volname)

        # Performing gluster volume set volname network.ping-timeout
        ret = set_volume_options(self.mnode, self.volname,
                                 {'network.ping-timeout': 20})
        self.assertTrue(ret, "gluster volume set %s network.ping-timeout"
                             " failed" % self.volname)
        g.log.info("gluster volume set %s network.ping-timeout executed"
                   " successfully", self.volname)

        # Performing gluster volume set volname  nfs.port
        ret = set_volume_options(self.mnode, self.volname,
                                 {'nfs.port': 2049})
        self.assertTrue(ret, "gluster volume set %s nfs.port"
                             " failed" % self.volname)
        g.log.info("gluster volume set %s nfs.port executed"
                   " successfully", self.volname)

        # Performing gluster volume set volname performance.nfs.write-behind
        ret = set_volume_options(self.mnode, self.volname,
                                 {'performance.nfs.write-behind': 'on'})
        self.assertTrue(ret, "gluster volume set %s "
                             "performance.nfs.write-behind failed"
                        % self.volname)
        g.log.info("gluster volume set %s performance.nfs.write-behind "
                   "executed successfully", self.volname)

        # Probing new node
        ret = peer_probe_servers(self.mnode, self.servers[5])
        self.assertTrue(ret, "Peer Probe failed for new server %s"
                        % self.servers[5])
        g.log.info("Peer Probe success for new server %s", self.servers[5])
        self.detach_peer = False
