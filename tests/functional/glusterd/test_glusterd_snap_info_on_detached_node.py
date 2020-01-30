#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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
import random
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfile import file_exists
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.volume_ops import (volume_create,
                                           set_volume_options, volume_start)
from glustolibs.gluster.snap_ops import snap_create, snap_activate
from glustolibs.gluster.peer_ops import (
    peer_detach_servers,
    peer_probe_servers)


@runs_on([['distributed'], ['glusterfs']])
class TestSnapInfoOnPeerDetachedNode(GlusterBaseClass):

    def tearDown(self):

        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to peer probe servers")

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_snap_info_from_detached_node(self):
        # pylint: disable=too-many-statements
        """
        Create a volume with single brick
        Create a snapshot
        Activate the snapshot created
        Enabled uss on the volume
        Validated snap info on all the nodes
        Peer detach one node
        Validate /var/lib/glusterd/snaps on the detached node
        Probe the detached node
        """

        # Creating volume with single brick on one node
        servers_info_single_node = {self.servers[0]:
                                    self.all_servers_info[self.servers[0]]}
        bricks_list = form_bricks_list(self.mnode, self.volname,
                                       1, self.servers[0],
                                       servers_info_single_node)
        ret, _, _ = volume_create(self.servers[0], self.volname, bricks_list)
        self.assertEqual(ret, 0, "Volume creation failed")
        g.log.info("Volume %s created successfully", self.volname)

        # Create a snapshot of the volume without volume start should fail
        self.snapname = "snap1"
        ret, _, _ = snap_create(
            self.mnode, self.volname, self.snapname, timestamp=False)
        self.assertNotEqual(
            ret, 0, "Snapshot created without starting the volume")
        g.log.info("Snapshot creation failed as expected")

        # Start the volume
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(
            ret, 0, "Failed to start the volume %s" % self.volname)
        g.log.info("Volume start succeeded")

        # Create a snapshot of the volume after volume start
        ret, _, _ = snap_create(
            self.mnode, self.volname, self.snapname, timestamp=False)
        self.assertEqual(
            ret, 0, "Snapshot creation failed on the volume %s" % self.volname)
        g.log.info("Snapshot create succeeded")

        # Activate snapshot created
        ret, _, err = snap_activate(self.mnode, self.snapname)
        self.assertEqual(
            ret, 0, "Snapshot activate failed with following error %s" % (err))
        g.log.info("Snapshot activated successfully")

        # Enable uss
        self.vol_options['features.uss'] = 'enable'
        ret = set_volume_options(self.mnode, self.volname, self.vol_options)
        self.assertTrue(ret, "gluster volume set %s features.uss "
                             "enable failed" % self.volname)
        g.log.info("gluster volume set %s features.uss "
                   "enable successfully", self.volname)

        # Validate files /var/lib/glusterd/snaps on all the servers is same
        self.pathname = "/var/lib/glusterd/snaps/%s" % self.snapname
        for server in self.servers:
            ret = file_exists(server, self.pathname)
            self.assertTrue(ret, "%s directory doesn't exist on node %s" %
                            (self.pathname, server))
            g.log.info("%s path exists on node %s", self.pathname, server)

        # Peer detach one node
        self.random_node_peer_detach = random.choice(self.servers[1:])
        ret = peer_detach_servers(self.mnode,
                                  self.random_node_peer_detach, validate=True)
        self.assertTrue(ret, "Peer detach of node: %s failed" %
                        self.random_node_peer_detach)
        g.log.info("Peer detach succeeded")

        # /var/lib/glusterd/snaps/<snapname> directory should not present

        ret = file_exists(self.random_node_peer_detach, self.pathname)
        self.assertFalse(ret, "%s directory should not exist on the peer"
                              "which is detached from cluster%s" % (
                                  self.pathname, self.random_node_peer_detach))
        g.log.info("Expected: %s path doesn't exist on peer detached node %s",
                   self.pathname, self.random_node_peer_detach)
