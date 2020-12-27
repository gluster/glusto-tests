#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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

from time import sleep

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.io.utils import collect_mounts_arequal
from glustolibs.gluster.peer_ops import (peer_probe_servers, peer_detach)


@runs_on([['distributed'], ['glusterfs']])
class TestRebalancePeerProbe(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Setup and mount volume")

        self.first_client = self.mounts[0].client_system
        self.is_peer_detached = False

    def tearDown(self):

        # Unmount and clean volume
        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Cleanup Volume")

        # Probe detached node in case it's still detached
        if self.is_peer_detached:
            if not peer_probe_servers(self.mnode, self.servers[5]):
                raise ExecutionError("Failed to probe detached "
                                     "servers %s" % self.servers)
        g.log.info("Peer probe success for detached "
                   "servers %s", self.servers)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_rebalance_peer_probe(self):
        """
        Test case:
        1. Detach a peer
        2. Create a volume, start it and mount it
        3. Start creating a few files on mount point
        4. Collect arequal checksum on mount point pre-rebalance
        5. Expand the volume
        6. Start rebalance
        7. While rebalance is going, probe a peer and check if
           the peer was probed successfully
        7. Collect arequal checksum on mount point post-rebalance
           and compare wth value from step 4
        """

        # Detach a peer
        ret, _, _ = peer_detach(self.mnode, self.servers[5])
        self.assertEqual(ret, 0, "Failed to detach peer %s"
                         % self.servers[5])

        self.is_peer_detached = True

        # Start I/O from mount point and wait for it to complete
        cmd = ("cd %s; for i in {1..1000} ; do "
               "dd if=/dev/urandom of=file$i bs=10M count=1; done"
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertEqual(ret, 0, "IO failed on volume %s"
                         % self.volname)

        # Collect arequal checksum before rebalance
        arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        # Add brick to volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)

        # Trigger rebalance and wait for it to complete
        ret, _, _ = rebalance_start(self.mnode, self.volname,
                                    force=True)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Let rebalance run for a while
        sleep(5)

        # Add new node to the cluster
        ret = peer_probe_servers(self.mnode, self.servers[5])
        self.assertTrue(ret, "Failed to peer probe server : %s"
                        % self.servers[5])
        g.log.info("Peer probe success for %s and all peers are in "
                   "connected state", self.servers[5])

        self.is_peer_detached = False

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1200)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

        # Collect arequal checksum after rebalance
        arequal_checksum_after = collect_mounts_arequal(self.mounts[0])

        # Check for data loss by comparing arequal before and after rebalance
        self.assertEqual(arequal_checksum_before, arequal_checksum_after,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")
