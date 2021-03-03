#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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

from time import sleep
from datetime import datetime, timedelta
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_init import (restart_glusterd,
                                             wait_for_glusterd_to_start)


class TestGlusterdFriendUpdatesWhenPeerRejoins(GlusterBaseClass):

    # pylint: disable=too-few-public-methods
    def test_glusterd_friend_update_on_peer_rejoin(self):
        """
        Test Steps:
        1. Restart glusterd on one of the node
        2. Check friend updates happened between nodes where
           glusterd was running
        3. Check friend updates between rejoined node to each other node
        """
        # Restart glusterd on one of the node
        ret = restart_glusterd(self.servers[1])
        self.assertTrue(ret, "Failed to restart glusterd on server: %s"
                        % self.servers[1])

        ret = wait_for_glusterd_to_start(self.servers[1])
        self.assertTrue(ret, "Unexpected: Glusterd not yet started on:"
                        " server: %s" % self.servers[1])

        # Save the current UTC time
        # Reducing 1 second to adjust with the race conditions in logging
        curr_time = datetime.utcnow() - timedelta(seconds=1)
        curr_time = curr_time.strftime("%H:%M:%S")

        # Minimum cluster size
        min_clstr_sz = 2

        # Friend updates for a minimum cluster
        min_updt = 4

        # Current cluster size
        crnt_clstr_size = len(self.servers)

        # Wait until all the updates between the cluster nodes finish
        sleep(2 * crnt_clstr_size)

        # Intentional, to leverage the filtering of command line
        cmd = "gluster peer status | grep 'Uuid:' | cut -d ':' -f 2"
        ret, peer_lst, _ = g.run(self.servers[1], cmd)
        self.assertEqual(ret, 0, "Failed to execute the peer status command")
        peer_lst = peer_lst.splitlines()
        peer_lst = [p_uuid.strip() for p_uuid in peer_lst]

        # Check if there are any friend update between other nodes
        # and the restarted node
        for server in self.servers:
            # Don't check on the restarted node
            if server != self.servers[1]:
                for uuid in peer_lst:
                    cmd = ("awk '/%s/,0' /var/log/glusterfs/glusterd.log |"
                           " grep '_handle_friend_update' | grep %s | wc -l"
                           % (curr_time, uuid))
                    ret, out, _ = g.run(server, cmd)
                    self.assertEqual(ret, 0, "Failed to get count of friend"
                                     " updates")
                    out = int(out)
                    self.assertEqual(out, 0, "Unexpected: Found friend updates"
                                     " between other nodes")

        g.log.info("Expected: No friend updates between other peer nodes")

        # Check friend updates between rejoined node and other nodes
        cmd = ("awk '/%s/,0' /var/log/glusterfs/glusterd.log "
               "| grep '_handle_friend_update' | wc -l" % curr_time)
        ret, count, _ = g.run(self.servers[1], cmd)
        self.assertEqual(ret, 0, "Failed to fetch the count of friend updates")
        count = int(count)

        # Calculate the expected friend updates for a given cluster size
        expected_frnd_updts = min_updt * (crnt_clstr_size - min_clstr_sz + 1)

        self.assertEqual(count, expected_frnd_updts, "Count of friend updates"
                         " is not equal to the expected value")
