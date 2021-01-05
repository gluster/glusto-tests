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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed', 'distributed-replicated'],
          ['glusterfs']])
class TestRebalanceMultipleExpansions(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Setup and mount volume")

        self.first_client = self.mounts[0].client_system

    def tearDown(self):

        # Unmount and clean volume
        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_rebalance_multiple_expansions(self):
        """
        Test case:
        1. Create a volume, start it and mount it
        2. Create some file on mountpoint
        3. Collect arequal checksum on mount point pre-rebalance
        4. Do the following 3 times:
        5. Expand the volume
        6. Start rebalance and wait for it to finish
        7. Collect arequal checksum on mount point post-rebalance
           and compare with value from step 3
        """

        # Create some file on mountpoint
        cmd = ("cd %s; for i in {1..500} ; do "
               "dd if=/dev/urandom of=file$i bs=10M count=1; done"
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertEqual(ret, 0, "IO failed on volume %s"
                         % self.volname)

        # Collect arequal checksum before rebalance
        arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        for _ in range(3):
            # Add brick to volume
            ret = expand_volume(self.mnode, self.volname, self.servers,
                                self.all_servers_info)
            self.assertTrue(ret, "Failed to add brick on volume %s"
                            % self.volname)

            # Trigger rebalance and wait for it to complete
            ret, _, _ = rebalance_start(self.mnode, self.volname,
                                        force=True)
            self.assertEqual(ret, 0, "Failed to start rebalance on "
                             "volume %s" % self.volname)

            # Wait for rebalance to complete
            ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                                 timeout=1200)
            self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                                 "%s" % self.volname)
            g.log.info("Rebalance successfully completed")

            # Collect arequal checksum after rebalance
            arequal_checksum_after = collect_mounts_arequal(self.mounts[0])

            # Check for data loss by comparing arequal before and after
            # rebalance
            self.assertEqual(arequal_checksum_before, arequal_checksum_after,
                             "arequal checksum is NOT MATCHNG")
            g.log.info("arequal checksum is SAME")
