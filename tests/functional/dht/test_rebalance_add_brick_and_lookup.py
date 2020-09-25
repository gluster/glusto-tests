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
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-131 USA.

"""
Description:
    Rebalance with add brick and log time taken for lookup
"""

from time import time
from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import expand_volume


@runs_on([['distributed', 'distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed'], ['glusterfs']])
class TestRebalanceWithAddBrickAndLookup(GlusterBaseClass):
    """ Rebalance with add brick and do lookup """

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        self.all_mounts_procs = []

        # Setup and mount the volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup Volume and Mount it")

    def test_rebalance_with_add_brick_and_lookup(self):
        """
        Rebalance with add brick and then lookup on mount
        - Create a Distributed-Replicated volume.
        - Create deep dirs(200) and 100 files on the deepest directory.
        - Expand volume.
        - Initiate rebalance
        - Once rebalance is completed, do a lookup on mount and time it.
        """
        # Create Deep dirs.
        cmd = (
            "cd %s/; for i in {1..200};do mkdir dir${i}; cd dir${i};"
            " if [ ${i} -eq 100 ]; then for j in {1..100}; do touch file${j};"
            " done; fi; done;" % (self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to create the deep dirs and files")
        g.log.info("Deep dirs and files created.")

        # Expand the volume.
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume %s", self.volname))
        g.log.info("Expanding volume is successful on "
                   "volume %s", self.volname)

        # Start Rebalance.
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=500)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        # Do a lookup on the mountpoint and note the time taken to run.
        # The time used for comparison is taken as a benchmark on using a
        # RHGS 3.5.2 for this TC. For 3.5.2, the time takes came out to be
        # 4 seconds. Now the condition for subtest to pass is for the lookup
        # should not be more than 10% of this value, i.e. 4.4 seconds.
        cmd = ("ls -R %s/" % (self.mounts[0].mountpoint))
        start_time = time()
        ret, _, _ = g.run(self.clients[0], cmd)
        end_time = time()
        self.assertEqual(ret, 0, "Failed to do a lookup")
        time_taken = end_time - start_time
        self.assertTrue(time_taken <= 4.4, "Lookup takes more time "
                        "than the previously benchmarked value.")
        g.log.info("Lookup took : %d seconds", time_taken)

    def tearDown(self):
        """tear Down callback"""
        # Unmount Volume and cleanup.
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Filed to Unmount Volume and "
                                 "Cleanup Volume")
        g.log.info("Successful in Unmount Volume and cleanup.")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
