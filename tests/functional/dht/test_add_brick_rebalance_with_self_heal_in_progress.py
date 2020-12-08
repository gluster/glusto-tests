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

from random import choice
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks, bring_bricks_online
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import kill_process


@runs_on([['distributed-replicated', 'distributed-arbiter'], ['glusterfs']])
class TestAddBrickRebalanceWithSelfHeal(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Setup and mount volume")

        self.is_io_running = False

    def tearDown(self):

        # If I/O processes are running wait for it to complete
        if self.is_io_running:
            if not wait_for_io_to_complete(self.list_of_io_processes,
                                           [self.mounts[0]]):
                raise ExecutionError("Failed to wait for I/O to complete")

        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_add_brick_rebalance_with_self_heal_in_progress(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Start creating a few files on mount point.
        3. While file creation is going on, kill one of the bricks
           in the replica pair.
        4. After file creattion is complete collect arequal checksum
           on mount point.
        5. Bring back the brick online by starting volume with force.
        6. Check if all bricks are online and if heal is in progress.
        7. Add bricks to the volume and start rebalance.
        8. Wait for rebalance and heal to complete on volume.
        9. Collect arequal checksum on mount point and compare
           it with the one taken in step 4.
        """
        # Start I/O from mount point and wait for it to complete
        cmd = ("cd %s; for i in {1..1000} ; do "
               "dd if=/dev/urandom of=file$i bs=10M count=1; done"
               % self.mounts[0].mountpoint)
        self.list_of_io_processes = [
            g.run_async(self.mounts[0].client_system, cmd)]
        self.is_copy_running = True

        # Get a list of all the bricks to kill brick
        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Empty present brick list")

        # Kill brick process of a brick which is being removed
        brick = choice(brick_list)
        node, _ = brick.split(":")
        ret = kill_process(node, process_names="glusterfsd")
        self.assertTrue(ret, "Failed to kill brick process of brick %s"
                        % brick)

        # Validate if I/O was successful or not.
        ret = validate_io_procs(self.list_of_io_processes, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.is_copy_running = False

        # Collect arequal checksum before ops
        arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        # Bring back the brick online by starting volume with force
        ret = bring_bricks_online(self.mnode, self.volname, brick_list,
                                  bring_bricks_online_methods=[
                                      'volume_start_force'])
        self.assertTrue(ret, "Error in bringing back brick online")
        g.log.info('All bricks are online now')

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

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1200)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

        # Wait for heal to complete
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "heal has not yet completed")
        g.log.info("Self heal completed")

        # Check for data loss by comparing arequal before and after ops
        arequal_checksum_after = collect_mounts_arequal(self.mounts[0])
        self.assertEqual(arequal_checksum_before, arequal_checksum_after,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")
