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
    heal info completes when there is ongoing I/O and a lot of pending heals.
"""
import random
from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.io.utils import run_linux_untar
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['distributed-replicated'],
          ['glusterfs']])
class TestHealInfoNoHang(GlusterBaseClass):

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        self.is_io_running = False

        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        self.bricks_list = get_all_bricks(self.mnode, self.volname)
        self.subvols = get_subvols(self.mnode, self.volname)['volume_subvols']

    def tearDown(self):
        if self.is_io_running:
            if not self._wait_for_untar_completion():
                g.log.error("I/O failed to stop on clients")

        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        self.get_super_method(self, 'tearDown')()

    def _wait_for_untar_completion(self):
        """Wait for the kernel untar to complete"""
        has_process_stopped = []
        for proc in self.list_of_io_processes:
            try:
                ret, _, _ = proc.async_communicate()
                if not ret:
                    has_process_stopped.append(False)
                has_process_stopped.append(True)
            except ValueError:
                has_process_stopped.append(True)
        return all(has_process_stopped)

    def _does_heal_info_complete_within_timeout(self):
        """Check if heal info CLI completes within a specific timeout"""
        # We are just assuming 1 entry takes one second to process, which is
        # a very high number but some estimate is better than a random magic
        # value for timeout.
        timeout = self.num_entries * 1

        # heal_info_data = get_heal_info(self.mnode, self.volname)
        cmd = "timeout %s  gluster volume heal %s info" % (timeout,
                                                           self.volname)
        ret, _, _ = g.run(self.mnode, cmd)
        if ret:
            return False
        return True

    def test_heal_info_no_hang(self):
        """
        Testcase steps:
        1. Start kernel untar on the mount
        2. While untar is going on, kill a brick of the replica.
        3. Wait for the untar to be over, resulting in pending heals.
        4. Get the approx. number of pending heals and save it
        5. Bring the brick back online.
        6. Trigger heal
        7. Run more I/Os with dd command
        8. Run heal info command and check that it completes successfully under
           a timeout that is based on the no. of heals in step 4.
        """
        self.list_of_io_processes = []
        self.linux_untar_dir = "{}/{}".format(self.mounts[0].mountpoint,
                                              "linuxuntar")
        ret = mkdir(self.clients[0], self.linux_untar_dir)
        self.assertTrue(ret, "Failed to create dir linuxuntar for untar")

        # Start linux untar on dir linuxuntar
        ret = run_linux_untar(self.clients[0], self.mounts[0].mountpoint,
                              dirs=tuple(['linuxuntar']))
        self.list_of_io_processes += ret
        self.is_io_running = True

        # Kill brick resulting in heal backlog.
        brick_to_bring_offline = random.choice(self.bricks_list)
        ret = bring_bricks_offline(self.volname, brick_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline'
                        % brick_to_bring_offline)
        ret = are_bricks_offline(self.mnode, self.volname,
                                 [brick_to_bring_offline])
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % brick_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   brick_to_bring_offline)

        ret = self._wait_for_untar_completion()
        self.assertFalse(ret, "IO didn't complete or failed on client")
        self.is_io_running = False

        # Get approx. no. of entries to be healed.
        cmd = ("gluster volume heal %s statistics heal-count | grep Number "
               "| awk '{sum+=$4} END {print sum/2}'" % self.volname)
        ret, self.num_entries, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to get heal-count statistics")

        # Restart the down bricks
        ret = bring_bricks_online(self.mnode, self.volname,
                                  brick_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring brick %s online' %
                        brick_to_bring_offline)
        g.log.info('Bringing brick %s online is successful',
                   brick_to_bring_offline)
        # Trigger heal
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Starting heal failed')
        g.log.info('Index heal launched')

        # Run more I/O
        cmd = ("for i in `seq 1 10`; do dd if=/dev/urandom of=%s/file_$i "
               "bs=1M count=100; done" % self.mounts[0].mountpoint)
        ret = g.run_async(self.mounts[0].client_system, cmd,
                          user=self.mounts[0].user)

        # Get heal info
        ret = self._does_heal_info_complete_within_timeout()
        self.assertTrue(ret, 'Heal info timed out')
        g.log.info('Heal info completed succesfully')
