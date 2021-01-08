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

""" Description:
      Increase in glusterd memory consumption on repetetive operations
      for 100 volumes
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.volume_ops import (volume_stop, volume_delete,
                                           get_volume_list,
                                           volume_start)
from glustolibs.gluster.gluster_init import (restart_glusterd,
                                             wait_for_glusterd_to_start)
from glustolibs.gluster.volume_libs import (bulk_volume_creation,
                                            cleanup_volume)
from glustolibs.gluster.volume_ops import set_volume_options


class TestGlusterMemoryConsumptionIncrease(GlusterBaseClass):
    def tearDown(self):
        # Clean up all volumes
        if self.volume_present:
            vol_list = get_volume_list(self.mnode)
            if vol_list is None:
                raise ExecutionError("Failed to get the volume list")

            for volume in vol_list:
                ret = cleanup_volume(self.mnode, volume)
                if not ret:
                    raise ExecutionError("Unable to delete volume %s" % volume)
                g.log.info("Volume deleted successfully : %s", volume)

        # Disable multiplex
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.brick-multiplex': 'disable'})
        self.assertTrue(ret, "Failed to enable brick-multiplex"
                        " for the cluster")

        # Calling baseclass tearDown method
        self.get_super_method(self, 'tearDown')()

    def _volume_operations_in_loop(self):
        """ Create, start, stop and delete 100 volumes in a loop """
        # Create and start 100 volumes in a loop
        self.volume_config = {
            'name': 'volume-',
            'servers': self.servers,
            'voltype': {'type': 'distributed-replicated',
                        'dist_count': 2,
                        'replica_count': 3},
        }

        ret = bulk_volume_creation(self.mnode, 100, self.all_servers_info,
                                   self.volume_config, "", False, True)
        self.assertTrue(ret, "Failed to create volumes")

        self.volume_present = True

        g.log.info("Successfully created all the volumes")

        # Start 100 volumes in loop
        for i in range(100):
            self.volname = "volume-%d" % i
            ret, _, _ = volume_start(self.mnode, self.volname)
            self.assertEqual(ret, 0, "Failed to start volume: %s"
                             % self.volname)

        g.log.info("Successfully started all the volumes")

        # Stop 100 volumes in loop
        for i in range(100):
            self.volname = "volume-%d" % i
            ret, _, _ = volume_stop(self.mnode, self.volname)
            self.assertEqual(ret, 0, "Failed to stop volume: %s"
                             % self.volname)

        g.log.info("Successfully stopped all the volumes")

        # Delete 100 volumes in loop
        for i in range(100):
            self.volname = "volume-%d" % i
            ret = volume_delete(self.mnode, self.volname)
            self.assertTrue(ret, "Failed to delete volume: %s"
                            % self.volname)

        self.volume_present = False

        g.log.info("Successfully deleted all the volumes")

    def _memory_consumption_for_all_nodes(self, pid_list):
        """Fetch the memory consumption by glusterd process for
           all the nodes
        """
        memory_consumed_list = []
        for i, server in enumerate(self.servers):
            # Get the memory consumption of glusterd in each node
            cmd = "top -b -n 1 -p %d | awk 'FNR==8 {print $6}'" % pid_list[i]
            ret, mem, _ = g.run(server, cmd)
            self.assertEqual(ret, 0, "Failed to get the memory usage of"
                             " glusterd process")
            mem = int(mem)//1024
            memory_consumed_list.append(mem)

        return memory_consumed_list

    def test_glusterd_memory_consumption_increase(self):
        """
        Test Case:
        1) Enable brick-multiplex and set max-bricks-per-process to 3 in
           the cluster
        2) Get the glusterd memory consumption
        3) Perform create,start,stop,delete operation for 100 volumes
        4) Check glusterd memory consumption, it should not increase by
           more than 50MB
        5) Repeat steps 3-4 for two more time
        6) Check glusterd memory consumption it should not increase by
           more than 10MB
        """
        # pylint: disable=too-many-locals
        # Restarting glusterd to refresh its memory consumption
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, "Restarting glusterd failed")

        # check if glusterd is running post reboot
        ret = wait_for_glusterd_to_start(self.servers)
        self.assertTrue(ret, "Glusterd service is not running post reboot")

        # Enable brick-multiplex, set max-bricks-per-process to 3 in cluster
        for key, value in (('cluster.brick-multiplex', 'enable'),
                           ('cluster.max-bricks-per-process', '3')):
            ret = set_volume_options(self.mnode, 'all', {key: value})
            self.assertTrue(ret, "Failed to set {} to {} "
                            " for the cluster".format(key, value))

        # Get the pidof of glusterd process
        pid_list = []
        for server in self.servers:
            # Get the pidof of glusterd process
            cmd = "pidof glusterd"
            ret, pid, _ = g.run(server, cmd)
            self.assertEqual(ret, 0, "Failed to get the pid of glusterd")
            pid = int(pid)
            pid_list.append(pid)

        # Fetch the list of memory consumed in all the nodes
        mem_consumed_list = self._memory_consumption_for_all_nodes(pid_list)

        # Perform volume operations for 100 volumes for first time
        self._volume_operations_in_loop()

        # Fetch the list of memory consumed in all the nodes after 1 iteration
        mem_consumed_list_1 = self._memory_consumption_for_all_nodes(pid_list)

        for i, mem in enumerate(mem_consumed_list_1):
            condition_met = False
            if mem - mem_consumed_list[i] <= 50:
                condition_met = True

            self.assertTrue(condition_met, "Unexpected: Memory consumption"
                            " glusterd increased more than the expected"
                            " of value")

        # Perform volume operations for 100 volumes for second time
        self._volume_operations_in_loop()

        # Fetch the list of memory consumed in all the nodes after 2 iterations
        mem_consumed_list_2 = self._memory_consumption_for_all_nodes(pid_list)

        for i, mem in enumerate(mem_consumed_list_2):
            condition_met = False
            if mem - mem_consumed_list_1[i] <= 10:
                condition_met = True

            self.assertTrue(condition_met, "Unexpected: Memory consumption"
                            " glusterd increased more than the expected"
                            " of value")

        # Perform volume operations for 100 volumes for third time
        self._volume_operations_in_loop()

        # Fetch the list of memory consumed in all the nodes after 3 iterations
        mem_consumed_list_3 = self._memory_consumption_for_all_nodes(pid_list)

        for i, mem in enumerate(mem_consumed_list_3):
            condition_met = False
            if mem - mem_consumed_list_2[i] <= 10:
                condition_met = True

            self.assertTrue(condition_met, "Unexpected: Memory consumption"
                            " glusterd increased more than the expected"
                            " of value")
