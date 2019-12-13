#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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
"""
    Description:
    A testcase to enable cluster.brick-multiplex and create three 1x3
    volumes and stop the volumes.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (cleanup_volume, setup_volume)
from glustolibs.gluster.volume_ops import (volume_stop, volume_start,
                                           set_volume_options)
from glustolibs.gluster.lib_utils import is_core_file_created
from glustolibs.gluster.brickmux_ops import get_brick_processes_count
from glustolibs.gluster.brick_libs import get_all_bricks


@runs_on([['replicated', 'distributed-replicated', 'distributed',
           'dispersed'], ['glusterfs']])
class TestEnableBrickMuxCreateAndStopThreevolumes(GlusterBaseClass):

    def tearDown(self):

        for number in range(1, 4):

            # Starting volumes.
            self.volume['name'] = ("test_volume_%s" % number)
            self.volname = ("test_volume_%s" % number)
            ret, _, _ = volume_start(self.mnode, self.volname)
            g.log.info("Volume %s started was successfully", self.volname)

            # Cleaning up volumes.
            ret = cleanup_volume(self.mnode, self.volname)
            if not ret:
                raise ExecutionError("Failed to cleanup %s" % self.volname)
            g.log.info("Successfully cleaned volume: %s", self.volname)

        # Setting cluster.brick-multiplex to disable.
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.brick-multiplex': 'disable'})
        if not ret:
            raise ExecutionError("Failed to disable cluster.brick-multiplex")
        g.log.info("Successfully set cluster.brick-multiplex to disable.")

        self.get_super_method(self, 'tearDown')()

    def test_enable_brickmux_create_and_stop_three_volumes(self):

        """
        Test Case:
        1.Set cluster.brick-multiplex to enabled.
        2.Create three 1x3 replica volumes.
        3.Start all the three volumes.
        4.Stop three volumes one by one.
        """

        # Timestamp of current test case of start time
        ret, test_timestamp, _ = g.run_local('date +%s')
        test_timestamp = test_timestamp.strip()

        # Setting cluster.brick-multiplex to enable
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.brick-multiplex': 'enable'})
        self.assertTrue(ret, "Failed to set brick-multiplex to enable.")
        g.log.info("Successfully set brick-multiplex to enable.")

        # Create and start 3 volume
        for number in range(1, 4):
            self.volume['name'] = ("test_volume_%s" % number)
            self.volname = ("test_volume_%s" % number)
            ret = setup_volume(self.mnode, self.all_servers_info,
                               self.volume)
            self.assertTrue(ret, "Failed to create and start %s" %
                            self.volname)
            g.log.info("Successfully created and started volume %s.",
                       self.volname)

        # Checking brick process count.
        for brick in get_all_bricks(self.mnode, self.volname):
            server = brick.split(":")[0]
            count = get_brick_processes_count(server)
            self.assertEqual(count, 1,
                             "ERROR: More than one brick process on %s."
                             % server)
            g.log.info("Only one brick process present on %s",
                       server)

        # Stop three volumes one by one.
        for number in range(1, 4):
            self.volume['name'] = ("test_volume_%s" % number)
            self.volname = ("test_volume_%s" % number)
            ret, _, _ = volume_stop(self.mnode, self.volname)
            self.assertEqual(ret, 0, "Failed to stop the volume %s"
                             % self.volname)
            g.log.info("Volume %s stopped successfully", self.volname)

        # Checking for core files.
        ret = is_core_file_created(self.servers, test_timestamp)
        self.assertTrue(ret, "Core file found.")
        g.log.info("No core files found, glusterd service running "
                   "successfully")
