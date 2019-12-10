#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
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
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brickmux_ops import (
    disable_brick_mux, is_brick_mux_enabled,
    enable_brick_mux, get_brick_mux_status,
    check_brick_pid_matches_glusterfsd_pid, get_brick_processes_count)
from glustolibs.gluster.volume_libs import setup_volume, cleanup_volume
from glustolibs.gluster.volume_ops import (get_volume_list,
                                           volume_stop,
                                           get_volume_options,
                                           volume_start)


@runs_on([['replicated'], ['glusterfs']])
class TestBrickMultiplexingResetCommand(GlusterBaseClass):
    """
    Tests for brick multiplexing reset command
    """
    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Enable brick multiplexing
        g.log.info("Checking for brick multiplexing status...")
        if not is_brick_mux_enabled(self.mnode):
            g.log.info("Enabling brick multiplexing...")
            if not enable_brick_mux(self.mnode):
                raise ExecutionError("Failed to enable brick multiplexing")
            g.log.info("Enabled brick multiplexing successfully")

    def tearDown(self):
        # Stopping all volumes
        g.log.info("Starting to Cleanup all Volumes")
        volume_list = get_volume_list(self.mnode)
        for volume in volume_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Failed to cleanup Volume %s" % volume)
            g.log.info("Volume: %s cleanup is done", volume)
        g.log.info("Successfully Cleanedup all Volumes")

        # Disable brick multiplexing
        g.log.info("Checking for brick multiplexing status...")
        if is_brick_mux_enabled(self.mnode):
            g.log.info("Disabling brick multiplexing...")
            if not disable_brick_mux(self.mnode):
                raise ExecutionError("Failed to disable brick multiplexing")
            g.log.info("Disabled brick multiplexing successfully")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def use_config_setup_volumes(self):
        """
        A function to setup volumes based on volume_configs.
        """
        for volume_config in self.volume_configs:
            ret = setup_volume(mnode=self.mnode,
                               all_servers_info=self.all_servers_info,
                               volume_config=volume_config,
                               force=False)
            if not ret:
                raise ExecutionError("Failed to setup Volume %s"
                                     % volume_config['name'])
            g.log.info("Successful in setting volume %s",
                       volume_config['name'])

    def test_cli_reset_commands_behaviour(self):
        """
        1. Set cluster.brick-multiplex to enabled.
        2. Create and start 2 volumes of type 1x3 and 2x3.
        3. Check if cluster.brick-multiplex is enabled.
        4. Reset the cluster using "gluster v reset all".
        5. Check if cluster.brick-multiplex is disabled.
        6. Create a new volume of type 2x3.
        7. Set cluster.brick-multiplex to enabled.
        8. Stop and start all three volumes.
        9. Check the if pids match and check if more
           than one pids of glusterfsd is present.
        """
        # pylint: disable=too-many-statements
        # Setup Volumes
        self.volume_configs = []

        # Define volumes to create
        # Define replicated volume
        self.volume['voltype'] = {
            'type': 'replicated',
            'replica_count': 3,
            'transport': 'tcp'}

        volume_config = {'name': '%s' % self.volume['voltype']['type'],
                         'servers': self.servers,
                         'voltype': self.volume['voltype']}
        self.volume_configs.append(volume_config)

        # Define 2x3 distributed-replicated volume
        self.volume['voltype'] = {
            'type': 'distributed-replicated',
            'dist_count': 2,
            'replica_count': 3,
            'transport': 'tcp'}

        volume_config = {'name': '%s' % self.volume['voltype']['type'],
                         'servers': self.servers,
                         'voltype': self.volume['voltype']}
        self.volume_configs.append(volume_config)

        # Create volumes using the config.
        self.use_config_setup_volumes()

        # Check if volume option cluster.brick-multiplex is enabled
        volume_list = get_volume_list(self.mnode)
        for volume in volume_list:
            options_dict = get_volume_options(self.mnode, volume)
            self.assertEqual(options_dict['cluster.brick-multiplex'], 'enable',
                             'Option brick-multiplex is not enabled')
            g.log.info('Option brick-multiplex is enabled for volume %s',
                       volume)

        # Reset cluster
        g.log.info("Reset cluster...")
        cmd = 'gluster v reset all'
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertFalse(ret, "Failed on reset cluster")
        g.log.info("Successfully reset cluster")

        # Check if brick-multiplex is disabled
        g.log.info("Checking for brick multiplexing status...")
        self.assertEqual('disable', get_brick_mux_status(self.mnode),
                         "Brick multiplexing status is not 'disable'")
        g.log.info("Brick multiplexing status is 'disable'")

        # Create new distributed-replicated volume
        # Define new 2x3 distributed-replicated volume
        new_vol = 'new_vol'
        self.volume['voltype'] = {
            'type': 'distributed-replicated',
            'dist_count': 2,
            'replica_count': 3,
            'transport': 'tcp'}

        volume_config = {'name': '%s' % new_vol,
                         'servers': self.servers,
                         'voltype': self.volume['voltype']}
        self.volume_configs.append(volume_config)

        # Create volumes using the config.
        self.use_config_setup_volumes()

        # Resetting brick-mux back to enabled.
        g.log.info("Enabling brick multiplexing...")
        if not enable_brick_mux(self.mnode):
            raise ExecutionError("Failed to enable brick multiplexing")
        g.log.info("Enabled brick multiplexing successfully")

        # Restart all volumes
        g.log.info("Restarting all volumes...")
        volume_list = get_volume_list(self.mnode)
        for volume in volume_list:
            # Stop the volume
            g.log.info("Stopping volume %s...", volume)
            ret, _, err = volume_stop(self.mnode, volume)
            self.assertFalse(ret, "Failed on stopping volume %s: %s"
                             % (volume, err))
            g.log.info("Stopped %s successfully", volume)

            # Sleeping for 2 seconds between stop and start.
            sleep(2)

            # Start the volume
            g.log.info("Starting volume %s...", volume)
            ret, _, err = volume_start(self.mnode, volume)
            self.assertFalse(ret, "Failed on starting volume %s: %s"
                             % (volume, err))
            g.log.info("Started %s successfully", volume)
        g.log.info("Restarted all volumes successfully")

        # Check if bricks pid don`t match glusterfsd pid
        g.log.info("Checking if bricks pid don`t match glusterfsd pid...")
        for volume in volume_list:
            g.log.info("Checking if bricks pid don`t match glusterfsd pid "
                       "for %s volume...", volume)
            self.assertTrue(
                check_brick_pid_matches_glusterfsd_pid(self.mnode, volume),
                "Bricks pid match glusterfsd pid for %s volume..." % volume)
            g.log.info("Bricks pid don`t match glusterfsd pid "
                       "for %s volume...", volume)

        # Checking if the number of glusterfsd is more than one
        for server in self.servers:
            ret = get_brick_processes_count(server)
            self.assertEqual(ret, 1,
                             "Number of glusterfsd is more than one.")
        g.log.info("Only one glusterfsd found on all the nodes.")
