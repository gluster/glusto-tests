#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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
        Test Cases in this module tests the self heal daemon process.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.volume_libs import (
    wait_for_volume_process_to_be_online, setup_volume, cleanup_volume,
    get_volume_type_info)
from glustolibs.gluster.heal_libs import (get_self_heal_daemon_pid,
                                          is_shd_daemonized,)
from glustolibs.gluster.volume_ops import (volume_stop, volume_start,
                                           get_volume_list)


class SelfHealDaemonProcessTestsWithMultipleVolumes(GlusterBaseClass):
    """
    SelfHealDaemonProcessTestsWithMultipleVolumes contains tests which
    verifies the self-heal daemon process on multiple volumes running.
    """
    @classmethod
    def setUpClass(cls):
        """
        setup volume and initialize necessary variables
        which is used in tests
        """
        # calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        list_of_vol = ['distributed-dispersed', 'replicated',
                       'dispersed', 'distributed', 'distributed-replicated']
        cls.volume_configs = []
        if cls.default_volume_type_config['distributed']['dist_count'] > 3:
            cls.default_volume_type_config['distributed']['dist_count'] = 3

        for volume_type in list_of_vol:
            cls.volume_configs.append(
                {'name': 'testvol_%s' % (volume_type),
                 'servers': cls.servers,
                 'voltype': cls.default_volume_type_config[volume_type]})
        for volume_config in cls.volume_configs:
            ret = setup_volume(mnode=cls.mnode,
                               all_servers_info=cls.all_servers_info,
                               volume_config=volume_config)
            volname = volume_config['name']
            if not ret:
                raise ExecutionError("Failed to setup Volume"
                                     " %s" % volname)
            g.log.info("Successful in setting volume %s", volname)

            # Verify volume's all process are online for 60 sec
            g.log.info("Verifying volume's all process are online")
            ret = wait_for_volume_process_to_be_online(cls.mnode, volname, 60)
            if not ret:
                raise ExecutionError("Volume %s : All process are not online"
                                     % volname)
            g.log.info("Successfully Verified volume %s processes are online",
                       volname)

        # Verfiy glustershd process releases its parent process
        g.log.info("Verifying Self Heal Daemon process is daemonized")
        ret = is_shd_daemonized(cls.servers)
        if not ret:
            raise ExecutionError("Self Heal Daemon process was still"
                                 " holding parent process.")
        g.log.info("Self Heal Daemon processes are online")

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume and umount volume from client
        """

        # stopping the volume
        g.log.info("Starting to Cleanup all Volumes")
        volume_list = get_volume_list(cls.mnode)
        for volume in volume_list:
            ret = cleanup_volume(cls.mnode, volume)
            if not ret:
                raise ExecutionError("Failed to cleanup Volume %s" % volume)
            g.log.info("Volume: %s cleanup is done", volume)
        g.log.info("Successfully Cleanedup all Volumes")

        # calling GlusterBaseClass tearDownClass
        cls.get_super_method(cls, 'tearDownClass')()

    def test_no_glustershd_with_distribute(self):
        """
        Test Script to verify the glustershd server vol file
        has only entries for replicate volumes

        * Create multiple volumes and start all volumes
        * Check the glustershd processes - Only 1 glustershd should be listed
        * Stop all volumes
        * Check the glustershd processes - No glustershd should be running
        * Start the distribute volume only
        * Check the glustershd processes - No glustershd should be running

        """

        nodes = self.servers

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either no self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in getting single self heal daemon process"
                   " on all nodes %s", nodes)

        # stop all the volumes
        g.log.info("Going to stop all the volumes")
        volume_list = get_volume_list(self.mnode)
        for volume in volume_list:
            g.log.info("Stopping Volume : %s", volume)
            ret = volume_stop(self.mnode, volume)
            self.assertTrue(ret, ("Failed to stop volume %s" % volume))
            g.log.info("Successfully stopped volume %s", volume)
        g.log.info("Successfully stopped all the volumes")

        # check the self-heal daemon process after stopping all volumes
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertFalse(ret, ("Self heal daemon process is still running "
                               "after stopping all volumes "))
        for node in pids:
            self.assertEquals(pids[node][0], -1, ("Self heal daemon is still "
                                                  "running on node %s even "
                                                  "after stoppong all "
                                                  "volumes" % node))
        g.log.info("EXPECTED: No self heal daemon process is "
                   "running after stopping all volumes")

        # start the distribute volume only
        for volume in volume_list:
            volume_type_info = get_volume_type_info(self.mnode, volume)
            volume_type = (volume_type_info['volume_type_info']['typeStr'])
            if volume_type == 'Distribute':
                g.log.info("starting to start distribute volume: %s", volume)
                ret = volume_start(self.mnode, volume)
                self.assertTrue(ret, ("Failed to start volume %s" % volume))
                g.log.info("Successfully started volume %s", volume)
                break

        # check the self-heal daemon process after starting distribute volume
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertFalse(ret, ("Self heal daemon process is still running "
                               "after stopping all volumes "))
        for node in pids:
            self.assertEquals(pids[node][0], -1, ("Self heal daemon is still "
                                                  "running on node %s even "
                                                  "after stopping all "
                                                  "volumes" % node))
        g.log.info("EXPECTED: No self heal daemon process is running "
                   "after stopping all volumes")
