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
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from random import choice
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status, wait_for_volume_process_to_be_online,
    setup_volume, cleanup_volume)
from glustolibs.gluster.lib_utils import get_servers_bricks_dict
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.brick_ops import replace_brick
from glustolibs.gluster.heal_libs import (get_self_heal_daemon_pid,
                                          do_bricks_exist_in_shd_volfile,
                                          is_shd_daemonized)
from glustolibs.gluster.volume_ops import get_volume_list


class SelfHealDaemonProcessTestsWithMultipleVolumes(GlusterBaseClass):
    """
    SelfHealDaemonProcessTestsWithMultipleVolumes contains tests which
    verifies the self-heal daemon process on multiple volumes running.
    """
    def setUp(self):
        """
        setup volume and initialize necessary variables
        which is used in tests
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume for all the volume types
        self.volume_configs = []
        for volume_type in self.default_volume_type_config:
            self.volume_configs.append(
                {'name': 'testvol_%s' % volume_type,
                 'servers': self.servers,
                 'voltype': self.default_volume_type_config[volume_type]})

        for volume_config in self.volume_configs[1:]:
            ret = setup_volume(mnode=self.mnode,
                               all_servers_info=self.all_servers_info,
                               volume_config=volume_config,
                               multi_vol=True)
            volname = volume_config['name']
            if not ret:
                raise ExecutionError("Failed to setup Volume"
                                     " %s" % volname)
            g.log.info("Successful in setting volume %s", volname)

            # Verify volume's all process are online for 60 sec
            ret = wait_for_volume_process_to_be_online(self.mnode, volname, 60)
            if not ret:
                raise ExecutionError("Volume %s : All process are not online"
                                     % volname)
            g.log.info("Successfully Verified volume %s processes are online",
                       volname)

        # Verfiy glustershd process releases its parent process
        ret = is_shd_daemonized(self.servers)
        if not ret:
            raise ExecutionError("Self Heal Daemon process was still"
                                 " holding parent process.")
        g.log.info("Self Heal Daemon processes are online")

        self.glustershd = "/var/lib/glusterd/glustershd/glustershd-server.vol"

    def tearDown(self):
        """
        Clean up the volume and umount volume from client
        """

        # Cleanup volume
        volume_list = get_volume_list(self.mnode)
        for volume in volume_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Failed to cleanup Volume %s" % volume)
        g.log.info("Successfully Cleaned up all Volumes")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_impact_of_replace_brick_on_glustershd(self):
        """
        Test Script to verify the glustershd server vol file
        has only entries for replicate volumes
        1.Create multiple volumes and start all volumes
        2.Check the glustershd processes - Only 1 glustershd should be listed
        3.Do replace brick on the replicate volume
        4.Confirm that the brick is replaced
        5.Check the glustershd processes - Only 1 glustershd should be listed
                                           and pid should be different
        6.glustershd server vol should be updated with new bricks
        """
        # Check the self-heal daemon process
        ret, glustershd_pids = get_self_heal_daemon_pid(self.servers)
        self.assertTrue(ret, ("Either no self heal daemon process found or "
                              "more than one self heal daemon process "
                              "found : %s" % glustershd_pids))
        g.log.info("Successful in getting single self heal daemon process"
                   " on all nodes %s", self.servers)

        volume_list = get_volume_list(self.mnode)
        for volume in volume_list:

            # Log Volume Info and Status before replacing brick
            ret = log_volume_info_and_status(self.mnode, volume)
            self.assertTrue(ret, ("Logging volume info and status "
                                  "failed on volume %s", volume))
            g.log.info("Successful in logging volume info and status "
                       "of volume %s", volume)

            # Selecting a random source brick to replace
            src_brick = choice(get_all_bricks(self.mnode, volume))
            src_node, original_brick = src_brick.split(":")

            # Creating a random destination brick in such a way
            # that the brick is select from the same node but always
            # picks a different from the original brick
            list_of_bricks = [
                brick for brick in get_servers_bricks_dict(
                    src_node, self.all_servers_info)[src_node]
                if brick not in original_brick]
            dst_brick = ('{}:{}/{}_replaced'.format(
                src_node, choice(list_of_bricks),
                original_brick.split('/')[::-1][0]))

            # Replace brick for the volume
            ret, _, _ = replace_brick(self.mnode, volume,
                                      src_brick, dst_brick)
            self.assertFalse(ret, "Failed to replace brick "
                             "from the volume %s" % volume)
            g.log.info("Successfully replaced faulty brick from "
                       "the volume %s", volume)

            # Verify all volume process are online
            ret = wait_for_volume_process_to_be_online(self.mnode, volume)
            self.assertTrue(ret, "Volume %s : All process are not online"
                            % volume)
            g.log.info("Volume %s : All process are online", volume)

            # Check the self-heal daemon process after replacing brick
            ret, pid_after_replace = get_self_heal_daemon_pid(self.servers)
            self.assertTrue(ret, "Either no self heal daemon process "
                            "found or more than one self heal "
                            "daemon process found : %s" % pid_after_replace)
            g.log.info("Successful in getting Single self heal "
                       " daemon process on all nodes %s", self.servers)

            # Compare the glustershd pids
            self.assertNotEqual(glustershd_pids, pid_after_replace,
                                "Self heal daemon process should be different "
                                "after replacing bricks in %s volume"
                                % volume)
            g.log.info("EXPECTED: Self heal daemon process should be different"
                       " after replacing bricks in replicate volume")

            # Get the bricks for the volume
            bricks_list = get_all_bricks(self.mnode, volume)
            g.log.info("Brick List : %s", bricks_list)

            # Validate the bricks present in volume info with
            # glustershd server volume file
            ret = do_bricks_exist_in_shd_volfile(self.mnode, volume,
                                                 bricks_list)
            self.assertTrue(ret, ("Brick List from volume info is "
                                  "different from glustershd server "
                                  "volume file. Please check log file "
                                  "for details"))
            g.log.info("Bricks in volume %s exists in glustershd server "
                       "volume file", volume)
