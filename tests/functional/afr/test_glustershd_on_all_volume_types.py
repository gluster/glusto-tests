#  Copyright (C) 2016-2018  Red Hat, Inc. <http://www.redhat.com>
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


from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.volume_libs import (
    expand_volume, log_volume_info_and_status,
    wait_for_volume_process_to_be_online, setup_volume, cleanup_volume,
    get_volume_type_info)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.heal_libs import (get_self_heal_daemon_pid,
                                          do_bricks_exist_in_shd_volfile,
                                          is_shd_daemonized)
from glustolibs.gluster.volume_ops import get_volume_list


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
        GlusterBaseClass.setUpClass.im_func(cls)

        cls.default_volume_type_config = {
            'replicated': {
                'type': 'replicated',
                'replica_count': 2,
                'transport': 'tcp'},
            'dispersed': {
                'type': 'dispersed',
                'disperse_count': 6,
                'redundancy_count': 2,
                'transport': 'tcp'},
            'distributed': {
                'type': 'distributed',
                'dist_count': 2,
                'transport': 'tcp'},
            'distributed-replicated': {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 3,
                'transport': 'tcp'}
        }

        # Setup Volume for all the volume types
        cls.volume_configs = []
        for volume_type in cls.default_volume_type_config:
            cls.volume_configs.append(
                {'name': 'testvol_%s' % volume_type,
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

        cls.GLUSTERSHD = "/var/lib/glusterd/glustershd/glustershd-server.vol"

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
        GlusterBaseClass.tearDownClass.im_func(cls)

    def test_glustershd_on_all_volume_types(self):
        """
        Test Script to verify the glustershd server vol file
        has only entries for replicate volumes

        * Create multiple volumes and start all volumes
        * Check the glustershd processes - Only One glustershd should be listed
        * Check the glustershd server vol file - should contain entries only
                                             for replicated involved volumes
        * Add bricks to the replicate volume - it should convert to
                                               distributed-replicate
        * Check the glustershd server vol file - newly added bricks
                                                 should present
        * Check the glustershd processes - Only 1 glustershd should be listed

        """
        # pylint: disable=too-many-statements
        nodes = self.servers

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, glustershd_pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s" % glustershd_pids))
        g.log.info("Successful in getting Single self heal daemon process"
                   " on all nodes %s", nodes)

        # For all the volumes, check whether bricks present in
        # glustershd server vol file
        volume_list = get_volume_list(self.mnode)
        for volume in volume_list:
            g.log.info("Volume Name: %s", volume)
            volume_type_info = get_volume_type_info(self.mnode, volume)
            volume_type = (volume_type_info['volume_type_info']['typeStr'])

            # get the bricks for the volume
            g.log.info("Fetching bricks for the volume : %s", volume)
            bricks_list = get_all_bricks(self.mnode, volume)
            g.log.info("Brick List : %s", bricks_list)

            # validate the bricks present in volume info with
            # glustershd server volume file
            g.log.info("Start parsing file %s on "
                       "node %s", self.GLUSTERSHD, self.mnode)
            ret = do_bricks_exist_in_shd_volfile(self.mnode, volume,
                                                 bricks_list)
            if volume_type == 'Distribute':
                self.assertFalse(ret, ("Bricks exist in glustershd server "
                                       "volume file for %s Volume"
                                       % volume_type))
                g.log.info("EXPECTED : Bricks doesn't exist in glustershd "
                           "server volume file for %s Volume", volume_type)
            else:
                self.assertTrue(ret, ("Brick List from volume info is "
                                      "different from glustershd server "
                                      "volume file. Please check log "
                                      "file for details"))
                g.log.info("Bricks exist in glustershd server volume file "
                           "for %s Volume", volume_type)

        # expanding volume for Replicate
        for volume in volume_list:
            volume_type_info = get_volume_type_info(self.mnode, volume)
            volume_type = (volume_type_info['volume_type_info']['typeStr'])
            if volume_type == 'Replicate':
                g.log.info("Start adding bricks to volume %s", volume)
                ret = expand_volume(self.mnode, volume, self.servers,
                                    self.all_servers_info)
                self.assertTrue(ret, ("Failed to add bricks to "
                                      "volume %s " % volume))
                g.log.info("Add brick successful")

                # Log Volume Info and Status after expanding the volume
                g.log.info("Logging volume info and Status after "
                           "expanding volume")
                ret = log_volume_info_and_status(self.mnode, volume)
                self.assertTrue(ret, ("Logging volume info and status failed "
                                      "on volume %s", volume))
                g.log.info("Successful in logging volume info and status "
                           "of volume %s", volume)

                # Verify volume's all process are online for 60 sec
                g.log.info("Verifying volume's all process are online")
                ret = wait_for_volume_process_to_be_online(self.mnode,
                                                           volume, 60)
                self.assertTrue(ret, ("Volume %s : All process are not "
                                      "online", volume))
                g.log.info("Successfully verified volume %s processes "
                           "are online", volume)

                # check the type for the replicate volume
                volume_type_info_for_replicate_after_adding_bricks = \
                    get_volume_type_info(self.mnode, volume)
                volume_type_for_replicate_after_adding_bricks = \
                    (volume_type_info_for_replicate_after_adding_bricks
                     ['volume_type_info']['typeStr'])

                self.assertEquals(
                    volume_type_for_replicate_after_adding_bricks,
                    'Distributed-Replicate',
                    ("Replicate volume type is not converted to "
                     "Distributed-Replicate after adding bricks"))
                g.log.info("Replicate Volume is successfully converted to"
                           " Distributed-Replicate after adding bricks")

                # get the bricks for the volume after expanding
                bricks_list_after_expanding = get_all_bricks(self.mnode,
                                                             volume)
                g.log.info("Brick List after expanding "
                           "volume: %s", bricks_list_after_expanding)

                # validate the bricks present in volume info
                # with glustershd server volume file after adding bricks
                g.log.info("Starting parsing file %s", self.GLUSTERSHD)
                ret = do_bricks_exist_in_shd_volfile(
                    self.mnode,
                    volume,
                    bricks_list_after_expanding)

                self.assertTrue(ret, ("Brick List from volume info is "
                                      "different from glustershd server "
                                      "volume file after expanding bricks. "
                                      "Please check log file for details"))
                g.log.info("Brick List from volume info is same as from "
                           "glustershd server volume file after "
                           "expanding bricks.")

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, glustershd_pids_after_adding_bricks = \
            get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s"
                              % glustershd_pids_after_adding_bricks))
        g.log.info("Successful in getting Single self heal daemon process"
                   " on all nodes %s", nodes)

        self.assertNotEqual(glustershd_pids,
                            glustershd_pids_after_adding_bricks,
                            "Self Daemon process is same before and"
                            " after adding bricks")
        g.log.info("Self Heal Daemon Process is different before and "
                   "after adding bricks")
