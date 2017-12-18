#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (
    expand_volume, shrink_volume, log_volume_info_and_status,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete,
                                              rebalance_status)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.heal_libs import (get_self_heal_daemon_pid,
                                          do_bricks_exist_in_shd_volfile,
                                          is_shd_daemonized)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'], ['glusterfs', 'nfs', 'cifs']])
class SelfHealDaemonProcessTests(GlusterBaseClass):
    """
    SelfHealDaemonProcessTests contains tests which verifies the
    self-heal daemon process of the nodes
    """
    @classmethod
    def setUpClass(cls):
        """
        setup volume, mount volume and initialize necessary variables
        which is used in tests
        """

        # calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Verfiy glustershd process releases its parent process
        ret = is_shd_daemonized(cls.servers)
        if not ret:
            raise ExecutionError("Self Heal Daemon process was still"
                                 " holding parent process.")
        g.log.info("Self Heal Daemon processes are online")

        cls.GLUSTERSHD = "/var/lib/glusterd/glustershd/glustershd-server.vol"

    def setUp(self):
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

    def tearDown(self):
        """
        tearDown for every test
        """

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume and umount volume from client
        """

        # stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # calling GlusterBaseClass tearDownClass
        GlusterBaseClass.tearDownClass.im_func(cls)

    def test_glustershd_with_add_remove_brick(self):
        """
        Test script to verify glustershd process with adding and
        removing bricks

        * check glustershd process - only 1 glustershd process should
          be running
        * bricks must be present in glustershd-server.vol file for
          the replicated involved volumes
        * Add bricks
        * check glustershd process - only 1 glustershd process should
          be running and its should be different from previous one
        * bricks which are added must present in glustershd-server.vol file
        * remove bricks
        * check glustershd process - only 1 glustershd process should
          be running and its different from previous one
        * bricks which are removed should not present
          in glustershd-server.vol file

        """

        nodes = self.volume['servers']
        bricks_list = []
        glustershd_pids = {}

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s" % nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in getting Single self heal daemon process"
                   " on all nodes %s", nodes)
        glustershd_pids = pids

        # get the bricks for the volume
        g.log.info("Fetching bricks for the volume : %s" % self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s" % bricks_list)

        # validate the bricks present in volume info with
        # glustershd server volume file
        g.log.info("Starting parsing file %s on "
                   "node %s" % (self.GLUSTERSHD, self.mnode))
        ret = do_bricks_exist_in_shd_volfile(self.mnode, self.volname,
                                             bricks_list)
        self.assertTrue(ret, ("Brick List from volume info is different "
                              "from glustershd server volume file. "
                              "Please check log file for details"))
        g.log.info("Successfully parsed %s file" % self.GLUSTERSHD)

        # expanding volume
        g.log.info("Start adding bricks to volume %s" % self.volname)
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to add bricks to "
                              "volume %s " % self.volname))
        g.log.info("Add brick successfull")

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed "
                              "on volume %s", self.volname))
        g.log.info("Successful in logging volume info and status "
                   "of volume %s", self.volname)

        # Verify volume's all process are online for 60 sec
        g.log.info("Verifying volume's all process are online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname,
                                                   60)
        self.assertTrue(ret, ("Volume %s : All process are not "
                              "online", self.volname))
        g.log.info("Successfully Verified volume %s processes are online",
                   self.volname)

        # Start Rebalance
        g.log.info("Starting Rebalance on the volume")
        ret, out, err = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on "
                                  "the volume %s with error %s" %
                                  (self.volname, err)))
        g.log.info("Successfully started rebalance on the "
                   "volume %s", self.volname)

        # Log Rebalance status
        g.log.info("Log Rebalance status")
        _, _, _ = rebalance_status(self.mnode, self.volname)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete "
                              "on the volume %s", self.volname))
        g.log.info("Rebalance is successfully complete on "
                   "the volume %s", self.volname)

        # Check Rebalance status after rebalance is complete
        g.log.info("Checking Rebalance status")
        ret, _, _ = rebalance_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to get rebalance status for "
                                  "the volume %s", self.volname))
        g.log.info("Successfully got rebalance status of the "
                   "volume %s", self.volname)

        # Check the self-heal daemon process after adding bricks
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s" % nodes)
        glustershd_pids_after_expanding = {}
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))
        g.log.info("Successfull in getting self-heal daemon process "
                   "on nodes %s" % nodes)

        glustershd_pids_after_expanding = pids
        g.log.info("Self Heal Daemon Process ID's afetr expanding "
                   "volume: %s" % glustershd_pids_after_expanding)

        self.assertNotEqual(glustershd_pids,
                            glustershd_pids_after_expanding,
                            "Self Daemon process is same before and"
                            " after adding bricks")
        g.log.info("Self Heal Daemon Process is different before and "
                   "after adding bricks")

        # get the bricks for the volume after expanding
        bricks_list_after_expanding = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List after expanding "
                   "volume: %s" % bricks_list_after_expanding)

        # validate the bricks present in volume info
        # with glustershd server volume file after adding bricks
        g.log.info("Starting parsing file %s" % self.GLUSTERSHD)
        ret = do_bricks_exist_in_shd_volfile(self.mnode, self.volname,
                                             bricks_list_after_expanding)

        self.assertTrue(ret, ("Brick List from volume info is different "
                              "from glustershd server volume file after "
                              "expanding bricks. Please check log file "
                              "for details"))
        g.log.info("Successfully parsed %s file" % self.GLUSTERSHD)

        # shrink the volume
        g.log.info("Starting volume shrink")
        ret = shrink_volume(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to shrink the volume on "
                              "volume %s", self.volname))
        g.log.info("Shrinking volume is successful on "
                   "volume %s", self.volname)

        # Log Volume Info and Status after shrinking the volume
        g.log.info("Logging volume info and Status after shrinking volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status "
                   "of volume %s", self.volname)

        # get the bricks after shrinking the volume
        bricks_list_after_shrinking = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List after shrinking "
                   "volume: %s" % bricks_list_after_shrinking)

        self.assertEqual(len(bricks_list_after_shrinking), len(bricks_list),
                         "Brick Count is mismatched after "
                         "shrinking the volume %s" % self.volname)
        g.log.info("Brick Count matched before before expanding "
                   "and after shrinking volume")

        # Verfiy glustershd process releases its parent process
        ret = is_shd_daemonized(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))

        # check the self-heal daemon process after removing bricks
        g.log.info("Starting to get self-heal daemon process "
                   "on nodes %s" % nodes)
        glustershd_pids_after_shrinking = {}
        ret, pids = get_self_heal_daemon_pid(nodes)
        glustershd_pids_after_shrinking = pids
        self.assertNotEqual(glustershd_pids_after_expanding,
                            glustershd_pids_after_shrinking,
                            "Self Heal Daemon process is same "
                            "after adding bricks and shrinking volume")
        g.log.info("Self Heal Daemon Process is different after adding bricks "
                   "and shrinking volume")

        # validate bricks present in volume info
        # with glustershd server volume file after removing bricks
        g.log.info("Starting parsing file %s" % self.GLUSTERSHD)
        ret = do_bricks_exist_in_shd_volfile(self.mnode, self.volname,
                                             bricks_list_after_shrinking)
        self.assertTrue(ret, ("Brick List from volume info is different "
                              "from glustershd server volume file after "
                              "removing bricks. Please check log file "
                              "for details"))
        g.log.info("Successfully parsed %s file" % self.GLUSTERSHD)
