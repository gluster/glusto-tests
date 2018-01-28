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

import time
import calendar
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (
    expand_volume, shrink_volume, log_volume_info_and_status,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete,
                                              rebalance_status)
from glustolibs.gluster.brick_libs import (
    get_all_bricks, bring_bricks_offline, bring_bricks_online,
    are_bricks_online, select_bricks_to_bring_offline, are_bricks_offline,
    select_volume_bricks_to_bring_offline, get_online_bricks_list)
from glustolibs.gluster.brick_ops import replace_brick
from glustolibs.gluster.heal_libs import (get_self_heal_daemon_pid,
                                          do_bricks_exist_in_shd_volfile,
                                          is_shd_daemonized,
                                          are_all_self_heal_daemons_are_online,
                                          monitor_heal_completion)
from glustolibs.gluster.heal_ops import get_heal_info_summary
from glustolibs.gluster.volume_ops import (volume_stop, volume_start)
from glustolibs.gluster.gluster_init import restart_glusterd
from glustolibs.io.utils import validate_io_procs
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'], ['glusterfs', 'nfs', 'cifs']])
class SelfHealDaemonProcessTests(GlusterBaseClass):
    """
    SelfHealDaemonProcessTests contains tests which verifies the
    self-heal daemon process of the nodes
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [script_local_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        setup volume, mount volume and initialize necessary variables
        which is used in tests
        """

        # calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUp.im_func(self)

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

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

        # stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # calling GlusterBaseClass tearDownClass
        GlusterBaseClass.tearDown.im_func(self)

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
        # pylint: disable=too-many-statements
        nodes = self.volume['servers']
        bricks_list = []
        glustershd_pids = {}

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s", pids))
        g.log.info("Successful in getting Single self heal daemon process"
                   " on all nodes %s", nodes)
        glustershd_pids = pids

        # get the bricks for the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # validate the bricks present in volume info with
        # glustershd server volume file
        g.log.info("Starting parsing file %s on "
                   "node %s", self.glustershd, self.mnode)
        ret = do_bricks_exist_in_shd_volfile(self.mnode, self.volname,
                                             bricks_list)
        self.assertTrue(ret, ("Brick List from volume info is different "
                              "from glustershd server volume file. "
                              "Please check log file for details"))
        g.log.info("Successfully parsed %s file", self.glustershd)

        # expanding volume
        g.log.info("Start adding bricks to volume %s", self.volname)
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
        ret, _, err = rebalance_start(self.mnode, self.volname)
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
                   "nodes %s", nodes)
        glustershd_pids_after_expanding = {}
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))
        g.log.info("Successfull in getting self-heal daemon process "
                   "on nodes %s", nodes)

        glustershd_pids_after_expanding = pids
        g.log.info("Self Heal Daemon Process ID's afetr expanding "
                   "volume: %s", glustershd_pids_after_expanding)

        self.assertNotEqual(glustershd_pids,
                            glustershd_pids_after_expanding,
                            "Self Daemon process is same before and"
                            " after adding bricks")
        g.log.info("Self Heal Daemon Process is different before and "
                   "after adding bricks")

        # get the bricks for the volume after expanding
        bricks_list_after_expanding = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List after expanding "
                   "volume: %s", bricks_list_after_expanding)

        # validate the bricks present in volume info
        # with glustershd server volume file after adding bricks
        g.log.info("Starting parsing file %s", self.glustershd)
        ret = do_bricks_exist_in_shd_volfile(self.mnode, self.volname,
                                             bricks_list_after_expanding)

        self.assertTrue(ret, ("Brick List from volume info is different "
                              "from glustershd server volume file after "
                              "expanding bricks. Please check log file "
                              "for details"))
        g.log.info("Successfully parsed %s file", self.glustershd)

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
                   "volume: %s", bricks_list_after_shrinking)

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
                   "on nodes %s", nodes)
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
        g.log.info("Starting parsing file %s", self.glustershd)
        ret = do_bricks_exist_in_shd_volfile(self.mnode, self.volname,
                                             bricks_list_after_shrinking)
        self.assertTrue(ret, ("Brick List from volume info is different "
                              "from glustershd server volume file after "
                              "removing bricks. Please check log file "
                              "for details"))
        g.log.info("Successfully parsed %s file", self.glustershd)

    def test_glustershd_with_restarting_glusterd(self):
        """
        Test Script to verify the self heal daemon process with restarting
        glusterd and rebooting the server

        * stop all volumes
        * restart glusterd - should not run self heal daemon process
        * start replicated involved volumes
        * single self heal daemon process running
        * restart glusterd
        * self heal daemon pid will change
        * bring down brick and restart glusterd
        * self heal daemon pid will change and its different from previous
        * brought up the brick

        """
        # pylint: disable=too-many-statements
        nodes = self.volume['servers']

        # stop the volume
        g.log.info("Stopping the volume %s", self.volname)
        ret = volume_stop(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to stop volume %s" % self.volname))
        g.log.info("Successfully stopped volume %s", self.volname)

        # check the self heal daemon process after stopping the volume
        g.log.info("Verifying the self heal daemon process for "
                   "volume %s", self.volname)
        ret = are_all_self_heal_daemons_are_online(self.mnode, self.volname)
        self.assertFalse(ret, ("Self Heal Daemon process is still running "
                               "even after stopping volume %s" % self.volname))
        g.log.info("Self Heal Daemon is not running after stopping  "
                   "volume %s", self.volname)

        # restart glusterd service on all the servers
        g.log.info("Restarting glusterd on all servers %s", nodes)
        ret = restart_glusterd(nodes)
        self.assertTrue(ret, ("Failed to restart glusterd on all nodes %s",
                              nodes))
        g.log.info("Successfully restarted glusterd on all nodes %s",
                   nodes)

        # check the self heal daemon process after restarting glusterd process
        g.log.info("Starting to get self-heal daemon process on"
                   " nodes %s", nodes)
        ret = are_all_self_heal_daemons_are_online(self.mnode, self.volname)
        self.assertFalse(ret, ("Self Heal Daemon process is running after "
                               "glusterd restart with volume %s in "
                               "stop state" % self.volname))
        g.log.info("Self Heal Daemon is not running after stopping  "
                   "volume and restarting glusterd %s", self.volname)

        # start the volume
        g.log.info("Starting the volume %s", self.volname)
        ret = volume_start(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to start volume %s" % self.volname))
        g.log.info("Volume %s started successfully", self.volname)

        # Verfiy glustershd process releases its parent process
        g.log.info("Checking whether glustershd process is daemonized or not")
        ret = is_shd_daemonized(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))
        g.log.info("Single self heal daemon process on all nodes %s", nodes)

        # get the self heal daemon pids after starting volume
        g.log.info("Starting to get self-heal daemon process "
                   "on nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))
        g.log.info("Succesfull in getting self heal daemon pids")
        glustershd_pids = pids

        # get the bricks for the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # validate the bricks present in volume info
        # with glustershd server volume file
        g.log.info("Starting parsing file %s on "
                   "node %s", self.glustershd, self.mnode)
        ret = do_bricks_exist_in_shd_volfile(self.mnode, self.volname,
                                             bricks_list)
        self.assertTrue(ret, ("Brick List from volume info is different from "
                              "glustershd server volume file. "
                              "Please check log file for details."))
        g.log.info("Successfully parsed %s file", self.glustershd)

        # restart glusterd service on all the servers
        g.log.info("Restarting glusterd on all servers %s", nodes)
        ret = restart_glusterd(nodes)
        self.assertTrue(ret, ("Failed to restart glusterd on all nodes %s",
                              nodes))
        g.log.info("Successfully restarted glusterd on all nodes %s",
                   nodes)

        # Verify volume's all process are online for 60 sec
        g.log.info("Verifying volume's all process are online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname,
                                                   60)
        self.assertTrue(ret, ("Volume %s : All process are not "
                              "online", self.volname))
        g.log.info("Successfully Verified volume %s processes are online",
                   self.volname)

        # Verfiy glustershd process releases its parent process
        ret = is_shd_daemonized(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))

        # check the self heal daemon process after starting volume and
        # restarting glusterd process
        g.log.info("Starting to get self-heal daemon process "
                   "on nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))
        glustershd_pids_after_glusterd_restart = pids

        self.assertNotEqual(glustershd_pids,
                            glustershd_pids_after_glusterd_restart,
                            ("Self Heal Daemon pids are same after "
                             "restarting glusterd process"))
        g.log.info("Self Heal Daemon process are different before and "
                   "after restarting glusterd process")

        # select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # bring bricks offline
        g.log.info("Going to bring down the brick process "
                   "for %s", bricks_to_bring_offline)
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                              "check the log file for more details."))
        g.log.info("Brought down the brick process "
                   "for %s succesfully", bricks_to_bring_offline)

        # restart glusterd after brought down the brick
        g.log.info("Restart glusterd on all servers %s", nodes)
        ret = restart_glusterd(nodes)
        self.assertTrue(ret, ("Failed to restart glusterd on all nodes %s",
                              nodes))
        g.log.info("Successfully restarted glusterd on all nodes %s",
                   nodes)

        # Verify volume's all process are online for 60 sec
        g.log.info("Verifying volume's all process are online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname,
                                                   60)
        self.assertTrue(ret, ("Volume %s : All process are not "
                              "online", self.volname))
        g.log.info("Successfully Verified volume %s processes are online",
                   self.volname)

        # Verfiy glustershd process releases its parent process
        ret = is_shd_daemonized(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))

        # check the self heal daemon process after killing brick and
        # restarting glusterd process
        g.log.info("Starting to get self-heal daemon process "
                   "on nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))
        glustershd_pids_after_killing_brick = pids

        self.assertNotEqual(glustershd_pids_after_glusterd_restart,
                            glustershd_pids_after_killing_brick,
                            ("Self Heal Daemon process are same from before "
                             "killing the brick,restarting glusterd process"))
        g.log.info("Self Heal Daemon process are different after killing the "
                   "brick, restarting the glusterd process")

        # brought the brick online
        g.log.info("bringing up the bricks : %s online",
                   bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, ("Failed to brought the bricks online"))
        g.log.info("Successfully brought the bricks online")

        # check all bricks are online
        g.log.info("Verifying all bricka are online or not.....")
        ret = are_bricks_online(self.mnode, self.volname,
                                bricks_to_bring_offline)
        self.assertTrue(ret, ("Not all bricks are online"))
        g.log.info("All bricks are online.")

    def test_brick_process_not_started_on_read_only_node_disks(self):
        """
        * create volume and start
        * kill one brick
        * start IO
        * unmount the brick directory from node
        * remount the brick directory with read-only option
        * start the volume with "force" option
        * check for error 'posix: initializing translator failed' in log file
        * remount the brick directory with read-write option
        * start the volume with "force" option
        * validate IO
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # Bring brick offline
        g.log.info('Bringing bricks %s offline...', bricks_to_bring_offline)
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Creating files for all volumes
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            cmd = ("python %s create_files -f 100 %s/test_dir"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # umount brick
        brick_node, volume_brick = bricks_to_bring_offline[0].split(':')
        node_brick = '/'.join(volume_brick.split('/')[0:3])
        g.log.info('Start umount brick %s...', node_brick)
        ret, _, _ = g.run(brick_node, 'umount %s' % node_brick)
        self.assertFalse(ret, 'Failed to umount brick %s' % node_brick)
        g.log.info('Successfully umounted %s', node_brick)

        # get time before remount the directory and checking logs for error
        g.log.info('Getting time before remount the directory and '
                   'checking logs for error...')
        _, time_before_checking_logs, _ = g.run(brick_node, 'date -u +%s')
        g.log.info('Time before remount the directory and checking logs - %s',
                   time_before_checking_logs)

        # remount the directory with read-only option
        g.log.info('Start remount brick %s with read-only option...',
                   node_brick)
        ret, _, _ = g.run(brick_node, 'mount -o ro %s' % node_brick)
        self.assertFalse(ret, 'Failed to remount brick %s' % node_brick)
        g.log.info('Successfully remounted %s with read-only option',
                   node_brick)

        # start volume with "force" option
        g.log.info('starting volume with "force" option...')
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertFalse(ret, 'Failed to start volume %s with "force" option'
                         % self.volname)
        g.log.info('Successfully started volume %s with "force" option',
                   self.volname)

        # check logs for an 'initializing translator failed' error
        g.log.info("Checking logs for an 'initializing translator failed' "
                   "error for %s brick...", node_brick)
        error_msg = 'posix: initializing translator failed'
        cmd = ("cat /var/log/glusterfs/bricks/bricks-%s-%s.log | "
               "grep '%s'"
               % (volume_brick.split('/')[-2], volume_brick.split('/')[-1],
                  error_msg))
        ret, log_msgs, _ = g.run(brick_node, cmd)
        log_msg = log_msgs.rstrip().split('\n')[-1]

        self.assertTrue(error_msg in log_msg, 'No errors in logs')
        g.log.info('EXPECTED: %s', error_msg)

        # get time from log message
        log_time_msg = log_msg.split('E')[0][1:-2].split('.')[0]
        log_time_msg_converted = calendar.timegm(time.strptime(
            log_time_msg, '%Y-%m-%d %H:%M:%S'))
        g.log.info('Time_msg from logs - %s ', log_time_msg)
        g.log.info('Time from logs - %s ', log_time_msg_converted)

        # get time after remount the directory checking logs for error
        g.log.info('Getting time after remount the directory and '
                   'checking logs for error...')
        _, time_after_checking_logs, _ = g.run(brick_node, 'date -u +%s')
        g.log.info('Time after remount the directory and checking logs - %s',
                   time_after_checking_logs)

        # check time periods
        g.log.info('Checking if an error is in right time period...')
        self.assertTrue(int(time_before_checking_logs) <=
                        int(log_time_msg_converted) <=
                        int(time_after_checking_logs),
                        'Expected error is not in right time period')
        g.log.info('Expected error is in right time period')

        # umount brick
        g.log.info('Start umount brick %s...', node_brick)
        ret, _, _ = g.run(brick_node, 'umount %s' % node_brick)
        self.assertFalse(ret, 'Failed to umount brick %s' % node_brick)
        g.log.info('Successfully umounted %s', node_brick)

        # remount the directory with read-write option
        g.log.info('Start remount brick %s with read-write option...',
                   node_brick)
        ret, _, _ = g.run(brick_node, 'mount %s' % node_brick)
        self.assertFalse(ret, 'Failed to remount brick %s' % node_brick)
        g.log.info('Successfully remounted %s with read-write option',
                   node_brick)

        # start volume with "force" option
        g.log.info('starting volume with "force" option...')
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertFalse(ret, 'Failed to start volume %s with "force" option'
                         % self.volname)
        g.log.info('Successfully started volume %s with "force" option',
                   self.volname)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class ImpactOfReplaceBrickForGlustershdTests(GlusterBaseClass):
    """
    ClientSideQuorumTests contains tests which verifies the
    client side quorum Test Cases
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Override Volumes
        if cls.volume_type == "distributed-replicated":
            # Define distributed-replicated volume
            cls.volume['voltype'] = {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 3,
                'arbiter_count': 1,
                'transport': 'tcp'}

        cls.glustershd = "/var/lib/glusterd/glustershd/glustershd-server.vol"

    def setUp(self):
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Cleanup and umount volume
        """
        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    def test_impact_of_replace_brick_for_glustershd(self):
        nodes = self.volume['servers']

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in getting Single self heal daemon process"
                   " on all nodes %s", nodes)
        glustershd_pids = pids

        # get the bricks for the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # validate the bricks present in volume info with
        # glustershd server volume file
        g.log.info("Starting parsing file %s on "
                   "node %s", self.glustershd, self.mnode)
        ret = do_bricks_exist_in_shd_volfile(self.mnode, self.volname,
                                             bricks_list)
        self.assertTrue(ret, ("Brick List from volume info is different "
                              "from glustershd server volume file. "
                              "Please check log file for details"))
        g.log.info("Successfully parsed %s file", self.glustershd)

        # replace brick
        brick_to_replace = bricks_list[-1]
        new_brick = brick_to_replace + 'new'
        g.log.info("Replacing the brick %s for the volume : %s",
                   brick_to_replace, self.volname)
        ret, _, err = replace_brick(self.mnode, self.volname,
                                    brick_to_replace, new_brick)
        self.assertFalse(ret, err)
        g.log.info('Replaced brick %s to %s successfully',
                   brick_to_replace, new_brick)

        # check bricks
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertEqual(bricks_list[-1], new_brick, 'Replaced brick and '
                                                     'new brick are not equal')

        # Verify volume's all process are online for 60 sec
        g.log.info("Verifying volume's all process are online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname,
                                                   timeout=60)
        self.assertTrue(ret, ("Volume %s : All process are not "
                              "online", self.volname))
        g.log.info("Successfully Verified volume %s processes are online",
                   self.volname)

        # Verify glustershd process releases its parent process
        ret = is_shd_daemonized(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in getting Single self heal daemon process"
                   " on all nodes %s", nodes)
        glustershd_pids_after_replacement = pids

        # Compare pids before and after replacing
        self.assertNotEqual(glustershd_pids,
                            glustershd_pids_after_replacement,
                            "Self Daemon process is same before and"
                            " after replacing bricks")
        g.log.info("Self Heal Daemon Process is different before and "
                   "after replacing bricks")

        # get the bricks for the volume after replacing
        bricks_list_after_replacing = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List after expanding "
                   "volume: %s", bricks_list_after_replacing)

        # validate the bricks present in volume info
        # with glustershd server volume file after replacing bricks
        g.log.info("Starting parsing file %s", self.glustershd)
        ret = do_bricks_exist_in_shd_volfile(self.mnode, self.volname,
                                             bricks_list_after_replacing)

        self.assertTrue(ret, ("Brick List from volume info is different "
                              "from glustershd server volume file after "
                              "replacing bricks. Please check log file "
                              "for details"))
        g.log.info("Successfully parsed %s file", self.glustershd)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class SelfHealDaemonProcessTestsWithHealing(GlusterBaseClass):
    """
    SelfHealDaemonProcessTestsWithHealing contains tests which verifies the
    self-heal daemon process with healing.
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
        g.log.info("Verifying Self Heal Daemon process is daemonized")
        ret = is_shd_daemonized(cls.servers)
        if not ret:
            raise ExecutionError("Self Heal Daemon process was still"
                                 " holding parent process.")
        g.log.info("Self Heal Daemon processes are online")

        # upload script
        script_abs_path = "/usr/share/glustolibs/io/scripts/file_dir_ops.py"
        cls.script_upload_path = "/usr/share/glustolibs/io/scripts/" \
                                 "file_dir_ops.py"

        ret = upload_scripts(cls.clients, script_abs_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

        cls.GLUSTERSHD = "/var/lib/glusterd/glustershd/glustershd-server.vol"

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

    def test_existing_glustershd_should_take_care_of_self_healing(self):
        """
        Test Script which verifies that the existing glustershd should take
        care of self healing

        * Create and start the Replicate volume
        * Check the glustershd processes - Note the pids
        * Bring down the One brick ( lets say brick1)  without affecting
          the cluster
        * Create 5000 files on volume
        * bring the brick1 up which was killed in previous steps
        * check the heal info - proactive self healing should start
        * Bring down brick1 again
        * wait for 60 sec and brought up the brick1
        * Check the glustershd processes - pids should be different
        * Monitor the heal till its complete

        """
        # pylint: disable=too-many-locals,too-many-lines,too-many-statements
        nodes = self.servers

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in getting Single self heal daemon process"
                   " on all nodes %s", nodes)
        glustershd_pids = pids

        # select the bricks to bring offline
        g.log.info("Selecting bricks to brought offline for volume %s",
                   self.volname)
        bricks_to_bring_offline = \
            select_volume_bricks_to_bring_offline(self.mnode,
                                                  self.volname)
        g.log.info("Brick List to bring offline : %s",
                   bricks_to_bring_offline)

        # Bring down the selected bricks
        g.log.info("Going to bring down the brick process "
                   "for %s", bricks_to_bring_offline)
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                              "check the log file for more details."))
        g.log.info("Brought down the brick process "
                   "for %s succesfully", bricks_to_bring_offline)

        # get the bricks which are running
        g.log.info("getting the brick list which are online")
        online_bricks = get_online_bricks_list(self.mnode, self.volname)
        g.log.info("Online Bricks for volume %s : %s",
                   self.volname, online_bricks)

        # write 1MB files to the mounts
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = "for i in `seq 1 5000`;do dd if=/dev/urandom " \
                  "of=%s/file_$i bs=1M count=1;done" % mount_obj.mountpoint
            g.log.info(cmd)
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating IO on mounts.....")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # check the heal info
        g.log.info("Get the pending heal info for the volume %s",
                   self.volname)
        heal_info = get_heal_info_summary(self.mnode, self.volname)
        g.log.info("Successfully got heal info for the volume %s",
                   self.volname)
        g.log.info("Heal Info for volume %s : %s", self.volname, heal_info)

        # Bring bricks online
        g.log.info("Bring bricks: %s online", bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline, 'glusterd_restart')
        self.assertTrue(ret, ("Failed to bring bricks: %s online"
                              % bricks_to_bring_offline))
        g.log.info("Successfully brought all bricks: %s online",
                   bricks_to_bring_offline)

        # Wait for 90 sec to start self healing
        time.sleep(90)

        # check the heal info
        g.log.info("Get the pending heal info for the volume %s",
                   self.volname)
        heal_info_after_brick_online = get_heal_info_summary(self.mnode,
                                                             self.volname)
        g.log.info("Successfully got heal info for the volume %s",
                   self.volname)
        g.log.info("Heal Info for volume %s : %s",
                   self.volname, heal_info_after_brick_online)

        # check heal pending is decreased
        flag = False
        for brick in online_bricks:
            if int(heal_info_after_brick_online[brick]['numberOfEntries'])\
                    < int(heal_info[brick]['numberOfEntries']):
                flag = True
                break

        self.assertTrue(flag, ("Pro-active self heal is not started"))
        g.log.info("Pro-active self heal is started")

        # bring down bricks again
        g.log.info("Going to bring down the brick process "
                   "for %s", bricks_to_bring_offline)
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                              "check the log file for more details."))
        g.log.info("Brought down the brick process "
                   "for %s succesfully", bricks_to_bring_offline)

        # wait for 60 sec and brought up the brick agian
        time.sleep(60)
        g.log.info("Bring bricks: %s online", bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline, 'glusterd_restart')
        self.assertTrue(ret, ("Failed to bring bricks: %s online"
                              % bricks_to_bring_offline))
        g.log.info("Successfully brought all bricks: %s online",
                   bricks_to_bring_offline)

        # Verfiy glustershd process releases its parent process
        ret = is_shd_daemonized(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process found"))

        # check the self-heal daemon process
        g.log.info("Starting to get self-heal daemon process on "
                   "nodes %s", nodes)
        ret, pids = get_self_heal_daemon_pid(nodes)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in getting Single self heal daemon process"
                   " on all nodes %s", nodes)
        glustershd_pids_after_bricks_online = pids

        # compare the glustershd pids
        self.assertNotEqual(glustershd_pids,
                            glustershd_pids_after_bricks_online,
                            ("self heal daemon process are same before and "
                             "after bringing up bricks online"))
        g.log.info("EXPECTED : self heal daemon process are different before "
                   "and after bringing up bricks online")

        # wait for heal to complete
        g.log.info("Monitoring the heal.....")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, ("Heal is not completed on volume %s"
                              % self.volname))
        g.log.info("Heal Completed on volume %s", self.volname)
