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

""" Description:
        Test Cases in this module tests the self heal daemon process.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (
    bring_bricks_offline, bring_bricks_online,
    select_volume_bricks_to_bring_offline, get_online_bricks_list)
from glustolibs.gluster.heal_libs import (
    get_self_heal_daemon_pid, is_shd_daemonized,
    monitor_heal_completion, bring_self_heal_daemon_process_offline)
from glustolibs.gluster.heal_ops import (get_heal_info_summary,
                                         trigger_heal_full)
from glustolibs.io.utils import validate_io_procs
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.mount_ops import mount_volume, umount_volume


@runs_on([['replicated'], ['glusterfs']])
class SelfHealDaemonProcessTestsWithSingleVolume(GlusterBaseClass):
    """
    SelfHealDaemonProcessTestsWithSingleVolume contains tests which
    verifies the self-heal daemon process on a single volume
    """

    @classmethod
    def setUpClass(cls):
        """
        setup volume, mount volume and initialize necessary variables
        which is used in tests
        """

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Verify glustershd process releases its parent process
        ret = is_shd_daemonized(cls.servers)
        if not ret:
            raise ExecutionError("Self Heal Daemon process was still"
                                 " holding parent process.")
        g.log.info("Self Heal Daemon processes are online")

        # Upload script
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [cls.script_upload_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume and umount volume from client
        """
        # Stopping the volume
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDownClass
        cls.get_super_method(cls, 'tearDownClass')()

    def test_server_side_healing_happens_only_when_glustershd_running(self):
        """
        Test Script which verifies that the server side healing must happen
        only if the heal daemon is running on the node where source brick
        resides.

         * Create and start the Replicate volume
         * Check the glustershd processes - Only 1 glustershd should be listed
         * Bring down the bricks without affecting the cluster
         * Create files on volume
         * kill the glustershd on node where bricks is running
         * bring the bricks up which was killed in previous steps
         * check the heal info - heal info must show pending heal info, heal
           shouldn't happen since glustershd is down on source node
         * issue heal
         * trigger client side heal
         * heal should complete successfully
        """
        # pylint: disable=too-many-locals,too-many-statements,too-many-lines
        # Setting Volume options
        options = {"metadata-self-heal": "on",
                   "entry-self-heal": "on",
                   "data-self-heal": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # Check the self-heal daemon process
        ret, pids = get_self_heal_daemon_pid(self.servers)
        self.assertTrue(ret, ("Either No self heal daemon process found or "
                              "more than One self heal daemon process "
                              "found : %s" % pids))
        g.log.info("Successful in verifying self heal daemon process"
                   " on all nodes %s", self.servers)

        # Select the bricks to bring offline
        bricks_to_bring_offline = (select_volume_bricks_to_bring_offline
                                   (self.mnode, self.volname))
        g.log.info("Brick List to bring offline : %s", bricks_to_bring_offline)

        # Bring down the selected bricks
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, "Failed to bring down the bricks")
        g.log.info("Brought down the brick process "
                   "for %s", bricks_to_bring_offline)

        # Write files on all mounts
        all_mounts_procs, num_files_to_write = [], 100
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_files "
                   "-f %s --base-file-name file %s" % (self.script_upload_path,
                                                       num_files_to_write,
                                                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Get online bricks list
        online_bricks = get_online_bricks_list(self.mnode, self.volname)
        g.log.info("Online Bricks for volume %s : %s",
                   self.volname, online_bricks)

        # Get the nodes where bricks are running
        bring_offline_glustershd_nodes = []
        for brick in online_bricks:
            bring_offline_glustershd_nodes.append(brick.split(":")[0])
        g.log.info("self heal deamon on nodes %s to be killed",
                   bring_offline_glustershd_nodes)

        # Kill the self heal daemon process on nodes
        ret = bring_self_heal_daemon_process_offline(
            bring_offline_glustershd_nodes)
        self.assertTrue(ret, ("Unable to bring self heal daemon process"
                              " offline for nodes %s"
                              % bring_offline_glustershd_nodes))
        g.log.info("Sucessfully brought down self heal process for "
                   "nodes %s", bring_offline_glustershd_nodes)

        # Check the heal info
        heal_info = get_heal_info_summary(self.mnode, self.volname)
        g.log.info("Successfully got heal info %s for the volume %s",
                   heal_info, self.volname)

        # Bring bricks online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline, 'glusterd_restart')
        self.assertTrue(ret, ("Failed to bring bricks: %s online"
                              % bricks_to_bring_offline))

        # Issue heal
        ret = trigger_heal_full(self.mnode, self.volname)
        self.assertFalse(ret, ("Able to trigger heal on volume %s where "
                               "self heal daemon is not running"
                               % self.volname))
        g.log.info("Expected : Unable to trigger heal on volume %s where "
                   "self heal daemon is not running", self.volname)

        # Wait for 130 sec to heal
        ret = monitor_heal_completion(self.mnode, self.volname, 130)
        self.assertFalse(ret, ("Heal Completed on volume %s" % self.volname))
        g.log.info("Expected : Heal pending on volume %s", self.volname)

        # Check the heal info
        heal_info_after_triggering_heal = get_heal_info_summary(self.mnode,
                                                                self.volname)
        g.log.info("Successfully got heal info for the volume %s",
                   self.volname)

        # Compare with heal pending with the files wrote
        for node in online_bricks:
            self.assertGreaterEqual(
                int(heal_info_after_triggering_heal[node]['numberOfEntries']),
                num_files_to_write,
                ("Some of the files are healed from source bricks %s where "
                 "self heal daemon is not running" % node))
        g.log.info("EXPECTED: No files are healed from source bricks where "
                   "self heal daemon is not running")

        # Unmount and Mount volume again as volume options were set
        # after mounting the volume
        for mount_obj in self.mounts:
            ret, _, _ = umount_volume(mount_obj.client_system,
                                      mount_obj.mountpoint)
            self.assertEqual(ret, 0, "Failed to unmount %s"
                             % mount_obj.client_system)
            ret, _, _ = mount_volume(self.volname,
                                     mtype='glusterfs',
                                     mpoint=mount_obj.mountpoint,
                                     mserver=self.mnode,
                                     mclient=mount_obj.client_system)
            self.assertEqual(ret, 0, "Failed to mount %s"
                             % mount_obj.client_system)

        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s read %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "Reads failed on some of the clients")
        g.log.info("Reads successful on all mounts")

        # Wait for heal to complete
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "Unable to heal the pending entries")
        g.log.info("Successfully healed the pending entries for volume %s",
                   self.volname)
