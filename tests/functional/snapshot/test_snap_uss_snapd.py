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

"""
    Description:
        Test Cases in this module tests the USS functionality
        before and after snapd is killed. validate snapd after
        volume is started with force option.
"""
from os import path
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.mount_ops import (mount_volume,
                                          is_mounted, unmount_mounts)
from glustolibs.gluster.volume_ops import (volume_start,
                                           get_volume_info,
                                           volume_stop)
from glustolibs.gluster.volume_libs import (log_volume_info_and_status,
                                            cleanup_volume)
from glustolibs.gluster.snap_ops import (get_snap_list,
                                         snap_create,
                                         snap_activate,
                                         snap_clone, terminate_snapd_on_node)
from glustolibs.gluster.uss_ops import (is_snapd_running, is_uss_enabled,
                                        enable_uss, disable_uss,
                                        uss_list_snaps)
from glustolibs.gluster.mount_ops import create_mount_objs
from glustolibs.io.utils import validate_io_procs, view_snaps_from_mount


@runs_on([['replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'distributed'],
          ['glusterfs']])
class SnapshotSnapdCloneVol(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        cls.mount1 = []
        cls.mpoint = "/mnt/clone1"
        cls.server_list = []
        cls.server_lists = []

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):

        self.snap = 'test_snap_clone_snapd-snap'
        self.clone_vol1 = 'clone-of-test_snap_clone_snapd-clone1'
        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def validate_snapd(self, check_condition=True):
        """ Validate snapd running """
        for server in self.server_list:
            ret = is_snapd_running(server, self.clone_vol1)
            if check_condition:
                self.assertTrue(
                    ret, "Unexpected: Snapd is Not running for "
                    "volume %s on node %s" % (self.clone_vol1, server))
                g.log.info(
                    "Snapd Running for volume %s "
                    "on node: %s", self.clone_vol1, server)
            else:
                self.assertFalse(
                    ret, "Unexpected: Snapd is running for"
                    "volume %s on node %s" % (self.clone_vol1, server))
                g.log.info("Expected: Snapd is not Running for volume"
                           " %s on node: %s", self.clone_vol1, server)

    def check_snaps(self):
        """ Check snapshots under .snaps folder """
        ret, _, _ = uss_list_snaps(self.clients[0], self.mpoint)
        self.assertEqual(ret, 0, "Unexpected: .snaps directory not found")
        g.log.info("Expected: .snaps directory is present")

    def validate_uss(self):
        """ Validate USS running """
        ret = is_uss_enabled(self.mnode, self.clone_vol1)
        self.assertTrue(ret, "USS is disabled in clone volume "
                        "%s" % self.clone_vol1)
        g.log.info("USS enabled in cloned Volume %s", self.clone_vol1)

    def validate_snaps(self):
        """ Validate snapshots under .snaps folder """
        for count in range(0, 40):
            ret = view_snaps_from_mount(self.mount1, self.snaps_list)
            if ret:
                break
            sleep(2)
            count += 1
        self.assertTrue(ret, "Failed to lists .snaps folder")
        g.log.info("Successfully validated snapshots from .snaps folder")

    def test_snap_clone_snapd(self):
        """
        Steps:

        1. create a volume
        2. Create a snapshots and activate
        3. Clone the snapshot and mount it
        4. Check for snapd daemon
        5. enable uss and validate snapd
        5. stop cloned volume
        6. Validate snapd
        7. start cloned volume
        8. validate snapd
        9. Create 5 more snapshot
        10. Validate total number of
            snapshots created.
        11. Activate 5 snapshots
        12. Enable USS
        13. Validate snapd
        14. kill snapd on all nodes
        15. validate snapd running
        16. force start clone volume
        17. validate snaps inside .snaps directory
        """
        # pylint: disable=too-many-statements, too-many-locals

        # Starting I/O
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_files "
                   "-f 10 --base-file-name file %s" % (
                       self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate I/O
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Creating snapshot
        ret, _, _ = snap_create(self.mnode, self.volname, self.snap)
        self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                  % self.volname))
        g.log.info("Snapshot %s created successfully for "
                   "volume %s", self.snap, self.volname)

        # Activating created snapshots
        ret, _, _ = snap_activate(self.mnode, self.snap)
        self.assertEqual(ret, 0, ("Failed to activate snapshot %s"
                                  % self.snap))
        g.log.info("Snapshot snap%s activated successfully", self.snap)

        # Snapshot list
        self.assertIsNotNone(
            get_snap_list(self.mnode), "Failed to list snapshot")
        g.log.info("Snapshot list command Successful")

        # Creating and starting a Clone of snapshot:
        ret, _, _ = snap_clone(self.mnode, self.snap, self.clone_vol1)
        self.assertEqual(ret, 0, "Failed to clone %s" % self.clone_vol1)
        g.log.info("Clone volume %s created successfully", self.clone_vol1)

        # Start the clone volumes
        ret, _, _ = volume_start(self.mnode, self.clone_vol1)
        self.assertEqual(ret, 0, "Failed to start %s" % self.clone_vol1)
        g.log.info("%s started successfully", self.clone_vol1)

        # Form server list
        brick_list = get_all_bricks(self.mnode, self.clone_vol1)
        for bricks in brick_list:
            self.server_lists.append(bricks.split(":")[0])
        self.server_list = list(set(self.server_lists))

        # Get volume info
        vol_info = get_volume_info(self.mnode, self.clone_vol1)
        self.assertIsNotNone(vol_info, "Failed to get vol info")
        g.log.info("Successfully in getting vol info")

        # Redefining mounts for cloned volume
        self.mount_points, self.mounts_dict_list = [], []
        for client in self.all_clients_info:
            mount = {
                'protocol': self.mount_type,
                'server': self.mnode,
                'volname': self.volname,
                'client': self.all_clients_info[client],
                'mountpoint': (path.join(
                    "%s" % self.mpoint)),
                'options': ''
            }
            self.mounts_dict_list.append(mount)
        self.mount1 = create_mount_objs(self.mounts_dict_list)
        self.mount_points.append(self.mpoint)
        g.log.info("Successfully made entry in self.mount1")

        # FUSE mount clone1 volume
        for mount_obj in self.mounts:
            ret, _, _ = mount_volume(self.clone_vol1, self.mount_type,
                                     self.mpoint,
                                     self.mnode, mount_obj.client_system)
            self.assertEqual(ret, 0, "Volume mount failed for clone1")
            g.log.info("%s mounted Successfully", self.clone_vol1)

            # Validate clone volume is mounted or not
            ret = is_mounted(self.clone_vol1, self.mpoint, self.mnode,
                             mount_obj.client_system, self.mount_type)
            self.assertTrue(ret, "Volume not mounted on mount point: "
                            "%s" % self.mpoint)
            g.log.info("Volume %s mounted on %s", self.clone_vol1, self.mpoint)

        # Log Cloned Volume information
        ret = log_volume_info_and_status(self.mnode, self.clone_vol1)
        self.assertTrue("Failed to Log Info and Status of Volume "
                        "%s" % self.clone_vol1)
        g.log.info("Successfully Logged Info and Status")

        # Validate snapd running on all nodes
        self.validate_snapd(check_condition=False)

        # Enable USS
        ret, _, _ = enable_uss(self.mnode, self.clone_vol1)
        self.assertEqual(ret, 0, "Failed to enable USS on cloned volume")
        g.log.info("Successfully enabled USS on Cloned volume")

        # Validate USS running
        self.validate_uss()

        # Validate snapd running on all nodes
        self.validate_snapd()

        # Stop cloned volume
        ret, _, _ = volume_stop(self.mnode, self.clone_vol1)
        self.assertEqual(ret, 0, "Failed to stop cloned volume "
                         "%s" % self.clone_vol1)
        g.log.info("Successfully Stopped Cloned volume %s", self.clone_vol1)

        # Validate snapd running on all nodes
        self.validate_snapd(check_condition=False)

        # Start cloned volume
        ret, _, _ = volume_start(self.mnode, self.clone_vol1)
        self.assertEqual(ret, 0, "Failed to start cloned volume"
                         " %s" % self.clone_vol1)
        g.log.info("Successfully started cloned volume"
                   " %s", self.clone_vol1)

        # Validate snapd running on all nodes
        self.validate_snapd()

        # Create 5 snapshots
        self.snaps_list = [('test_snap_clone_snapd-snap%s'
                            % i)for i in range(0, 5)]
        for snapname in self.snaps_list:
            ret, _, _ = snap_create(self.mnode, self.clone_vol1,
                                    snapname)
            self.assertEqual(ret, 0, ("Failed to create snapshot for volume"
                                      " %s" % self.clone_vol1))
            g.log.info("Snapshot %s created successfully for volume "
                       "%s", snapname, self.clone_vol1)

        # Validate USS running
        self.validate_uss()

        # Check snapshot under .snaps directory
        self.check_snaps()

        # Activate Snapshots
        for snapname in self.snaps_list:
            ret, _, _ = snap_activate(self.mnode, snapname)
            self.assertEqual(ret, 0, ("Failed to activate snapshot %s"
                                      % snapname))
            g.log.info("Snapshot %s activated "
                       "successfully", snapname)

        # Validate USS running
        self.validate_uss()

        # Validate snapshots under .snaps folder
        self.validate_snaps()

        # Kill snapd on node and validate snapd except management node
        for server in self.servers[1:]:
            ret, _, _ = terminate_snapd_on_node(server)
            self.assertEqual(ret, 0, "Failed to Kill snapd on node %s"
                             % server)
            g.log.info("snapd Killed Successfully on node %s", server)

            # Check snapd running
            ret = is_snapd_running(server, self.clone_vol1)
            self.assertTrue(ret, "Unexpected: Snapd running on node: "
                            "%s" % server)
            g.log.info("Expected: Snapd is not running on node:%s", server)

            # Check snapshots under .snaps folder
            g.log.info("Validating snapshots under .snaps")
            ret, _, _ = uss_list_snaps(self.clients[0], self.mpoint)
            self.assertEqual(ret, 0, "Target endpoint not connected")
            g.log.info("Successfully listed snapshots under .snaps")

        # Kill snapd in management node
        ret, _, _ = terminate_snapd_on_node(self.servers[0])
        self.assertEqual(ret, 0, "Failed to Kill snapd on node %s"
                         % self.servers[0])
        g.log.info("snapd Killed Successfully on node %s", self.servers[0])

        # Validate snapd running on all nodes
        self.validate_snapd(check_condition=False)

        # Validating snapshots under .snaps
        ret, _, _ = uss_list_snaps(self.clients[0], self.mpoint)
        self.assertNotEqual(ret, 0, "Unexpected: Successfully listed "
                            "snapshots under .snaps")
        g.log.info("Expected: Target endpoint not connected")

        # Start the Cloned volume(force start)
        ret, _, _ = volume_start(self.mnode, self.clone_vol1, force=True)
        self.assertEqual(ret, 0, "Failed to start cloned volume "
                         "%s" % self.clone_vol1)
        g.log.info("Successfully Started Cloned volume %s", self.clone_vol1)

        # Validate snapd running on all nodes
        self.validate_snapd()

        # Validate snapshots under .snaps folder
        self.validate_snaps()

    def tearDown(self):

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

        # Disable USS on cloned volume
        ret, _, _ = disable_uss(self.mnode, self.clone_vol1)
        if ret:
            raise ExecutionError("Failed to disable USS on cloned volume")
        g.log.info("Successfully disabled USS on Cloned volume")

        # Cleanup cloned volume
        ret = unmount_mounts(self.mount1)
        if not ret:
            raise ExecutionError("Failed to unmount cloned volume")
        ret = cleanup_volume(self.mnode, self.clone_vol1)
        if not ret:
            raise ExecutionError("Failed to unmount and cleanup cloned volume")
        g.log.info("Successfully umounted and cleanup cloned volume")

        # Unmount and cleanup-volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")
