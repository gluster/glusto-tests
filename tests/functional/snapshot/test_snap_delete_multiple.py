#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

Test Cases in this module tests the
Creation of clone from snapshot of volume.

"""
import os

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.volume_libs import (log_volume_info_and_status,
                                            cleanup_volume)
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_list,
                                         snap_delete_all,
                                         set_snap_config,
                                         snap_clone)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.mount_ops import (mount_volume,
                                          is_mounted, create_mount_objs,
                                          umount_volume)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class SnapshotCloneDeleteMultiple(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        cls.snap1 = "snap1"
        cls.snap2 = "snap21"
        cls.clone1 = "clone1"
        cls.clone2 = "clone2"
        cls.mpoint1 = "/mnt/clone1"
        cls.mpoint2 = "/mnt/clone2"

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients ")
        g.log.info("Successfully uploaded IO scripts to clients %s")

    def setUp(self):

        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp Volume and mount volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_snap_delete_multiple(self):
        # pylint: disable=too-many-statements
        """
        Steps:

        1. create and mount volume
        2. Create 20 snapshots
        3. Clone one of the snapshot
        4. mount the clone volume
        5. Perform I/O on mounts
        6. Create 10 more snapshots
        7. create Clone volume from latest snapshot
           and Mount it
        8. Perform I/O
        9. Create 10 more snapshot
        10. Validate total number of
            snapshots created.
        11. Delete all created snapshots.

        """
        # Perform I/O
        def io_operation(name):
            g.log.info("Starting to Perform I/O")
            all_mounts_procs = []
            for mount_obj in self.mounts:
                g.log.info("Generating data for %s:%s",
                           mount_obj.client_system, mount_obj.mountpoint)
                # Create files
                g.log.info('Creating files...')
                fname = "{}-{}".format(mount_obj.client_system, name)
                command = ("/usr/bin/env python {} create_files -f 100 "
                           "--fixed-file-size 1k --base-file-name {}"
                           " {}".format(self.script_upload_path,
                                        fname, mount_obj.mountpoint))
                proc = g.run_async(mount_obj.client_system, command,
                                   user=mount_obj.user)
                all_mounts_procs.append(proc)
            self.io_validation_complete = False

            # Validate IO
            g.log.info("Wait for IO to complete and validate IO ...")
            ret = validate_io_procs(all_mounts_procs, self.mounts)
            self.assertTrue(ret, "IO failed on some of the clients")
            self.io_validation_complete = True
            g.log.info("IO is successful on all mounts")
            return 0

        # Enable Activate on create
        g.log.info("Enabling activate-on-create")
        option = {'activate-on-create': 'enable'}
        ret, _, _ = set_snap_config(self.mnode, option)
        self.assertEqual(ret, 0, ("Failed to set activateOnCreate"
                                  "config option"))
        g.log.info("Activate-on-Create config option Successfully set")

        def create_snap(value, volname, snap, clone, counter):
            # Creating snapshots
            g.log.info("Starting to Create snapshot")
            for snap_count in value:
                ret, _, _ = snap_create(self.mnode, volname,
                                        "snap%s" % snap_count)
                self.assertEqual(ret, 0, ("Failed to create "
                                          "snapshot for volume %s"
                                          % volname))
                g.log.info("Snapshot snap%s created successfully"
                           " for volume %s", snap_count, volname)

            # Validate snapshot list
            g.log.info("Starting to list all snapshots")
            ret, out, _ = snap_list(self.mnode)
            self.assertEqual(ret, 0, ("Failed to list snapshot of volume %s"
                                      % volname))
            v_list = out.strip().split('\n')
            self.assertEqual(len(v_list), counter, "Failed to validate "
                             "all snapshots")
            g.log.info("Snapshot listed and  Validated for volume %s"
                       " successfully", volname)
            if counter == 40:
                return 0

            # Creating a Clone of snapshot:
            g.log.info("Starting to Clone Snapshot")
            ret, _, _ = snap_clone(self.mnode, snap, clone)
            self.assertEqual(ret, 0, "Failed to clone %s" % clone)
            g.log.info("Clone volume %s created successfully", clone)

            # Start cloned volumes
            g.log.info("starting to Validate clone volumes are started")
            ret, _, _ = volume_start(self.mnode, clone)
            self.assertEqual(ret, 0, "Failed to start %s" % clone)
            g.log.info("%s started successfully", clone)

            # log Cloned Volume information
            g.log.info("Logging Volume info and Volume status")
            ret = log_volume_info_and_status(self.mnode, clone)
            self.assertTrue("Failed to Log Info and Status of Volume %s"
                            % clone)
            g.log.info("Successfully Logged Info and Status")
            return counter+10

        def mount_clone_and_io(clone, mpoint):
            # define mounts
            self.mount_points = []
            self.mounts_dict_list = []
            for client in self.all_clients_info:
                mount = {
                    'protocol': self.mount_type,
                    'server': self.mnode,
                    'volname': clone,
                    'client': self.all_clients_info[client],
                    'mountpoint': (os.path.join(
                        "%s" % mpoint)),
                    'options': ''
                }
                self.mounts_dict_list.append(mount)
            self.mounts1 = create_mount_objs(self.mounts_dict_list)
            g.log.info("Successfully made entry in self.mounts")
            # Mounting a volume
            g.log.info("Starting to mount volume")
            ret = mount_volume(clone, self.mount_type, mpoint,
                               self.mnode, self.clients[0])
            self.assertTrue(ret, "Volume mount failed for clone1")
            g.log.info("%s mounted Successfully", clone)

            # Checking volume mounted or not
            ret = is_mounted(clone, mpoint, self.mnode,
                             self.clients[0], self.mount_type)
            self.assertTrue(ret, "Volume not mounted on mount point: %s"
                            % mpoint)
            g.log.info("Volume %s mounted on %s", clone, mpoint)
            return 0

        value1 = list(range(0, 20))
        value2 = list(range(20, 30))
        value3 = list(range(30, 40))
        ret1 = create_snap(value1, self.volname, self.snap1,
                           self.clone1, counter=20)
        self.assertEqual(ret1, 30, "Failed")
        ret2 = mount_clone_and_io(self.clone1, self.mpoint1)
        self.assertEqual(ret2, 0, "Failed to mount volume")
        ret = io_operation("first")
        self.assertEqual(ret, 0, "Failed to perform io")
        ret3 = create_snap(value2, self.clone1, self.snap2,
                           self.clone2, ret1)
        self.assertEqual(ret3, 40, "Failed")
        ret4 = mount_clone_and_io(self.clone2, self.mpoint2)
        self.assertEqual(ret4, 0, "Failed to mount volume")
        ret = io_operation("second")
        self.assertEqual(ret, 0, "Failed to perform io")
        ret1 = create_snap(value3, self.clone2, self.snap2,
                           self.clone2, ret3)
        self.assertEqual(ret1, 0, "Failed to create snapshots")

    def tearDown(self):
        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

        # delete created snapshots
        g.log.info("starting to delete all created snapshots")
        ret, _, _ = snap_delete_all(self.mnode)
        self.assertEqual(ret, 0, "Failed to delete all snapshots")
        g.log.info("Successfully deleted all snapshots")

        # Disable Activate on create
        option = {'activate-on-create': 'disable'}
        ret, _, _ = set_snap_config(self.mnode, option)
        if ret != 0:
            raise ExecutionError("Failed to set activateOnCreate"
                                 "config option")
        g.log.info("ActivateOnCreate config option Successfully set")

        # umount clone volume
        g.log.info("Unmounting clone volume")
        ret, _, _ = umount_volume(self.clients[0], self.mpoint1)
        if ret != 0:
            raise ExecutionError("Failed to unmount clone "
                                 "volume %s" % self.clone1)
        g.log.info("Successfully unmounted clone volume %s", self.clone1)

        ret, _, _ = umount_volume(self.clients[0], self.mpoint2)
        if ret != 0:
            raise ExecutionError("Failed to unmount clone "
                                 "volume %s" % self.clone2)
        g.log.info("Successfully unmounted clone volume %s", self.clone2)

        # cleanup volume
        g.log.info("starting to cleanup volume")
        ret1 = cleanup_volume(self.mnode, self.clone1)
        ret2 = cleanup_volume(self.mnode, self.clone2)
        if not ret1:
            raise ExecutionError("Failed to cleanup %s clone "
                                 "volume" % self.clone1)
        if not ret2:
            raise ExecutionError("Failed to cleanup %s clone "
                                 "volume" % self.clone2)
        g.log.info("Successfully cleanedup cloned volumes")

        # Unmount and cleanup-volume
        g.log.info("Starting to Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")
