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

"""
Description:

Test Cases in this module tests the
Creation of clone from snapshot of one volume.

"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.gluster.volume_ops import (get_volume_info,
                                           volume_reset, volume_start,
                                           set_volume_options,
                                           get_volume_options)
from glustolibs.gluster.volume_libs import (
    cleanup_volume, verify_all_process_of_volume_are_online)
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_activate, snap_list,
                                         snap_clone)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class CloneSnapshot(GlusterBaseClass):
    """
    CloneSnapTest contains tests which verifies snapshot clone,
    creating snapshot and performing I/O on mountpoints
    """

    def setUp(self):
        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_clone_delete_snap(self):
        """
        clone from snap of one volume
        * Create and Mount the volume
        * Enable some volume options
        * Creating 2 snapshots and activate
        * reset the volume
        * create a clone of snapshots created
        * Mount both the clones
        * Perform I/O on mount point
        * Check volume options of cloned volumes
        * Create snapshot of the cloned snapshot volume
        * cleanup snapshots and volumes
        """

        # pylint: disable=too-many-statements, too-many-locals
        # Enabling Volume options on the volume and validating
        g.log.info("Enabling volume options for volume %s ", self.volname)
        options = {" features.uss": "enable"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Failed to set volume options for volume %s"
                              % self.volname))
        g.log.info("Successfully set volume options"
                   "for volume %s", self.volname)

        # Validate feature.uss enabled or not
        g.log.info("Validating feature.uss is enabled")
        option = "features.uss"
        vol_option = get_volume_options(self.mnode, self.volname, option)
        self.assertEqual(vol_option['features.uss'], 'enable', "Failed"
                         " to validate "
                         "volume options")
        g.log.info("Successfully validated volume options"
                   "for volume %s", self.volname)

        # Creating snapshot
        g.log.info("Starting to Create snapshot")
        for snap_count in range(0, 2):
            ret, _, _ = snap_create(self.mnode, self.volname,
                                    "snap%s" % snap_count)
            self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                      % self.volname))
            g.log.info("Snapshot snap%s created successfully"
                       "for volume %s", snap_count, self.volname)

        # Activating snapshot
        g.log.info("Starting to Activate Snapshot")
        for snap_count in range(0, 2):
            ret, _, _ = snap_activate(self.mnode, "snap%s" % snap_count)
            self.assertEqual(ret, 0, ("Failed to Activate snapshot snap%s"
                                      % snap_count))
            g.log.info("Snapshot snap%s activated successfully", snap_count)

        # Reset volume:
        g.log.info("Starting to Reset Volume")
        ret, _, _ = volume_reset(self.mnode, self.volname, force=False)
        self.assertEqual(ret, 0, ("Failed to reset volume %s" % self.volname))
        g.log.info("Reset Volume on volume %s is Successful", self.volname)

        # Validate feature.uss enabled or not
        g.log.info("Validating feature.uss is enabled")
        option = "features.uss"
        vol_option = get_volume_options(self.mnode, self.volname, option)
        self.assertEqual(vol_option['features.uss'], 'off', "Failed"
                         " to validate "
                         "volume options")
        g.log.info("Successfully validated volume options"
                   "for volume %s", self.volname)

        # Verify volume's all process are online
        g.log.info("Starting to Verify volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are"
                              "not online" % self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Creating and starting a Clone of snapshot
        g.log.info("Starting to Clone Snapshot")
        for clone_count in range(0, 2):
            ret, _, _ = snap_clone(self.mnode, "snap%s" % clone_count,
                                   "clone%s" % clone_count)
            self.assertEqual(ret, 0, ("Failed to clone clone%s volume"
                                      % clone_count))
            g.log.info("clone%s volume created successfully", clone_count)

        # Start Cloned volume
        g.log.info("starting to Validate clone volumes are started")
        for clone_count in range(0, 2):
            ret, _, _ = volume_start(self.mnode, "clone%s" % clone_count)
            self.assertEqual(ret, 0, ("Failed to start clone%s"
                                      % clone_count))
            g.log.info("clone%s started successfully", clone_count)
        g.log.info("All the clone volumes are started Successfully")

        # Validate Volume start of cloned volume
        g.log.info("Starting to Validate Volume start")
        for clone_count in range(0, 2):
            vol_info = get_volume_info(self.mnode, "clone%s" % clone_count)
            if vol_info["clone%s" % clone_count]['statusStr'] != 'Started':
                raise ExecutionError("Failed to get volume info for clone%s"
                                     % clone_count)
            g.log.info("Volume clone%s is in Started state", clone_count)

        # Validate feature.uss enabled or not
        g.log.info("Validating feature.uss is enabled")
        option = "features.uss"
        for clone_count in range(0, 2):
            vol_option = get_volume_options(self.mnode, "clone%s"
                                            % clone_count, option)
            self.assertEqual(vol_option['features.uss'], 'enable', "Failed"
                             " to validate"
                             "volume options")
            g.log.info("Successfully validated volume options"
                       "for volume clone%s", clone_count)

        # Mount both the cloned volumes
        g.log.info("Mounting Cloned Volumes")
        for mount_obj in range(0, 2):
            self.mpoint = "/mnt/clone%s" % mount_obj
            cmd = "mkdir -p  %s" % self.mpoint
            ret, _, _ = g.run(self.clients[0], cmd)
            self.assertEqual(ret, 0, ("Creation of directory %s"
                                      "for mounting"
                                      "volume %s failed: Directory already"
                                      "present"
                                      % (self.mpoint, "clone%s" % mount_obj)))
            g.log.info("Creation of directory %s for mounting volume %s "
                       "success", self.mpoint, ("clone%s" % mount_obj))
            ret, _, _ = mount_volume("clone%s" % mount_obj, self.mount_type,
                                     self.mpoint, self.mnode, self.clients[0])
            self.assertEqual(ret, 0, ("clone%s is not mounted"
                                      % mount_obj))
            g.log.info("clone%s is mounted Successfully", mount_obj)

        # Perform I/O on mount
        # Start I/O on all mounts
        g.log.info("Starting to Perform I/O on Mountpoint")
        all_mounts_procs = []
        for mount_obj in range(0, 2):
            cmd = ("cd /mnt/clone%s/; for i in {1..10};"
                   "do touch file$i; done; cd;") % mount_obj
            proc = g.run(self.clients[0], cmd)
            all_mounts_procs.append(proc)
        g.log.info("I/O on mountpoint is successful")

        # create snapshot
        g.log.info("Starting to Create snapshot of clone volume")
        ret0, _, _ = snap_create(self.mnode, "clone0", "snap2")
        self.assertEqual(ret0, 0, "Failed to create the snapshot"
                         "snap2 from clone0")
        g.log.info("Snapshots snap2 created successfully from clone0")
        ret1, _, _ = snap_create(self.mnode, "clone1", "snap3")
        self.assertEqual(ret1, 0, "Failed to create the snapshot snap3"
                         "from clone1")
        g.log.info("Snapshots snap3 created successfully from clone1")

        # Listing all Snapshots present
        g.log.info("Starting to list all snapshots")
        ret, _, _ = snap_list(self.mnode)
        self.assertEqual(ret, 0, ("Failed to list snapshots present"))
        g.log.info("Snapshots successfully listed")

    def tearDown(self):

        # Unmount Cloned volume
        g.log.info("Starting to Unmount Cloned volume")
        for count in range(0, 2):
            self.mpoint = "/mnt/clone%s" % count
            ret, _, _ = umount_volume(self.clients[0], self.mpoint,
                                      self.mount_type)
            if ret == 1:
                raise ExecutionError("Unmounting the mount point %s failed"
                                     % self.mpoint)
            g.log.info("Mount point %s deleted successfully", self.mpoint)
        g.log.info("Unmount Volume Successful")

        # Cleanup Cloned Volumes
        g.log.info("Starting to cleanup cloned volumes")
        for clone_count in range(0, 2):
            ret = cleanup_volume(self.mnode, "clone%s" % clone_count)
            if not ret:
                raise ExecutionError("Failed to cleanup clone%s volume"
                                     % clone_count)
            g.log.info("Successful in clone%s volume cleanup", clone_count)

        # Unmount and cleanup-volume
        g.log.info("Starting to Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Cleanup Volume Successfully")
