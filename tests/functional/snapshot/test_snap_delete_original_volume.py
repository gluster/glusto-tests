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

"""
Description:

Test Cases in this module tests the
Creation of clone from snapshot of volume
and delete snapshot and original volume.
Validate cloned volume is not affected.

"""
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.volume_ops import (get_volume_info, volume_status,
                                           volume_list, volume_start)
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.mount_ops import umount_volume
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_list,
                                         snap_activate,
                                         snap_clone)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs']])
class SnapshotSelfheal(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

        cls.clone = "clone1"
        cls.mpoint = "/mnt/clone1"
        cls.snap = "snap1"
        # Uploading file_dir script in all client direcotries
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")
        g.log.info("Successfully uploaded IO scripts to clients ")

        g.log.info("Starting to SetUp Volume")
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % cls.volname)
        g.log.info("Volume %s has been setup successfully", cls.volname)

    def test_snap_del_original_volume(self):
        # pylint: disable=too-many-statements
        """
        Steps:
        1. Create and mount distributed-replicated volume
        2. Perform I/O on mountpoints
        3. Create snapshot
        4. activate snapshot created in step3
        5. clone created snapshot in step3
        6. delete original volume
        7. Validate clone volume

        """
        # Perform I/O
        g.log.info("Starting to Perform I/O")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:"
                       "%s", mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("/usr/bin/env python%d %s create_files -f 100 "
                       "--fixed-file-size 1k %s" % (
                           sys.version_info.major, self.script_upload_path,
                           mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Creating snapshot
        g.log.info("Starting to Create snapshot")
        ret, _, _ = snap_create(self.mnode, self.volname, self.snap)
        self.assertEqual(ret, 0, ("Failed to create snapshot %s for "
                                  "volume %s"
                                  % (self.snap, self.volname)))
        g.log.info("Snapshot %s created successfully for volume "
                   "%s", self.snap, self.volname)

        # Activating snapshot
        g.log.info("Starting to Activate Snapshot")
        ret, _, _ = snap_activate(self.mnode, self.snap)
        self.assertEqual(ret, 0, ("Failed to Activate snapshot "
                                  "%s" % self.snap))
        g.log.info("Snapshot %s activated successfully", self.snap)

        # snapshot list
        g.log.info("getting snapshot list")
        ret, out, _ = snap_list(self.mnode)
        self.assertEqual(ret, 0, ("Failed to list snapshot of volume %s"
                                  % self.volname))
        self.assertIn(self.snap, out, "Failed to validate snapshot"
                      " %s in snap list" % self.snap)
        g.log.info("Snapshot list command for volume %s is "
                   "successful", self.volname)

        # Creating a Clone of snapshot:
        g.log.info("Starting to create Clone of Snapshot")
        ret, _, _ = snap_clone(self.mnode, self.snap, self.clone)
        self.assertEqual(ret, 0, ("Failed to create clone volume %s "
                                  "from snapshot %s"
                                  % (self.clone, self.snap)))
        g.log.info("Clone Volume %s created successfully from snapshot "
                   "%s", self.clone, self.snap)

        # Validate clone volumes are started:
        g.log.info("starting to Validate clone volumes are started")
        ret, _, _ = volume_start(self.mnode, self.clone)
        self.assertEqual(ret, 0, ("Failed to start cloned volume "
                                  "%s" % self.clone))
        g.log.info("Volume %s started successfully", self.clone)

        # Delete original volume
        g.log.info("deleting original volume")
        ret = cleanup_volume(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to delete volume %s"
                              % self.volname))
        g.log.info("successfully deleted volume %s", self.volname)

        # get volume info
        g.log.info("Getting and validating cloned volume %s", self.clone)
        vol_info = get_volume_info(self.mnode, self.clone)
        self.assertIsNotNone(vol_info, "Failed to get volume info "
                             "for cloned volume %s" % self.clone)
        self.assertEqual(vol_info[self.clone]['statusStr'], 'Started',
                         "Unexpected: cloned volume is not started "
                         "%s " % self.clone)
        g.log.info("Volume %s is in Started state", self.clone)

        # Volume status
        g.log.info("Getting volume status")
        ret, out, _ = volume_status(self.mnode, self.clone)
        self.assertEqual(ret, 0, "Failed to get volume status for"
                         " %s" % self.clone)
        vol = out.strip().split("\n")
        vol1 = vol[0].strip().split(":")
        self.assertEqual(vol1[1], " %s" % self.clone, "Failed to "
                         "get volume status for volume %s" % self.clone)
        g.log.info("Volume Status is Successful for %s clone volume",
                   self.clone)

        # Volume list validate
        g.log.info("Starting to list volume")
        ret, vol_list, _ = volume_list(self.mnode)
        self.assertEqual(ret, 0, "Failed to get volume list")
        vol_list1 = vol_list.strip().split("\n")
        self.assertIn("%s" % self.clone, vol_list1, "Failed to validate "
                      "volume list for volume %s" % self.clone)
        g.log.info("Volume list validated Successfully for"
                   "volume %s", self.clone)

    def tearDown(self):

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

        # Cleanup cloned volume
        g.log.info("Starting to delete cloned volume")
        ret = cleanup_volume(self.mnode, self.clone)
        if not ret:
            raise ExecutionError("Failed to delete the cloned volume")
        g.log.info("Successful in deleting Cloned volume")

        # Unmount Volume
        g.log.info("Starting to Unmount Volume")
        for mount_obj in self.mounts:
            ret = umount_volume(mount_obj.client_system, self.mpoint,
                                self.mount_type)
            if not ret:
                raise ExecutionError("Failed to umount the vol "
                                     "& cleanup Volume")
            g.log.info("Successful in umounting the volume and Cleanup")
