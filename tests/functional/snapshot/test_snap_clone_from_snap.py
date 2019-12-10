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
Creation of clone volume from snapshot.
"""

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.volume_ops import (volume_start,
                                           get_volume_info, volume_list)
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_activate,
                                         snap_clone)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs', 'nfs', 'cifs']])
class SnapshotCloneValidate(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        cls.snap = "snap0"

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients ")
        g.log.info("Successfully uploaded IO scripts to clients %s")

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_snap_clone_validate(self):

        """
        CloneSnapTest contains tests which verifies Clone volume
        created from snapshot

        Steps:

        1. Create a volume
        2. Mount the volume
        3. Perform I/O on mount poit
        4. Create a snapshot
        5. Activate the snapshot created in step 4
        6. Create 10 clones from snapshot created in step 4
        7. Verify Information about the volumes
           along with the original volume.
        8. Validate total number of clone volumes and existing volume
           with volume list
        """

        # write files on all mounts
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python%d %s create_files "
                   "-f 10 --base-file-name file %s" % (
                       sys.version_info.major, self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run(self.clients[0], cmd)
            all_mounts_procs.append(proc)
        g.log.info("Successfully Performed I/O on all mount points")

        # Creating snapshot:
        g.log.info("Starting to Create snapshot")
        ret, _, _ = snap_create(self.mnode, self.volname, self.snap)
        self.assertEqual(ret, 0, ("Failed to create snapshot for volume %s"
                                  % self.volname))
        g.log.info("Snapshot snap1 created successfully for volume %s",
                   self.volname)

        # Activating snapshot
        g.log.info("Starting to Activate Snapshot")
        ret, _, _ = snap_activate(self.mnode, self.snap)
        self.assertEqual(ret, 0, ("Failed to Activate snapshot %s"
                                  % self.snap))
        g.log.info("Snapshot %s activated successfully", self.snap)

        # Creating and starting a Clone of snapshot:
        g.log.info("Starting to Clone Snapshot")
        for count in range(1, 11):
            self.clone = "clone%s" % count
            ret, _, _ = snap_clone(self.mnode, self.snap, self.clone)
            self.assertEqual(ret, 0, "Failed to clone %s" % self.clone)
            g.log.info("%s created successfully", self.clone)

        # Start clone volumes
        g.log.info("starting to Validate clone volumes are started")
        for count in range(1, 11):
            self.clone = "clone%s" % count
            ret, _, _ = volume_start(self.mnode, self.clone)
            self.assertEqual(ret, 0, ("Failed to start %s" % self.clone))
            g.log.info("%s started successfully", self.clone)

        # Validate Volume Started
        g.log.info("Validating volume started")
        for count in range(1, 11):
            self.clone = "clone%s" % count
            vol_info = get_volume_info(self.mnode, self.clone)
            if vol_info[self.clone]['statusStr'] != 'Started':
                raise ExecutionError("Volume %s failed to start" % self.clone)
            g.log.info("Volume %s is in Started state", self.clone)

        # validate with list information
        # with 10 clone volume and 1 existing volume
        g.log.info("Validating with list information")
        ret, out, _ = volume_list(self.mnode)
        vlist = out.strip().split('\n')
        self.assertEqual(len(vlist), 11, "Failed to validate volume list")
        g.log.info("Successfully validated volumes in list")

    def tearDown(self):

        # Cleanup and umount volume
        g.log.info("Starting to cleanup volume %s", self.clone)
        for count in range(1, 11):
            self.clone = "clone%s" % count
            ret = cleanup_volume(self.mnode, self.clone)
            if not ret:
                raise ExecutionError("Failed to Cleanup cloned volume")
            g.log.info("successfully Unmount Volume and Cleanup Volume")

        # Unmount and cleanup original volume
        g.log.info("Starting to unmount and cleanup volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount"
                                 "the volume & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")
