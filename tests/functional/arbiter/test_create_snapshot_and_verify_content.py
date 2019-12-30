#  Copyright (C) 2015-2020  Red Hat, Inc. <http://www.redhat.com>
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

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.snap_ops import (snap_create, snap_restore)
from glustolibs.gluster.volume_ops import (volume_stop, volume_start)
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online,
    get_subvols)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (
    collect_mounts_arequal,
    validate_io_procs)


@runs_on([['distributed-replicated', 'replicated'],
          ['glusterfs', 'nfs']])
class TestArbiterSelfHeal(GlusterBaseClass):
    """
            Arbiter Self-Heal tests
    """
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volumes
        if self.volume_type == "distributed-replicated":
            self.volume_configs = []

            # Redefine distributed-replicated volume
            self.volume['voltype'] = {
                'type': 'distributed-replicated',
                'replica_count': 3,
                'dist_count': 2,
                'arbiter_count': 1,
                'transport': 'tcp'}

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        self.subvols = get_subvols(self.mnode, self.volname)['volume_subvols']

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Clear all brick folders before snapshot
        for subvol in self.subvols:
            for brick in subvol:
                g.log.info('Clearing brick %s', brick)
                node, brick_path = brick.split(':')
                ret, _, _ = g.run(node, 'rm -rf %s' % brick_path)
                if ret:
                    raise ExecutionError('Failed in clearing brick %s' % brick)
                g.log.info('Clearing brick %s is successful', brick)
        g.log.info('Clearing for all brick is successful')

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_create_snapshot_and_verify_content(self):
        """
        - Create an arbiter volume
        - Create IO
        - Calculate arequal of the mount point
        - Take a snapshot of the volume
        - Create new data on mount point
        - Restore the snapshot
        - Calculate arequal of the mount point
        - Compare arequals
        """
        # Creating files on client side
        g.log.info("Generating data for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        # Create dirs with file
        all_mounts_procs = []
        g.log.info('Creating dirs with file...')
        command = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "-d 2 -l 2 -n 2 -f 20 %s" % (
                       sys.version_info.major, self.script_upload_path,
                       self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients")

        # Get arequal before snapshot
        g.log.info('Getting arequal before snapshot...')
        ret, arequal_before_snapshot = collect_mounts_arequal(self.mounts[0])
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before snapshot is successful')

        # Create snapshot
        snapshot_name = 'testsnap'
        g.log.info("Creating snapshot %s ...", snapshot_name)
        ret, _, err = snap_create(self.mnode, self.volname, snapshot_name)
        self.assertEqual(ret, 0, err)
        g.log.info("Snapshot %s created successfully", snapshot_name)

        # Add files on client side
        g.log.info("Generating data for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        # Create dirs with file
        all_mounts_procs = []
        g.log.info('Adding dirs with file...')
        command = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "-d 2 -l 2 -n 2 -f 20 %s" % (
                       sys.version_info.major, self.script_upload_path,
                       self.mounts[0].mountpoint+'/new_files'))
        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients")

        # Stop the volume
        g.log.info("Stopping %s ...", self.volname)
        ret, _, err = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, err)
        g.log.info("Volume %s stopped successfully", self.volname)

        # Revert snapshot
        g.log.info("Reverting snapshot %s ...", snapshot_name)
        ret, _, err = snap_restore(self.mnode, snapshot_name)
        self.assertEqual(ret, 0, err)
        g.log.info("Snapshot %s restored successfully", snapshot_name)

        # Start the volume
        g.log.info("Starting %s ...", self.volname)
        ret, _, err = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, err)
        g.log.info("Volume %s started successfully", self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online"
                              % self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Get arequal after restoring snapshot
        g.log.info('Getting arequal after restoring snapshot...')
        ret, arequal_after_restoring = collect_mounts_arequal(self.mounts[0])
        self.assertTrue(ret, "Collecting arequal-checksum failed")

        # Checking arequals before creating and after restoring snapshot
        self.assertEqual(arequal_before_snapshot, arequal_after_restoring,
                         'Arequal before creating snapshot '
                         'and after restoring snapshot are not equal')
        g.log.info('Arequal before creating snapshot '
                   'and after restoring snapshot are equal')
