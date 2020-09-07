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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain,
                                          is_shd_daemon_running)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal,
                                 validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.gluster.gluster_init import (start_glusterd,
                                             stop_glusterd)
from glustolibs.misc.misc_libs import kill_process


@runs_on([['arbiter', 'distributed-arbiter'],
          ['glusterfs', 'nfs']])
class TestArbiterSelfHeal(GlusterBaseClass):
    """
    Description:
        Arbiter Test cases related to
        healing in default configuration of the volume
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [cls.script_upload_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Cleanup and umount volume
        """
        if not self.io_validation_complete:
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

            # List all files and dirs created
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")

        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_heal_full_after_deleting_files(self):
        """
        - Create IO
        - Calculate arequal from mount
        - kill glusterd process and glustershd process on arbiter nodes
        - Delete data from backend from the arbiter nodes
        - Start glusterd process and force start the volume
          to bring the processes online
        - Check if heal is completed
        - Check for split-brain
        - Calculate arequal checksum and compare it
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create dirs with file
            command = ("/usr/bin/env python %s create_deep_dirs_with_files "
                       "-d 2 -l 2 -n 2 -f 20 %s"
                       % (self.script_upload_path, mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Get arequal before killing gluster processes on arbiter node
        ret, result_before_killing_procs = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks offline '
                   'is successful')

        # Kill glusterd process and glustershd process on arbiter node
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        for subvol in subvols:
            arbiter = subvol[-1]
            node, brick_path = arbiter.split(':')
            # Stop glusterd
            ret = stop_glusterd(node)
            self.assertTrue(ret, "Failed to stop the glusterd on arbiter node")
            # Stop glustershd
            ret = kill_process(node, "glustershd")
            if not ret:
                # Validate glustershd process is not running
                self.assertFalse(
                    is_shd_daemon_running(self.mnode, node, self.volname),
                    "The glustershd process is still running.")
        g.log.info('Killed glusterd and glustershd for all arbiter '
                   'brick successfully')

        # Delete data from backend from the arbiter node
        for subvol in subvols:
            arbiter = subvol[-1]
            # Clearing the arbiter bricks
            node, brick_path = arbiter.split(':')
            ret, _, err = g.run(node, 'rm -rf %s/*' % brick_path)
            self.assertFalse(
                ret, err)
        g.log.info('Clearing for all arbiter brick is successful')

        # Start glusterd process on each arbiter
        for subvol in subvols:
            arbiter = subvol[-1]
            node, brick_path = arbiter.split(':')
            ret = start_glusterd(node)
            self.assertTrue(
                ret, "Failed to start glusterd on the arbiter node")

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')

        # Get arequal after healing
        ret, result_after_healing = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Comparing arequals before before killing arbiter processes
        # and after healing
        self.assertEqual(
            result_before_killing_procs, result_after_healing,
            'Arequals arequals before before killing arbiter '
            'processes and after healing are not equal')

        g.log.info('Arequals before killing arbiter '
                   'processes and after healing are equal')
