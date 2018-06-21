#  Copyright (C) 2015-2018  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_libs import (expand_volume, shrink_volume,
                                            log_volume_info_and_status)

from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal,
                                 validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)


@runs_on([['distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class TestArbiterSelfHeal(GlusterBaseClass):
    """
    Description:
        Arbiter Test cases related to
        healing in default configuration of the volume
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
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

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

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
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
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

            # List all files and dirs created
            g.log.info("List all files and directories:")
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    def test_remove_faulty_subvol_and_add_new_subvol(self):
        """
        - Create an distribute-replicate arbiter volume
        - Create IO
        - Calculate areequal
        - Remove a subvolume
        - Calculate areequal and compare with previous
        - Expand the volume
        - Do rebalance and check status
        - Calculate areequal and compare with previous
        """
        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create dirs with file
            g.log.info('Creating dirs with file...')
            command = ("python %s create_deep_dirs_with_files "
                       "-d 2 "
                       "-l 2 "
                       "-n 2 "
                       "-f 20 "
                       "%s"
                       % (self.script_upload_path, mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Get areequal before removing subvol
        g.log.info('Getting areequal before removing subvol...')
        ret, result_before_remove_subvol = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before removing subvol is successful')

        # Remove subvol
        g.log.info('Removing subvol...')
        ret = shrink_volume(self.mnode, self.volname, subvol_num=1)
        self.assertTrue(ret, 'Failed to remove subvolume')
        g.log.info('Removed subvolume sucsessfully')

        # Get areequal after removing subvol
        g.log.info('Getting areequal after removing subvol...')
        ret, result_after_remove_subvol = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after removing subvol is successful')

        # Comparing areequal before removing subvol
        # and after removing subvol
        self.assertEqual(result_before_remove_subvol,
                         result_after_remove_subvol,
                         'Areequals before removing subvol and  '
                         'after removing subvol are not equal')
        g.log.info('Areequals before removing subvol and  '
                   'after removing subvol are equal')

        # Expand volume
        g.log.info('Expanding volume %s...', self.volname)
        ret = expand_volume(self.mnode, self.volname,
                            self.servers, self.all_servers_info)
        self.assertTrue(ret, 'Failed to expand volume %s' % self.volname)
        g.log.info('Expanded volume %s sucsessfully', self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed "
                              "on volume %s", self.volname))
        g.log.info("Successful in logging volume info and status "
                   "of volume %s", self.volname)

        # Do rebalance
        g.log.info('Starting rebalance...')
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Failed to start rebalance')
        g.log.info('Rebalance is started')

        g.log.info('Waiting for rebalance to complete...')
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Rebalance is not completed')
        g.log.info('Rebalance is completed successfully')

        # Get areequal after expanding volume
        g.log.info('Getting areequal after expanding volume...')
        ret, result_after_expand_volume = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after expanding volume is successful')

        # Comparing areequal before removing subvol
        # and after expanding volume
        self.assertEqual(result_before_remove_subvol,
                         result_after_expand_volume,
                         'Areequals before removing subvol and  '
                         'after expanding volume are not equal')
        g.log.info('Areequals before removing subvol and  '
                   'after expanding volume are equal')
