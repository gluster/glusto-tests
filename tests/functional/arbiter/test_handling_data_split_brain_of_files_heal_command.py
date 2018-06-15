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
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain)
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated'],
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

    def test_handling_data_split_brain(self):
        """
        - create IO
        - calculate arequal from mountpoint
        - set volume option 'self-heal-daemon' to value "off"
        - kill data brick1
        - calculate arequal checksum and compare it
        - modify files and directories
        - bring back all bricks processes online
        - kill data brick3
        - modify files and directories
        - calculate arequal from mountpoint
        - bring back all bricks processes online
        - run the find command to trigger heal from mountpoint
        - set volume option 'self-heal-daemon' to value "on"
        - check if heal is completed
        - check for split-brain
        - read files
        - calculate arequal checksum and compare it
        """
        # pylint: disable=too-many-locals,too-many-statements

        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("cd %s ; "
                       "for i in `seq 1 10` ; "
                       "do mkdir dir.$i ; "
                       "for j in `seq 1 5` ; "
                       "do dd if=/dev/urandom of=dir.$i/file.$j "
                       "bs=1K count=1 ; "
                       "done ; "
                       "dd if=/dev/urandom of=file.$i bs=1k count=1 ; "
                       "done"
                       % mount_obj.mountpoint)

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

        # Get arequal before getting bricks offline
        g.log.info('Getting arequal before getting bricks offline...')
        ret, result_before_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks offline '
                   'is successful')

        # Setting options
        options = {"self-heal-daemon": "off"}
        g.log.info('Setting options %s for volume %s',
                   options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Option 'self-heal-daemon' is set to 'off' successfully")

        # get the bricks for the volume
        g.log.info("Fetching bricks for the volume: %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick list: %s", bricks_list)

        # Bring brick 1 offline
        bricks_to_bring_offline = [bricks_list[0]]
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

        # Get arequal after getting bricks offline
        g.log.info('Getting arequal after getting bricks offline...')
        ret, result_after_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks offline '
                   'is successful')

        # Comparing arequals before getting bricks offline
        # and after getting bricks offline
        self.assertEqual(result_before_offline, result_after_offline,
                         'Arequals before getting bricks offline '
                         'and after getting bricks offline are not equal')
        g.log.info('Arequals before getting bricks offline '
                   'and after getting bricks offline are equal')

        # Modify the data
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Modifying data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Modify files
            g.log.info('Modifying files...')
            command = ("cd %s ; "
                       "for i in `seq 1 10` ; "
                       "do for j in `seq 1 5` ; "
                       "do dd if=/dev/urandom of=dir.$i/file.$j "
                       "bs=1M count=1 ; "
                       "done ; "
                       "dd if=/dev/urandom of=file.$i bs=1M count=1 ; "
                       "done"
                       % mount_obj.mountpoint)

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

        # Bring 1-st brick online
        g.log.info('Bringing bricks %s online...', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Bring brick 3rd offline
        bricks_to_bring_offline = [bricks_list[-1]]
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

        # Modify the data
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Modifying data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Modifying files...')
            command = ("cd %s ; "
                       "for i in `seq 1 10` ; "
                       "do for j in `seq 1 5` ; "
                       "do dd if=/dev/urandom of=dir.$i/file.$j "
                       "bs=1M count=1 ; "
                       "done ; "
                       "dd if=/dev/urandom of=file.$i bs=1M count=1 ; "
                       "done"
                       % mount_obj.mountpoint)

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

        # Get arequal before getting bricks online
        g.log.info('Getting arequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks online '
                   'is successful')

        # Bring 3rd brick online
        g.log.info('Bringing bricks %s online...', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Mount and unmount mounts
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, 'Failed to unmount %s' % self.volname)

        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, 'Unable to mount %s' % self.volname)

        # Start heal from mount point
        g.log.info('Starting heal from mount point...')
        for mount_obj in self.mounts:
            g.log.info("Start heal for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            command = ("python %s read %s"
                       % (self.script_upload_path,
                          self.mounts[0].mountpoint))
            ret, _, err = g.run(mount_obj.client_system, command)
            self.assertFalse(ret, err)
            g.log.info("Heal triggered for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
        g.log.info('Heal triggered for all mountpoints')

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

        # Reading files
        g.log.info('Reading files...')
        for mount_obj in self.mounts:
            g.log.info("Start reading files for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            command = ('cd %s/ ; '
                       'for i in `seq 1 10` ; '
                       'do cat file.$i > /dev/null ; '
                       'for j in `seq 1 5` ; '
                       'do cat dir.$i/file.$j > /dev/null ; '
                       'done ; done'
                       % mount_obj.mountpoint)
            ret, _, err = g.run(mount_obj.client_system, command)
            self.assertFalse(ret, err)
            g.log.info("Reading files successfully for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
        g.log.info('Reading files successfully for all mountpoints')

        # Get arequal after getting bricks online
        g.log.info('Getting arequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Comparing arequals before getting bricks online
        # and after getting bricks online
        self.assertEqual(result_before_online, result_after_online,
                         'Arequals before getting bricks online '
                         'and after getting bricks online are not equal')
        g.log.info('Arequals before getting bricks online '
                   'and after getting bricks online are equal')
