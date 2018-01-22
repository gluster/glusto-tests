#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable=too-many-lines

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.gluster.brick_libs import (select_bricks_to_bring_offline,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain,
                                          is_shd_daemonized)
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'cifs', 'nfs']])
class TestSelfHeal(GlusterBaseClass):
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

        cls.counter = 1
        # int: Value of counter is used for dirname-start-num argument for
        # file_dir_ops.py create_deep_dirs_with_files.

        # The --dir-length argument value for file_dir_ops.py
        # create_deep_dirs_with_files is set to 10 (refer to the cmd in setUp
        # method). This means every mount will create
        # 10 top level dirs. For every mountpoint/testcase to create new set of
        # dirs, we are incrementing the counter by --dir-length value i.e 10
        # in this test suite.

        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

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

    def test_data_self_heal_daemon_off(self):
        """
        Test Data-Self-Heal (heal command)

        Description:
        - set the volume option
        "metadata-self-heal": "off"
        "entry-self-heal": "off"
        "data-self-heal": "off"
        - create IO
        - Get areequal before getting bricks offline
        - set the volume option
        "self-heal-daemon": "off"
        - bring down all bricks processes from selected set
        - Get areequal after getting bricks offline and compare with
        areequal before getting bricks offline
        - modify the data
        - bring bricks online
        - set the volume option
        "self-heal-daemon": "on"
        - check daemons and start healing
        - check if heal is completed
        - check for split-brain
        - add bricks
        - do rebalance
        - create 5k files
        - while creating files - kill bricks and bring bricks online one by one
        in cycle
        - validate IO
        """
        # pylint: disable=too-many-statements

        # Setting options
        g.log.info('Setting options...')
        options = {"metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "data-self-heal": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("python %s create_files -f 100 --fixed-file-size 1k %s"
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

        # Get areequal before getting bricks offline
        g.log.info('Getting areequal before getting bricks offline...')
        ret, result_before_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before getting bricks offline '
                   'is successful')

        # Setting options
        g.log.info('Setting options...')
        options = {"self-heal-daemon": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Option 'self-heal-daemon' is set to 'off' successfully")

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # Bring brick offline
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

        # Get areequal after getting bricks offline
        g.log.info('Getting areequal after getting bricks offline...')
        ret, result_after_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after getting bricks offline '
                   'is successful')

        # Checking areequals before bringing bricks offline
        # and after bringing bricks offline
        self.assertEqual(result_before_offline, result_after_offline,
                         'Checksums before and '
                         'after bringing bricks online are not equal')
        g.log.info('Checksums before and after bringing bricks online '
                   'are equal')

        # Modify the data
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Modifying data for %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("python %s create_files -f 100 --fixed-file-size 10k %s"
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

        # Bring brick online
        g.log.info('Bringing bricks %s online...', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Setting options
        g.log.info('Setting options...')
        options = {"self-heal-daemon": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Option 'self-heal-daemon' is set to 'on' successfully")

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

        # Wait for self-heal-daemons to be online
        g.log.info("Waiting for self-heal-daemons to be online")
        ret = is_shd_daemonized(self.all_servers)
        self.assertTrue(ret, "Either No self heal daemon process found")
        g.log.info("All self-heal-daemons are online")

        # Start healing
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not started')
        g.log.info('Healing is started')

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

        # Add bricks
        g.log.info("Start adding bricks to volume...")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume %s", self.volname))
        g.log.info("Expanding volume is successful on "
                   "volume %s", self.volname)

        # Do rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Failed to start rebalance')
        g.log.info('Rebalance is started')

        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Rebalance is not completed')
        g.log.info('Rebalance is completed successfully')

        # Create 1k files
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Modifying data for %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("python %s create_files -f 1000 %s"
                       % (self.script_upload_path, mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Kill all bricks in cycle
        bricks_list = get_all_bricks(self.mnode, self.volname)
        for brick in bricks_list:
            # Bring brick offline
            g.log.info('Bringing bricks %s offline', brick)
            ret = bring_bricks_offline(self.volname, [brick])
            self.assertTrue(ret, 'Failed to bring bricks %s offline' % brick)

            ret = are_bricks_offline(self.mnode, self.volname,
                                     [brick])
            self.assertTrue(ret, 'Bricks %s are not offline'
                            % brick)
            g.log.info('Bringing bricks %s offline is successful',
                       bricks_to_bring_offline)

            # Bring brick online
            g.log.info('Bringing bricks %s online...', brick)
            ret = bring_bricks_online(self.mnode, self.volname,
                                      [brick])
            self.assertTrue(ret, 'Failed to bring bricks %s online' %
                            bricks_to_bring_offline)
            g.log.info('Bringing bricks %s online is successful',
                       bricks_to_bring_offline)

            # Wait for volume processes to be online
            g.log.info("Wait for volume processes to be online")
            ret = wait_for_volume_process_to_be_online(self.mnode,
                                                       self.volname)
            self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                                  "be online", self.volname))
            g.log.info("Successful in waiting for volume %s processes to be "
                       "online", self.volname)

            # Verify volume's all process are online
            g.log.info("Verifying volume's all process are online")
            ret = verify_all_process_of_volume_are_online(self.mnode,
                                                          self.volname)
            self.assertTrue(ret, ("Volume %s : All process are not online"
                                  % self.volname))
            g.log.info("Volume %s : All process are online", self.volname)

            # Wait for self-heal-daemons to be online
            g.log.info("Waiting for self-heal-daemons to be online")
            ret = is_shd_daemonized(self.all_servers)
            self.assertTrue(ret, "Either No self heal daemon process found or"
                                 "more than one self heal daemon process"
                                 "found")
            g.log.info("All self-heal-daemons are online")

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

    def test_entry_self_heal_heal_command(self):
        """
        Test Entry-Self-Heal (heal command)

        Description:
        - set the volume option
        "metadata-self-heal": "off"
        "entry-self-heal": "off"
        "data-self-heal": "off"
        - create IO
        - get areequal before getting bricks offline
        - set the volume option
        "self-heal-daemon": "off"
        - bring down all bricks processes from selected set
        - get areequal after getting bricks offline and compare with
        arequal after bringing bricks offline
        - modify the data
        - get areequal before getting bricks online
        - bring bricks online
        - set the volume option
        "self-heal-daemon": "on"
        - check daemons and start healing
        - check if heal is completed
        - check for split-brain
        - get areequal after getting bricks online and compare with
        arequal before bringing bricks online
        """
        # pylint: disable=too-many-statements

        # Setting options
        g.log.info('Setting options...')
        options = {"metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "data-self-heal": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Options "
                   "'metadata-self-heal', "
                   "'entry-self-heal', "
                   "'data-self-heal', "
                   "are set to 'off'")

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-length 2 "
                   "--dir-depth 2 "
                   "--max-num-of-dirs 2 "
                   "--num-of-files 20 %s" % (self.script_upload_path,
                                             self.counter,
                                             mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10
            g.log.info("IO on %s:%s is started successfully",
                       mount_obj.client_system, mount_obj.mountpoint)
        self.io_validation_complete = False

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Command list to do different operations with data -
        # create, rename, copy and delete
        cmd_list = ["python %s create_files -f 20 %s",
                    "python %s mv -i '.trashcan' %s",
                    "python %s copy --dest-dir new_dir %s",
                    "python %s delete %s"]

        for cmd in cmd_list:
            # Get areequal before getting bricks offline
            g.log.info('Getting areequal before getting bricks offline...')
            ret, result_before_offline = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            g.log.info('Getting areequal before getting bricks offline '
                       'is successful')

            # Setting options
            g.log.info('Setting options...')
            options = {"self-heal-daemon": "off"}
            ret = set_volume_options(self.mnode, self.volname, options)
            self.assertTrue(ret, 'Failed to set options %s' % options)
            g.log.info("Option 'self-heal-daemon' "
                       "is set to 'off' successfully")

            # Select bricks to bring offline
            bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
                self.mnode, self.volname))
            bricks_to_bring_offline = filter(None, (
                bricks_to_bring_offline_dict['hot_tier_bricks'] +
                bricks_to_bring_offline_dict['cold_tier_bricks'] +
                bricks_to_bring_offline_dict['volume_bricks']))

            # Bring brick offline
            g.log.info('Bringing bricks %s offline...',
                       bricks_to_bring_offline)
            ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
            self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                            bricks_to_bring_offline)

            ret = are_bricks_offline(self.mnode, self.volname,
                                     bricks_to_bring_offline)
            self.assertTrue(ret, 'Bricks %s are not offline'
                            % bricks_to_bring_offline)
            g.log.info('Bringing bricks %s offline is successful',
                       bricks_to_bring_offline)

            # Get areequal after getting bricks offline
            g.log.info('Getting areequal after getting bricks offline...')
            ret, result_after_offline = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            g.log.info('Getting areequal after getting bricks offline '
                       'is successful')

            # Checking areequals before bringing bricks offline
            # and after bringing bricks offline
            self.assertEqual(result_before_offline, result_after_offline,
                             'Checksums are not equal')
            g.log.info('Checksums before bringing bricks offline '
                       'and after bringing bricks offline are equal')

            # Modify the data
            g.log.info("Start modifying IO on all mounts...")
            self.all_mounts_procs = []
            for mount_obj in self.mounts:
                g.log.info("Modifying IO on %s:%s", mount_obj.client_system,
                           mount_obj.mountpoint)
                cmd = cmd % (self.script_upload_path, mount_obj.mountpoint)
                proc = g.run_async(mount_obj.client_system, cmd,
                                   user=mount_obj.user)
                self.all_mounts_procs.append(proc)
                g.log.info("IO on %s:%s is modified successfully",
                           mount_obj.client_system, mount_obj.mountpoint)
            self.io_validation_complete = False

            # Validate IO
            g.log.info("Wait for IO to complete and validate IO ...")
            ret = validate_io_procs(self.all_mounts_procs, self.mounts)
            self.assertTrue(ret, "IO failed on some of the clients")
            self.io_validation_complete = True
            g.log.info("IO is successful on all mounts")

            # Get areequal before getting bricks online
            g.log.info('Getting areequal before getting bricks online...')
            ret, result_before_online = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            g.log.info('Getting areequal before getting bricks online '
                       'is successful')

            # List all files and dirs created
            g.log.info("List all files and directories:")
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

            # Bring brick online
            g.log.info('Bringing bricks %s online...',
                       bricks_to_bring_offline)
            ret = bring_bricks_online(self.mnode, self.volname,
                                      bricks_to_bring_offline)
            self.assertTrue(ret, 'Failed to bring bricks %s online'
                            % bricks_to_bring_offline)
            g.log.info('Bringing bricks %s online is successful',
                       bricks_to_bring_offline)

            # Setting options
            g.log.info('Setting options...')
            options = {"self-heal-daemon": "on"}
            ret = set_volume_options(self.mnode, self.volname, options)
            self.assertTrue(ret, 'Failed to set options %s' % options)
            g.log.info("Option 'self-heal-daemon' is set to 'on' successfully")

            # Wait for volume processes to be online
            g.log.info("Wait for volume processes to be online")
            ret = wait_for_volume_process_to_be_online(self.mnode,
                                                       self.volname)
            self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                                  "be online", self.volname))
            g.log.info("Successful in waiting for volume %s processes to be "
                       "online", self.volname)

            # Verify volume's all process are online
            g.log.info("Verifying volume's all process are online")
            ret = verify_all_process_of_volume_are_online(self.mnode,
                                                          self.volname)
            self.assertTrue(ret, ("Volume %s : All process are not online"
                                  % self.volname))
            g.log.info("Volume %s : All process are online", self.volname)

            # Wait for self-heal-daemons to be online
            g.log.info("Waiting for self-heal-daemons to be online")
            ret = is_shd_daemonized(self.all_servers)
            self.assertTrue(ret, "Either No self heal daemon process found")
            g.log.info("All self-heal-daemons are online")

            # Start healing
            ret = trigger_heal(self.mnode, self.volname)
            self.assertTrue(ret, 'Heal is not started')
            g.log.info('Healing is started')

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

            # Get areequal after getting bricks online
            g.log.info('Getting areequal after getting bricks online...')
            ret, result_after_online = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            g.log.info('Getting areequal after getting bricks online '
                       'is successful')

            # List all files and dirs created
            g.log.info("List all files and directories:")
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

            # Checking areequals before bringing bricks online
            # and after bringing bricks online
            self.assertEqual(result_before_online, result_after_online,
                             'Checksums are not equal')
            g.log.info('Checksums before bringing bricks online '
                       'and after bringing bricks online are equal')

    def test_data_self_heal_algorithm_diff_heal_command(self):
        """
        Test Volume Option - 'cluster.data-self-heal-algorithm' : 'diff'

        Description:
        - set the volume option
        "metadata-self-heal": "off"
        "entry-self-heal": "off"
        "data-self-heal": "off"
        "data-self-heal-algorithm": "diff"
        "self-heal-daemon": "off"
        - create IO
        - calculate areequal
        - bring down all bricks processes from selected set
        - modify the data
        - get areequal before getting bricks online
        - bring bricks online
        - expand volume by adding bricks to the volume
        - do rebalance
        - set the volume option "self-heal-daemon": "on" and check for daemons
        - start healing
        - check if heal is completed
        - check for split-brain
        - calculate areequal and compare with arequal before bringing bricks
        offline and after bringing bricks online
        """
        # pylint: disable=too-many-branches,too-many-statements
        # Setting options
        g.log.info('Setting options...')
        options = {"metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "data-self-heal": "off",
                   "data-self-heal-algorithm": "diff"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Options "
                   "'metadata-self-heal', "
                   "'entry-self-heal', "
                   "'data-self-heal', "
                   "'self-heal-daemon' "
                   "are set to 'off',"
                   "'data-self-heal-algorithm' "
                   "is set to 'diff' successfully")

        # Creating files on client side
        for mount_object in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            # Creating files
            command = ("python %s create_files -f 100 %s"
                       % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Setting options
        g.log.info('Setting options...')
        options = {"self-heal-daemon": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Option 'self-heal-daemon' is set to 'off' successfully")

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # Bring brick offline
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
        for mount_object in self.mounts:
            g.log.info("Modifying data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            command = ("python %s create_files -f 100 --fixed-file-size 1M %s"
                       % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Get areequal before getting bricks online
        g.log.info('Getting areequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before getting bricks online '
                   'is successful')

        # Bring brick online
        g.log.info('Bringing bricks %s online...', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

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

        # Expand volume by adding bricks to the volume
        g.log.info("Start adding bricks to volume...")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume when IO in "
                              "progress on volume %s", self.volname))
        g.log.info("Expanding volume is successful on volume %s", self.volname)
        self.bricks_list = get_all_bricks(self.mnode, self.volname)

        # Do rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Failed to start rebalance')
        g.log.info('Rebalance is started')

        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Rebalance is not completed')
        g.log.info('Rebalance is completed successfully')

        # Setting options
        g.log.info('Setting options...')
        options = {"self-heal-daemon": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Option 'self-heal-daemon' is set to 'on' successfully")

        # Wait for self-heal-daemons to be online
        g.log.info("Waiting for self-heal-daemons to be online")
        ret = is_shd_daemonized(self.all_servers)
        self.assertTrue(ret, "Either No self heal daemon process found")
        g.log.info("All self-heal-daemons are online")

        # Start healing
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not started')
        g.log.info('Healing is started')

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

        # Get areequal after getting bricks online
        g.log.info('Getting areequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after getting bricks online '
                   'is successful')

        # Checking areequals before bringing bricks offline
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums are not equal')
        g.log.info('Checksums are equal')

    def test_data_self_heal_algorithm_diff_default(self):
        """
        Test Volume Option - 'cluster.data-self-heal-algorithm' : 'diff'

        Description:
        - set the volume option "data-self-heal-algorithm" to value "diff"
        - create IO
        - bring down all bricks processes from selected set
        - modify the data
        - calculate areequal
        - bring bricks online
        - start healing
        - calculate areequal and compare with arequal before bringing bricks
        offline and after bringing bricks online
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Setting options
        g.log.info('Setting options "data-self-heal-algorithm": "diff"...')
        options = {"data-self-heal-algorithm": "diff"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Option 'data-self-heal-algorithm' is set to 'diff' "
                   "successfully")

        # Creating files on client side
        for mount_object in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            # Creating files
            command = ("python %s create_files -f 100 %s"
                       % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # Bring brick offline
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
        for mount_object in self.mounts:
            g.log.info("Modifying data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            command = ("python %s create_files -f 100 --fixed-file-size 1M %s"
                       % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Get areequal before getting bricks online
        g.log.info('Getting areequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before getting bricks online '
                   'is successful')

        # Bring brick online
        g.log.info('Bringing bricks %s online...', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

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

        # Wait for self-heal-daemons to be online
        g.log.info("Waiting for self-heal-daemons to be online")
        ret = is_shd_daemonized(self.all_servers)
        self.assertTrue(ret, "Either No self heal daemon process found")
        g.log.info("All self-heal-daemons are online")

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

        # Get areequal after getting bricks online
        g.log.info('Getting areequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after getting bricks online '
                   'is successful')

        # Checking areequals before bringing bricks online
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums are not equal')
        g.log.info('Checksums before bringing bricks online '
                   'and after bringing bricks online are equal')

    def test_data_self_heal_algorithm_full_default(self):
        """
        Test Volume Option - 'cluster.data-self-heal-algorithm' : 'full'

        Description:
        - set the volume option "data-self-heal-algorithm" to value "full"
        - create IO
        - bring down all bricks processes from selected set
        - modify the data
        - calculate areequal
        - bring bricks online
        - start healing
        - calculate areequal and compare with arequal before bringing bricks
        offline and after bringing bricks online
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Setting options
        g.log.info('Setting options "data-self-heal-algorithm": "full"...')
        options = {"data-self-heal-algorithm": "full"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Option 'data-self-heal-algorithm' is set to 'full' "
                   "successfully")

        # Creating files on client side
        for mount_object in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            # Creating files
            command = ("python %s create_files -f 100 %s"
                       % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # Bring brick offline
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
        for mount_object in self.mounts:
            g.log.info("Modifying data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            command = ("python %s create_files -f 100 --fixed-file-size 1M %s"
                       % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Get areequal before getting bricks online
        g.log.info('Getting areequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before getting bricks online '
                   'is successful')

        # Bring brick online
        g.log.info('Bringing bricks %s online...', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

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

        # Wait for self-heal-daemons to be online
        g.log.info("Waiting for self-heal-daemons to be online")
        ret = is_shd_daemonized(self.all_servers)
        self.assertTrue(ret, "Either No self heal daemon process found")
        g.log.info("All self-heal-daemons are online")

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

        # Get areequal after getting bricks online
        g.log.info('Getting areequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after getting bricks online '
                   'is successful')

        # Checking areequals before bringing bricks online
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums are not equal')
        g.log.info('Checksums before bringing bricks online '
                   'and after bringing bricks online are equal')

    def test_self_heal_differing_in_file_type(self):
        """
        testing self heal of files with different file types
        with default configuration

        Description:
        - create IO
        - calculate areequal
        - bring down all bricks processes from selected set
        - calculate arequal and compare with arequal before
        getting bricks offline
        - modify the data
        - areequal before getting bricks online
        - bring bricks online
        - check daemons and healing completion
        - start healing
        - calculate areequal and compare with arequal before bringing bricks
        online and after bringing bricks online
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Creating files on client side
        test_file_type_differs_self_heal_folder = \
            'test_file_type_differs_self_heal'
        for mount_object in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)

            # Creating files
            command = ("cd %s/ ; "
                       "mkdir %s ;"
                       "cd %s/ ;"
                       "for i in `seq 1 10` ; "
                       "do mkdir l1_dir.$i ; "
                       "for j in `seq 1 5` ; "
                       "do mkdir l1_dir.$i/l2_dir.$j ; "
                       "for k in `seq 1 10` ; "
                       "do dd if=/dev/urandom of=l1_dir.$i/l2_dir.$j/test.$k "
                       "bs=1k count=$k ; "
                       "done ; "
                       "done ; "
                       "done ; "
                       % (mount_object.mountpoint,
                          test_file_type_differs_self_heal_folder,
                          test_file_type_differs_self_heal_folder))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Get areequal before getting bricks offline
        g.log.info('Getting areequal before getting bricks offline...')
        ret, result_before_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before getting bricks offline '
                   'is successful')

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # Bring brick offline
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

        # Get areequal after getting bricks offline
        g.log.info('Getting areequal after getting bricks offline...')
        ret, result_after_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after getting bricks offline '
                   'is successful')

        # Checking areequals before bringing bricks offline
        # and after bringing bricks offline
        self.assertItemsEqual(result_before_offline, result_after_offline,
                              'Checksums before and after '
                              'bringing bricks offline are not equal')
        g.log.info('Checksums before and after '
                   'bringing bricks offline are equal')

        # Modify the data
        self.all_mounts_procs = []
        for mount_object in self.mounts:
            g.log.info("Modifying data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            command = ("cd %s/%s/ ; "
                       "for i in `seq 1 10` ; "
                       "do for j in `seq 1 5` ; "
                       "do for k in `seq 1 10` ; "
                       "do rm -f l1_dir.$i/l2_dir.$j/test.$k ; "
                       "mkdir l1_dir.$i/l2_dir.$j/test.$k ; "
                       "done ; "
                       "done ; "
                       "done ;"
                       % (mount_object.mountpoint,
                          test_file_type_differs_self_heal_folder))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Get areequal before getting bricks online
        g.log.info('Getting areequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before getting bricks online '
                   'is successful')

        # Bring brick online
        g.log.info('Bringing bricks %s online', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

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

        # Wait for self-heal-daemons to be online
        g.log.info("Waiting for self-heal-daemons to be online")
        ret = is_shd_daemonized(self.all_servers)
        self.assertTrue(ret, "Either No self heal daemon process found")
        g.log.info("All self-heal-daemons are online")

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

        # Get areequal after getting bricks online
        g.log.info('Getting areequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after getting bricks online '
                   'is successful')

        # Checking areequals before bringing bricks online
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums before and '
                              'after bringing bricks online are not equal')
        g.log.info('Checksums before and after bringing bricks online '
                   'are equal')

    def test_self_heal_symbolic_links(self):
        """
        Test Self-Heal of Symbolic Links (heal command)

        Description:
        - set the volume option
        "metadata-self-heal": "off"
        "entry-self-heal": "off"
        "data-self-heal": "off"
        "data-self-heal-algorithm": "diff"
        "self-heal-daemon": "off"
        - create IO
        - calculate areequal
        - bring down all bricks processes from selected set
        - calculate areequals and compare with arequal
        before bringing bricks offline
        - modify the data and verify whether the links are properly created
        - calculate areequal before getting bricks online
        - bring bricks online
        - set the volume option
        "self-heal-daemon": "on"
        - check daemons and start healing
        - check is heal is complited
        - check for split-brain
        - calculate areequal after getting bricks online and compare with
        areequal before getting bricks online
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Setting options
        g.log.info('Setting options...')
        options = {"metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "data-self-heal": "off",
                   "self-heal-daemon": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Options "
                   "'metadata-self-heal', "
                   "'entry-self-heal', "
                   "'data-self-heal', "
                   "'self-heal-daemon' "
                   "are set to 'off' successfully")

        # Creating files on client side
        test_sym_link_self_heal_folder = 'test_sym_link_self_heal'
        for mount_object in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            # Creating files
            command = ("cd %s/ ; "
                       "mkdir %s ; "
                       "cd %s/ ;"
                       "for i in `seq 1 5` ; "
                       "do mkdir dir.$i ; "
                       "for j in `seq 1 10` ; "
                       "do dd if=/dev/urandom of=dir.$i/file.$j "
                       "bs=1k count=$j ; "
                       "done ; "
                       "done ;"
                       % (mount_object.mountpoint,
                          test_sym_link_self_heal_folder,
                          test_sym_link_self_heal_folder))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Get areequal before getting bricks offline
        g.log.info('Getting areequal before getting bricks offline...')
        ret, result_before_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before getting bricks offline '
                   'is successful')

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # Bring brick offline
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

        # Get areequal after getting bricks offline
        g.log.info('Getting areequal after getting bricks offline...')
        ret, result_after_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after getting bricks offline '
                   'is successful')

        # Checking areequals before bringing bricks offline
        # and after bringing bricks offline
        self.assertItemsEqual(result_before_offline, result_after_offline,
                              'Checksums before and '
                              'after bringing bricks online are not equal')
        g.log.info('Checksums before and after bringing bricks online '
                   'are equal')

        # Modify the data
        for mount_object in self.mounts:
            g.log.info("Modifying data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            # Create symlinks
            g.log.info('Creating symlinks...')
            command = ("cd %s/%s/ ; "
                       "for i in `seq 1 5` ; "
                       "do ln -s dir.$i sym_link_dir.$i ; "
                       "done ;"
                       % (mount_object.mountpoint,
                          test_sym_link_self_heal_folder))
            ret, _, _ = g.run(mount_object.client_system, command)
            self.assertEqual(ret, 0, 'Failed to modify the data for %s...'
                             % mount_object.mountpoint)
            g.log.info('Modifying the data for %s is successful',
                       mount_object.mountpoint)

            # Verify whether the links are properly created
            # Get symlink list
            command = ("cd %s/%s/ ; "
                       "ls |grep 'sym'"
                       % (mount_object.mountpoint,
                          test_sym_link_self_heal_folder))
            _, out, _ = g.run(mount_object.client_system, command)
            symlink_list = out.strip().split('\n')

            # Get folder list
            command = ("cd %s/%s/ ; "
                       "ls |grep -v 'sym'"
                       % (mount_object.mountpoint,
                          test_sym_link_self_heal_folder))
            _, out, _ = g.run(mount_object.client_system, command)
            folder_list = out.strip().split('\n')

            # Compare symlinks and folders
            for symlink in symlink_list:
                symlink_index = symlink_list.index(symlink)
                command = ("cd %s/%s/ ; "
                           "readlink %s"
                           % (mount_object.mountpoint,
                              test_sym_link_self_heal_folder,
                              symlink))
                _, out, _ = g.run(mount_object.client_system, command)
                symlink_to_folder = out.strip()
                self.assertEqual(symlink_to_folder, folder_list[symlink_index],
                                 'Links are not properly created')
                g.log.info('Links for %s are properly created',
                           mount_object.mountpoint)

        # Get areequal before getting bricks online
        g.log.info('Getting areequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before getting bricks online '
                   'is successful')

        # Bring brick online
        g.log.info('Bringing bricks %s online', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Setting options
        g.log.info('Setting options...')
        options = {"self-heal-daemon": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Option 'self-heal-daemon' is set to 'on' successfully")

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

        # Wait for self-heal-daemons to be online
        g.log.info("Waiting for self-heal-daemons to be online")
        ret = is_shd_daemonized(self.all_servers)
        self.assertTrue(ret, "Either No self heal daemon process found")
        g.log.info("All self-heal-daemons are online")

        # Start healing
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not started')
        g.log.info('Healing is started')

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

        # Get areequal after getting bricks online
        g.log.info('Getting areequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal after getting bricks online '
                   'is successful')

        # Checking areequals before bringing bricks online
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums before and '
                              'after bringing bricks online are not equal')
        g.log.info('Checksums before and after bringing bricks online '
                   'are equal')
