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
                                           get_all_bricks,
                                           wait_for_bricks_to_be_online)
from glustolibs.gluster.heal_libs import (
    wait_for_self_heal_daemons_to_be_online,
    monitor_heal_completion,
    is_heal_complete,
    is_volume_in_split_brain,
    is_shd_daemonized)
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs,
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
        - Get arequal before getting bricks offline
        - set the volume option
        "self-heal-daemon": "off"
        - bring down all bricks processes from selected set
        - Get areeual after getting bricks offline and compare with
        arequal before getting bricks offline
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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Get arequal before getting bricks offline
        g.log.info('Getting arequal before getting bricks offline...')
        ret, result_before_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks offline '
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

        # Get arequal after getting bricks offline
        g.log.info('Getting arequal after getting bricks offline...')
        ret, result_after_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks offline '
                   'is successful')

        # Checking arequals before bringing bricks offline
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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

    def test_data_self_heal_algorithm_diff_default(self):
        """
        Test Volume Option - 'cluster.data-self-heal-algorithm' : 'diff'

        Description:
        - set the volume option "data-self-heal-algorithm" to value "diff"
        - create IO
        - bring down all bricks processes from selected set
        - modify the data
        - calculate arequal
        - bring bricks online
        - start healing
        - calculate arequal and compare with arequal before bringing bricks
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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Get arequal before getting bricks online
        g.log.info('Getting arequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks online '
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

        # Get arequal after getting bricks online
        g.log.info('Getting arequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Checking arequals before bringing bricks online
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
        - calculate arequal
        - bring bricks online
        - start healing
        - calculate arequal and compare with arequal before bringing bricks
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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Get arequal before getting bricks online
        g.log.info('Getting arequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks online '
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

        # Get arequal after getting bricks online
        g.log.info('Getting arequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Checking arequals before bringing bricks online
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
        - calculate arequal
        - bring down all bricks processes from selected set
        - calculate arequal and compare with arequal before
        getting bricks offline
        - modify the data
        - arequal before getting bricks online
        - bring bricks online
        - check daemons and healing completion
        - start healing
        - calculate arequal and compare with arequal before bringing bricks
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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Get arequal before getting bricks offline
        g.log.info('Getting arequal before getting bricks offline...')
        ret, result_before_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks offline '
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

        # Get arequal after getting bricks offline
        g.log.info('Getting arequal after getting bricks offline...')
        ret, result_after_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks offline '
                   'is successful')

        # Checking arequals before bringing bricks offline
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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Get arequal before getting bricks online
        g.log.info('Getting arequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks online '
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

        # Get arequal after getting bricks online
        g.log.info('Getting arequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Checking arequals before bringing bricks online
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
        - calculate arequal
        - bring down all bricks processes from selected set
        - calculate arequals and compare with arequal
        before bringing bricks offline
        - modify the data and verify whether the links are properly created
        - calculate arequal before getting bricks online
        - bring bricks online
        - set the volume option
        "self-heal-daemon": "on"
        - check daemons and start healing
        - check is heal is complited
        - check for split-brain
        - calculate arequal after getting bricks online and compare with
        arequal before getting bricks online
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
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Get arequal before getting bricks offline
        g.log.info('Getting arequal before getting bricks offline...')
        ret, result_before_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks offline '
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

        # Get arequal after getting bricks offline
        g.log.info('Getting arequal after getting bricks offline...')
        ret, result_after_offline = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks offline '
                   'is successful')

        # Checking arequals before bringing bricks offline
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

        # Get arequal before getting bricks online
        g.log.info('Getting arequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks online '
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

        # Get arequal after getting bricks online
        g.log.info('Getting arequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Checking arequals before bringing bricks online
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums before and '
                              'after bringing bricks online are not equal')
        g.log.info('Checksums before and after bringing bricks online '
                   'are equal')

    def test_self_heal_50k_files_heal_command_by_add_brick(self):
        """
        Test self-heal of 50k files (heal command
        Description:
        - set the volume option
        "metadata-self-heal": "off"
        "entry-self-heal": "off"
        "data-self-heal": "off"
        "self-heal-daemon": "off"
        - bring down all bricks processes from selected set
        - create IO (50k files)
        - Get arequal before getting bricks online
        - bring bricks online
        - set the volume option
        "self-heal-daemon": "on"
        - check for daemons
        - start healing
        - check if heal is completed
        - check for split-brain
        - get arequal after getting bricks online and compare with
        arequal before getting bricks online
        - add bricks
        - do rebalance
        - get arequal after adding bricks and compare with
        arequal after getting bricks online
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
        g.log.info("Successfully set %s for volume %s", options, self.volname)

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

        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create 50k files
            g.log.info('Creating files...')
            command = ("python %s create_files -f 50000 %s"
                       % (self.script_upload_path, mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Get arequal before getting bricks online
        g.log.info('Getting arequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks online '
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
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=3600)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')

        # Get arequal after getting bricks online
        g.log.info('Getting arequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Checking arequals before bringing bricks online
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums before and '
                              'after bringing bricks online are not equal')
        g.log.info('Checksums before and after bringing bricks online '
                   'are equal')

        # Add bricks
        g.log.info("Start adding bricks to volume...")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume when IO in "
                              "progress on volume %s", self.volname))
        g.log.info("Expanding volume is successful on volume %s", self.volname)

        # Do rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Failed to start rebalance')
        g.log.info('Rebalance is started')

        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Rebalance is not completed')
        g.log.info('Rebalance is completed successfully')

        # Get arequal after adding bricks
        g.log.info('Getting arequal after adding bricks...')
        ret, result_after_adding_bricks = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks '
                   'is successful')

        # Checking arequals after bringing bricks online
        # and after adding bricks
        self.assertItemsEqual(result_after_online, result_after_adding_bricks,
                              'Checksums after bringing bricks online and '
                              'after adding bricks are not equal')
        g.log.info('Checksums after bringing bricks online and '
                   'after adding bricks are equal')

    def test_self_heal_algorithm_full_daemon_off(self):
        """""
        Description:-
        Checking healing when algorithm is set to "full" and self heal daemon
        is "off".
        """""
        # pylint: disable=too-many-statements

        # Setting volume option of self heal & algorithm
        options = {"metadata-self-heal": "disable",
                   "entry-self-heal": "disable",
                   "data-self-heal": "disable",
                   "data-self-heal-algorithm": "full",
                   "self-heal-daemon": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, "Failed to set the volume options %s" % options)
        g.log.info(" Volume set options success")

        # Select bricks to bring down
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = bricks_to_bring_offline_dict['volume_bricks']
        g.log.info("Bringing bricks: %s offline", bricks_to_bring_offline)

        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, ("Failed to bring bricks: %s offline",
                              bricks_to_bring_offline))
        g.log.info("Successful in bringing bricks: %s offline",
                   bricks_to_bring_offline)

        # Validate if bricks are offline
        g.log.info("Validating if bricks: %s are offline",
                   bricks_to_bring_offline)
        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, "Not all the bricks in list:%s are offline"
                        % bricks_to_bring_offline)
        g.log.info("Successfully validated that bricks %s are all offline",
                   bricks_to_bring_offline)

        # IO on the mount point
        for mount_object in self.mounts:
            g.log.info("Creating Files on %s:%s", mount_object.client_system,
                       mount_object.mountpoint)
            cmd = ("cd %s ;for i in `seq 1 100` ;"
                   "do dd if=/dev/urandom of=file$i bs=1M "
                   "count=1;done" % mount_object.mountpoint)
            ret, _, _ = g.run(mount_object.client_system, cmd)
            self.assertEqual(ret, 0, "Failed to create files")
            g.log.info(" Files created successfully")

        # Collecting Arequal before bring the bricks up
        g.log.info("Collecting Arequal before the bring of bricks down")
        result_before = collect_mounts_arequal(self.mounts)

        # Turning self heal daemon ON
        optionstwo = {"self-heal-daemon": "on"}
        ret = set_volume_options(self.mnode, self.volname, optionstwo)
        self.assertTrue(ret, "Failed to turn self-heal ON")
        g.log.info("Volume set options %s: success", optionstwo)

        # Bring bricks online
        g.log.info("Bring bricks: %s online", bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, "Failed to bring bricks: %s online"
                        % bricks_to_bring_offline)
        g.log.info("Successfully brought all bricks:%s online",
                   bricks_to_bring_offline)

        # Waiting for bricks to come online
        g.log.info("Waiting for brick process to come online")
        timeout = 30
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname, timeout)
        self.assertTrue(ret, "bricks didn't come online after adding bricks")
        g.log.info("Bricks are online")

        # Verifying all bricks online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, "Volume %s : All process are not online"
                        % self.volname)
        g.log.info("Volume %s : All process are online", self.volname)

        # Wait for self heal processes to come online
        g.log.info("Wait for selfheal process to come online")
        timeout = 300
        ret = wait_for_self_heal_daemons_to_be_online(self.mnode,
                                                      self.volname, timeout)
        self.assertTrue(ret, "Self-heal process are not online")
        g.log.info("All self heal process are online")

        # Wait for self-heal to complete
        g.log.info("Wait for self-heal to complete")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "Self heal didn't complete even after waiting "
                        "for 20 minutes. 20 minutes is too much a time for "
                        "current test workload")
        g.log.info("self-heal is successful after replace-brick operation")

        # arequal after healing
        g.log.info("Collecting Arequal before the bring of bricks down")
        result_after = collect_mounts_arequal(self.mounts)

        # Comparing the results
        g.log.info("comparing both the results")
        self.assertEqual(result_before, result_after, "Arequals are not equal")


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'cifs', 'nfs']])
class TestMetadataSelfHeal(GlusterBaseClass):
    """
    Description:
        Test cases related to metadata delf heal
        in default configuration of the volume
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

        for mount_object in self.mounts:
            # Create user qa
            g.log.info("Creating user 'qa'...")
            command = "useradd qa"
            ret, _, err = g.run(mount_object.client_system, command)

            if 'already exists' in err:
                g.log.warn("User 'qa' is already exists")
            else:
                g.log.info("User 'qa' is created successfully")

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

        for mount_object in self.mounts:
            # Delete user
            g.log.info('Deleting user qa...')
            command = "userdel -r qa"
            ret, _, err = g.run(mount_object.client_system, command)

            if 'does not exist' in err:
                g.log.warn('User qa is already deleted')
            else:
                g.log.info('User qa successfully deleted')

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    def test_metadata_self_heal(self):
        """
        Test MetaData Self-Heal (heal command)

        Description:
        - set the volume option
        "metadata-self-heal": "off"
        "entry-self-heal": "off"
        "data-self-heal": "off"
        - create IO
        - set the volume option
        "self-heal-daemon": "off"
        - bring down all bricks processes from selected set
        - Change the permissions, ownership and the group
        of the files under "test_meta_data_self_heal" folder
        - get arequal before getting bricks online
        - bring bricks online
        - set the volume option
        "self-heal-daemon": "on"
        - check daemons and start healing
        - check is heal is completed
        - check for split-brain
        - get arequal after getting bricks online and compare with
        arequal before getting bricks online
        - check group and user are 'qa'
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Setting options
        g.log.info('Setting options...')
        options = {"metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "data-self-heal": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Options "
                   "'metadata-self-heal', "
                   "'entry-self-heal', "
                   "'data-self-heal', "
                   "are set to 'off' successfully")

        # Creating files on client side
        test_meta_data_self_heal_folder = 'test_meta_data_self_heal'
        for mount_object in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)

            # Create files
            g.log.info('Creating files...')
            command = ("cd %s/ ; "
                       "mkdir %s ;"
                       "cd %s/ ;"
                       "for i in `seq 1 50` ; "
                       "do dd if=/dev/urandom of=test.$i bs=10k count=1 ; "
                       "done ;"
                       % (mount_object.mountpoint,
                          test_meta_data_self_heal_folder,
                          test_meta_data_self_heal_folder))

            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

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

        # Changing the permissions, ownership and the group
        # of the files under "test_meta_data_self_heal" folder
        for mount_object in self.mounts:
            g.log.info("Modifying data for %s:%s",
                       mount_object.client_system, mount_object.mountpoint)

            # Change permissions to 444
            g.log.info('Changing permissions...')
            command = ("cd %s/%s/ ; "
                       "chmod -R 444 *"
                       % (mount_object.mountpoint,
                          test_meta_data_self_heal_folder))
            ret, out, err = g.run(mount_object.client_system, command)
            self.assertEqual(ret, 0, err)
            g.log.info('Permissions are changed successfully')

            # Change the ownership to qa
            g.log.info('Changing the ownership...')
            command = ("cd %s/%s/ ; "
                       "chown -R qa *"
                       % (mount_object.mountpoint,
                          test_meta_data_self_heal_folder))
            ret, out, err = g.run(mount_object.client_system, command)
            self.assertEqual(ret, 0, err)
            g.log.info('Ownership is changed successfully')

            # Change the group to qa
            g.log.info('Changing the group...')
            command = ("cd %s/%s/ ; "
                       "chgrp -R qa *"
                       % (mount_object.mountpoint,
                          test_meta_data_self_heal_folder))
            ret, out, err = g.run(mount_object.client_system, command)
            self.assertEqual(ret, 0, err)
            g.log.info('Group is changed successfully')

        # Get arequal before getting bricks online
        g.log.info('Getting arequal before getting bricks online...')
        ret, result_before_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks online '
                   'is successful')

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
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Option 'self-heal-daemon' is set to 'on' successfully")

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume process %s not online "
                              "despite waiting for 5 minutes", self.volname))
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

        # Get arequal after getting bricks online
        g.log.info('Getting arequal after getting bricks online...')
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Checking arequals before bringing bricks online
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums are not equal')
        g.log.info('Checksums before bringing bricks online '
                   'and after bringing bricks online are equal')

        # Check for user and group
        for mount_object in self.mounts:
            # Get file list
            command = ("cd %s/%s/ ; "
                       "ls"
                       % (mount_object.mountpoint,
                          test_meta_data_self_heal_folder))
            ret, out, err = g.run(mount_object.client_system, command)
            file_list = out.split()

            # Checking for user and group
            g.log.info('Checking for user and group...')
            conn = g.rpyc_get_connection(mount_object.client_system)
            if conn is None:
                raise Exception("Unable to get connection on node %s" %
                                mount_object.client_system)

            for file_name in file_list:
                file_to_check = '%s/%s/%s' % (mount_object.mountpoint,
                                              test_meta_data_self_heal_folder,
                                              file_name)

                g.log.info('Checking for user and group for %s...', file_name)
                # Check for user
                uid = conn.modules.os.stat(file_to_check).st_uid
                username = conn.modules.pwd.getpwuid(uid).pw_name
                self.assertEqual(username, 'qa', 'User %s is not equal qa'
                                 % username)
                g.log.info("User is 'qa' for %s", file_name)

                # Check for group
                gid = conn.modules.os.stat(file_to_check).st_gid
                groupname = conn.modules.grp.getgrgid(gid).gr_name
                self.assertEqual(groupname, 'qa', 'Group %s is not equal qa'
                                 % groupname)
                g.log.info("Group is 'qa' for %s", file_name)

            g.rpyc_close_connection(host=mount_object.client_system)
