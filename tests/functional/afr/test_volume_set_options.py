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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           get_volume_options)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain)
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs']])
class VolumeSetDataSelfHealTests(GlusterBaseClass):
    def setUp(self):
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(self.mounts)
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

    def test_volume_set_option_data_self_heal(self):
        """
        - turn off self-heal-daemon option
        - turn off data-self-heal option
        - check if the options are set correctly
        - create IO
        - calculate arequal
        If it is distribute-replicate, the  areequal-check sum of nodes
        in each replica set should match
        - bring down "brick1"
        - modify IO
        - bring back the brick1
        - execute "find . | xargs stat" from the mount point
        to trigger background data self-heal
        - calculate arequal
        If it is distribute-replicate, arequal's checksum of brick which
        was down should not match with the bricks which was up
        in the replica set but for other replicaset where all bricks are up
        should match the areequal-checksum
        - check if the data of existing files are not modified in brick1
        - turn on the option data-self-heal
        - execute "find . -type f  | xargs md5sum" from the mount point
        - wait for heal to complete
        - calculate areequal
        If it is distribute-replicate, the  areequal-check sum of nodes
        in each replica set should match
        """
        # pylint: disable=too-many-locals,too-many-statements,too-many-branches

        all_bricks = get_all_bricks(self.mnode, self.volname)

        # Setting options
        options = {"self-heal-daemon": "off",
                   "data-self-heal": "off"}
        g.log.info('Setting options %s...', options)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # Check if options are set to off
        options_dict = get_volume_options(self.mnode, self.volname)
        self.assertEqual(options_dict['cluster.self-heal-daemon'], 'off',
                         'Option self-heal-daemon is not set to off')
        self.assertEqual(options_dict['cluster.data-self-heal'], 'off',
                         'Option data-self-heal is not set to off')
        g.log.info('Option are set to off: %s', options)

        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files and dirs...')
            command = ('cd %s ; '
                       'mkdir test_data_self_heal ;'
                       'cd test_data_self_heal ; '
                       'for i in `seq 1 100` ; '
                       'do dd if=/dev/urandom of=file.$i bs=128K count=$i ; '
                       'done ;'
                       % mount_obj.mountpoint)

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

        # Check arequals
        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume: %s", num_subvols)

        # Get arequals for bricks in each subvol and compare with first brick
        for i in range(0, num_subvols):
            # Get arequal for first brick
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            node, brick_path = subvol_brick_list[0].split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan'
                       % brick_path)
            ret, arequal, _ = g.run(node, command)
            first_brick_total = arequal.splitlines()[-1].split(':')[-1]

            # Get arequal for every brick and compare with first brick
            for brick in subvol_brick_list:
                node, brick_path = brick.split(':')
                command = ('arequal-checksum -p %s '
                           '-i .glusterfs -i .landfill -i .trashcan'
                           % brick_path)
                ret, brick_arequal, _ = g.run(node, command)
                self.assertFalse(ret,
                                 'Failed to get arequal on brick %s'
                                 % brick)
                g.log.info('Getting arequal for %s is successful', brick)
                brick_total = brick_arequal.splitlines()[-1].split(':')[-1]
                self.assertEqual(first_brick_total, brick_total,
                                 'Arequals for subvol and %s are not equal'
                                 % brick)
                g.log.info('Arequals for subvol and %s are equal', brick)
        g.log.info('All arequals are equal for distributed-replicated')

        # Select bricks to bring offline, 1st brick only
        bricks_to_bring_offline = [get_all_bricks(self.mnode, self.volname)[0]]

        # Get files/dir size
        g.log.info('Getting file/dir list on brick to be offline')
        node, brick_path = bricks_to_bring_offline[0].split(':')
        # Get files/dir list
        command = 'cd %s ; ls' % brick_path
        ret, _, _ = g.run(node, command)
        self.assertFalse(ret, 'Failed to ls files on %s' % node)
        # Get arequal of brick before making offline
        # Get arequals for first subvol and compare
        command = ('arequal-checksum -p %s '
                   '-i .glusterfs -i .landfill -i .trashcan'
                   % brick_path)
        ret, arequal, _ = g.run(node, command)
        arequal_before_brick_offline = arequal.splitlines()[-1].split(':')[-1]
        # Bring brick 1 offline
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

        # Modify data
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Adding data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # changing files
            g.log.info('Creating dirs and files...')
            command = ('cd %s; cd test_data_self_heal ; '
                       'for i in `seq 1 100` ; '
                       'do dd if=/dev/urandom of=file.$i bs=512K count=$i ; '
                       'done ;'
                       % mount_obj.mountpoint)

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
        self.assertTrue(ret, 'Failed to bring bricks %s online'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Trigger heal from mount point
        g.log.info('Triggering heal from mount point...')
        for mount_obj in self.mounts:
            g.log.info("Triggering heal for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            command = ('cd %s/test_data_self_heal ; find . | xargs stat'
                       % mount_obj.mountpoint)
            ret, _, _ = g.run(mount_obj.client_system, command)
            self.assertFalse(ret, 'Failed to start "find . | xargs stat" '
                             'on %s ' % mount_obj.client_system)

        # Check arequals
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        subvols = subvols_dict['volume_subvols']

        # Get arequals for first subvol and compare
        first_brick = all_bricks[0]
        node, brick_path = first_brick.split(':')
        command = ('arequal-checksum -p %s '
                   '-i .glusterfs -i .landfill -i .trashcan'
                   % brick_path)
        ret, arequal, _ = g.run(node, command)
        first_brick_total = arequal.splitlines()[-1].split(':')[-1]

        # Get arequals for all bricks in subvol 0
        for brick in subvols[0]:
            g.log.info('Getting arequal on bricks %s...', brick)
            node, brick_path = brick.split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan'
                       % brick_path)
            ret, arequal, _ = g.run(node, command)
            self.assertFalse(ret,
                             'Failed to get arequal on brick %s' % brick)
            g.log.info('Getting arequal for %s is successful', brick)
            brick_total = arequal.splitlines()[-1].split(':')[-1]

            # Validate that the down brick had different arequal
            if brick != first_brick:
                self.assertNotEqual(first_brick_total, brick_total,
                                    'Arequals for mountpoint and %s '
                                    'are equal' % brick)
                g.log.info('Arequals for mountpoint and %s are not equal',
                           brick)
            else:
                self.assertEqual(first_brick_total, brick_total,
                                 'Arequals for mountpoint and %s '
                                 'are not equal' % brick)
                g.log.info('Arequals for mountpoint and %s are equal', brick)

        # Get arequals for all subvol except first and compare
        num_subvols = len(subvols_dict['volume_subvols'])
        for i in range(1, num_subvols):
            # Get arequal for first brick
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            node, brick_path = subvol_brick_list[0].split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan'
                       % brick_path)
            ret, arequal, _ = g.run(node, command)
            first_brick_total = arequal.splitlines()[-1].split(':')[-1]

            # Get arequal for every brick and compare with first brick
            for brick in subvol_brick_list:
                node, brick_path = brick.split(':')
                command = ('arequal-checksum -p %s '
                           '-i .glusterfs -i .landfill -i .trashcan'
                           % brick_path)
                ret, brick_arequal, _ = g.run(node, command)
                self.assertFalse(ret,
                                 'Failed to get arequal on brick %s'
                                 % brick)
                g.log.info('Getting arequal for %s is successful', brick)
                brick_total = brick_arequal.splitlines()[-1].split(':')[-1]

                self.assertEqual(first_brick_total, brick_total,
                                 'Arequals for subvol and %s are not equal'
                                 % brick)
                g.log.info('Arequals for subvol and %s are equal', brick)
        g.log.info('All arequals are equal for distributed-replicated')

        # Get files/dir size after bringing brick online
        g.log.info('Getting arequal size on brick after bringing online')
        node, brick_path = bricks_to_bring_offline[0].split(':')
        # Get files/dir list
        command = 'cd %s ; ls' % brick_path
        ret, _, _ = g.run(node, command)
        self.assertFalse(ret, 'Failed to ls files on %s' % node)
        # Get arequal of brick to be offline
        # Get arequals for first subvol and compare
        command = ('arequal-checksum -p %s '
                   '-i .glusterfs -i .landfill -i .trashcan'
                   % brick_path)
        ret, arequal, _ = g.run(node, command)
        arequal_after_brick_offline = arequal.splitlines()[-1].split(':')[-1]
        # Compare dicts with file size
        g.log.info('Compare arequal size on brick before bringing offline and'
                   ' after bringing online')
        self.assertFalse(cmp(arequal_before_brick_offline,
                             arequal_after_brick_offline),
                         'arequal size on brick before bringing offline and '
                         'after bringing online are not equal')
        g.log.info('arequal size on brick  before bringing offline and '
                   'after bringing online are equal')

        # Setting options
        time_delay = 5
        options = {"data-self-heal": "on"}
        g.log.info('Setting options %s...', options)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Option 'data-self-heal' is set to 'on' successfully")

        g.log.info('Droping client cache')
        command = 'echo 3 > /proc/sys/vm/drop_caches'
        ret, _, _ = g.run(mount_obj.client_system, command)
        self.assertFalse(ret, 'Failed to drop cache, comamnd failed')
        g.log.info('Successfully cleared client cache')

        # Start heal from mount point
        g.log.info('Starting heal from mount point...')
        for mount_obj in self.mounts:
            g.log.info("Start heal for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            command = ('cd %s/test_data_self_heal ;'
                       'for i in `ls -1`; do md5sum $i; sleep 1; done;'
                       % mount_obj.mountpoint)
            _, _, _ = g.run(mount_obj.client_system, command)
            sleep(time_delay)
            g.log.info('Execuing cat on mountpoint')
            command = ('cd %s/test_data_self_heal ;'
                       'for i in `ls -1`; do cat $i > /dev/null 2>&1; '
                       ' sleep 1; done;'
                       % mount_obj.mountpoint)
            _, _, _ = g.run(mount_obj.client_system, command)

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

        # Check arequals
        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume: %s", num_subvols)

        # Get arequals and compare
        for i in range(0, num_subvols):
            # Get arequal for first brick
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            node, brick_path = subvol_brick_list[0].split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan'
                       % brick_path)
            ret, arequal, _ = g.run(node, command)
            first_brick_total = arequal.splitlines()[-1].split(':')[-1]

            # Get arequal for every brick and compare with first brick
            for brick in subvol_brick_list:
                node, brick_path = brick.split(':')
                command = ('arequal-checksum -p %s '
                           '-i .glusterfs -i .landfill -i .trashcan'
                           % brick_path)
                ret, brick_arequal, _ = g.run(node, command)
                self.assertFalse(ret,
                                 'Failed to get arequal on brick %s'
                                 % brick)
                g.log.info('Getting arequal for %s is successful', brick)
                brick_total = brick_arequal.splitlines()[-1].split(':')[-1]

                self.assertEqual(first_brick_total, brick_total,
                                 'Arequals for subvol and %s are not equal'
                                 % brick)
                g.log.info('Arequals for subvol and %s are equal', brick)
        g.log.info('All arequals are equal for distributed-replicated')
