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
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs,
                                 wait_for_io_to_complete)


@runs_on([['replicated'],
          ['glusterfs', 'cifs', 'nfs']])
class VerifySelfHealTriggersHealCommand(GlusterBaseClass):
    """
    Description:
        Verify self-heal Triggers with self heal with heal command
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

        # Override Volumes
        if cls.volume_type == "replicated":
            # Define x2 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 2,
                'transport': 'tcp'}

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

    def test_multiple_clients_dd_on_same_file_default(self):
        """
        - Create 2GB file
        - While creating file, start reading file
        - Bring down brick1
        - Bring back the brick brick1
        - Start healing
        - Bring down brick1
        - Wait for IO to complete
        - Wait for reading to complete
        - Bring back the brick brick1
        - Start healing
        - Wait for heal to complete
        - Check for split-brain
        - Calculate arequals on all the bricks and compare with mountpoint
        """
        # pylint: disable=too-many-statements,too-many-locals
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, 'Brick list is None')

        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("cd %s ; "
                       "dd if=/dev/urandom of=test_file bs=1M count=2020"
                       % mount_obj.mountpoint)

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Reading files on client side
        all_mounts_procs_read = []
        for mount_obj in self.mounts:
            g.log.info("Reading data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Reading files...')
            command = ("python %s read %s"
                       % (self.script_upload_path, mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            all_mounts_procs_read.append(proc)

        # Bring brick1 offline
        g.log.info('Bringing bricks %s offline...', bricks_list[1])
        ret = bring_bricks_offline(self.volname, [bricks_list[1]])
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_list[1])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[1]])
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_list[1])
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_list[1])

        # Bring brick1 online
        g.log.info('Bringing bricks %s online...', bricks_list[1])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[1]])
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_list[1])
        g.log.info('Bringing bricks %s online is successful',
                   bricks_list[1])

        # Start healing
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not started')
        g.log.info('Healing is started')

        # Bring brick1 offline
        g.log.info('Bringing bricks %s offline...', bricks_list[1])
        ret = bring_bricks_offline(self.volname, [bricks_list[1]])
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_list[1])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[1]])
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_list[1])
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_list[1])

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Validate reading
        g.log.info("Wait for reading to complete ...")
        ret = validate_io_procs(all_mounts_procs_read, self.mounts)
        self.assertTrue(ret, "Reading failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("Reading is successful on all mounts")

        # Bring brick1 online
        g.log.info('Bringing bricks %s online...', bricks_list[1])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[1]])
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_list[1])
        g.log.info('Bringing bricks %s online is successful',
                   bricks_list[1])

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

        # Get arequal for mount
        g.log.info('Getting arequal...')
        ret, arequals = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after healing is successful')
        mount_point_total = arequals[0].splitlines()[-1].split(':')[-1]

        # Get arequal on bricks and compare with mount_point_total
        # It should be the same
        g.log.info('Getting arequal on bricks...')
        arequals_after_heal = {}
        for brick in bricks_list:
            g.log.info('Getting arequal on bricks %s...', brick)
            node, brick_path = brick.split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan'
                       % brick_path)
            ret, arequal, _ = g.run(node, command)
            self.assertFalse(ret, 'Failed to get arequal on brick %s'
                             % brick)
            g.log.info('Getting arequal for %s is successful', brick)
            brick_total = arequal.splitlines()[-1].split(':')[-1]
            arequals_after_heal[brick] = brick_total
            self.assertEqual(mount_point_total, brick_total,
                             'Arequals for mountpoint and %s are not equal'
                             % brick)
            g.log.info('Arequals for mountpoint and %s are equal', brick)
        g.log.info('All arequals are equal')
