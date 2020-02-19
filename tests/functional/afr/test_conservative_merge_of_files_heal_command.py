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

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain,
                                          is_shd_daemonized)
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

        # Override Volumes
        if cls.volume_type == "replicated":
            # Define x2 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 2,
                'transport': 'tcp'}

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

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
        self.get_super_method(self, 'tearDown')()

    def test_conservative_merge_of_files_heal_command(self):
        """
        - set options:
        "metadata-self-heal": "off",
        "entry-self-heal": "off",
        "data-self-heal": "off",
        "self-heal-daemon": "off"
        - Bring brick 0 offline
        - Creating files on client side
        - Bring brick 0 online
        - Bring brick 1 offline
        - Creating files on client side
        - Bring brick 1 online
        - Get arequal on bricks
        - Setting option
        "self-heal-daemon": "on"
        - Start healing
        - Get arequal on bricks and compare with arequals before healing
        and mountpoint
        """
        # pylint: disable=too-many-statements,too-many-locals
        # set options
        bricks_list = get_all_bricks(self.mnode, self.volname)
        options = {"metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "data-self-heal": "off",
                   "self-heal-daemon": "off"}
        g.log.info("setting options %s", options)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for"
                              "volume %s" % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # Bring brick 0 offline
        g.log.info('Bringing bricks %s offline', bricks_list[0])
        ret = bring_bricks_offline(self.volname, bricks_list[0])
        self.assertTrue(ret, 'Failed to bring bricks %s offline'
                        % bricks_list[0])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[0]])
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_list[0])
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_list[0])

        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("/usr/bin/env python %s create_deep_dirs_with_files "
                       "-d 0 -l 5 -f 10 --dirname-start-num 1 %s" % (
                           self.script_upload_path,
                           mount_obj.mountpoint))

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

        # Bring brick 0 online
        g.log.info('Bringing bricks %s online...', bricks_list[0])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[0]])
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_list[0])
        g.log.info('Bringing bricks %s online is successful', bricks_list[0])

        # Bring brick 1 offline
        g.log.info('Bringing bricks %s offline', bricks_list[1])
        ret = bring_bricks_offline(self.volname, bricks_list[1])
        self.assertTrue(ret, 'Failed to bring bricks %s offline'
                        % bricks_list[1])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[1]])
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_list[1])
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_list[1])

        # Creating files on client side
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("/usr/bin/env python %s create_deep_dirs_with_files "
                       "-d 0 -l 5 -f 10 --dirname-start-num 6 %s" % (
                           self.script_upload_path,
                           mount_obj.mountpoint))

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

        # Bring brick 1 online
        g.log.info('Bringing bricks %s online...', bricks_list[1])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[1]])
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_list[1])
        g.log.info('Bringing bricks %s online is successful', bricks_list[1])

        # Get arequal on bricks
        arequals_before_heal = {}
        g.log.info('Getting arequal on bricks...')
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
            arequals_before_heal[brick] = brick_total

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

        # Get arequals for mount
        g.log.info('Getting arequal before getting bricks offline...')
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
        g.log.info('All arequals are equal for replicated')

        self.assertNotEqual(
            arequals_before_heal, arequals_after_heal,
            'Arequals are equal for bricks before (%s) and after (%s) '
            'healing' % (arequals_before_heal, arequals_after_heal))
