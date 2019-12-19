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

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_ops import get_heal_info_summary
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs,
                                 wait_for_io_to_complete)


@runs_on([['replicated'],
          ['glusterfs']])
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
            # Define x3 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
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

    def test_shd_should_not_crash_executed_heal_info(self):
        """
        - set "entry-self-heal", "metadata-self-heal", "data-self-heal" to off
        - write a few files
        - bring down brick0
        - add IO
        - do a heal info and check for files pending heal on last 2 bricks
        - set "performance.enable-least-priority" to "enable"
        - bring down brick1
        - set the "quorum-type" to "fixed"
        - add IO
        - do a heal info and check for files pending heal on the last brick
        """
        # pylint: disable=too-many-statements
        bricks_list = get_all_bricks(self.mnode, self.volname)
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
            command = ("/usr/bin/env python%d %s create_files -f 10 "
                       "--fixed-file-size 1M %s" % (
                           sys.version_info.major, self.script_upload_path,
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

        # Bring brick0 offline
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
        number_of_files_one_brick_off = '1000'
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("/usr/bin/env python%d %s create_files "
                       "-f %s "
                       "--fixed-file-size 1k "
                       "--base-file-name new_file "
                       "%s"
                       % (sys.version_info.major,
                          self.script_upload_path,
                          number_of_files_one_brick_off,
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

        # Get heal info
        g.log.info("Getting heal info...")
        heal_info_data = get_heal_info_summary(self.mnode, self.volname)
        self.assertIsNotNone(heal_info_data, 'Failed to get heal info.')
        g.log.info('Success in getting heal info')

        # Check quantity of file pending heal
        for brick in bricks_list[1:]:
            self.assertEqual(heal_info_data[brick]['numberOfEntries'],
                             str(int(number_of_files_one_brick_off)+1),
                             'Number of files pending heal is not correct')

        # Setting options
        g.log.info('Setting options...')
        options = {"performance.enable-least-priority": "enable"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # Bring brick1 offline
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

        # Setting options
        g.log.info('Setting options...')
        options = {"quorum-type": "fixed"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # Creating files on client side
        number_of_files_two_brick_off = '100'
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("/usr/bin/env python%d %s create_files "
                       "-f %s "
                       "--fixed-file-size 1k "
                       "--base-file-name new_new_file "
                       "%s"
                       % (sys.version_info.major,
                          self.script_upload_path,
                          number_of_files_two_brick_off,
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

        # Get heal info
        g.log.info("Getting heal info...")
        heal_info_data = get_heal_info_summary(self.mnode, self.volname)
        self.assertIsNotNone(heal_info_data, 'Failed to get heal info.')
        g.log.info('Success in getting heal info')

        # Check quantity of file pending heal
        number_of_files_to_check = str(int(number_of_files_one_brick_off) +
                                       int(number_of_files_two_brick_off) + 1)
        self.assertEqual(heal_info_data[bricks_list[-1]]['numberOfEntries'],
                         number_of_files_to_check,
                         'Number of files pending heal is not correct')
