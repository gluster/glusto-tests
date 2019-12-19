#  Copyright (C) 2016-2018  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           get_volume_options)
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated'],
          ['glusterfs']])
class ArbiterSelfHealTests(GlusterBaseClass):
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
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

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

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_no_data_loss_arbiter_vol_after_rename_file(self):
        """
        - Create a 1x(2+1) arbiter replicate volume
        - Turn off Clients side healing option
        - Create a directory 'test_dir'
        - Bring down the 1-st data brick
        - Create a file under 'test_dir'
        - Bring down the 2-nd data brick
        - Bring up the 1-st data brick
        - Rename file under 'test_dir'
        - Bring up the 2-nd data brick
        - Turn on Clients side healing option
        - Trigger heal
        - Check if no pending heals
        - Check if md5sum on mountpoint is the same for md5sum_node on nodes
        """
        # pylint: disable=too-many-locals,too-many-statements
        test_dir = 'test_dir'

        # Setting options
        options = {"cluster.metadata-self-heal": "off",
                   "cluster.entry-self-heal": "off",
                   "cluster.data-self-heal": "off"}
        g.log.info('Setting options %s for volume %s...',
                   options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s for volume %s'
                        % (options, self.volname))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)
        options_dict = get_volume_options(self.mnode, self.volname)
        # validating  options are off
        for opt in options:
            self.assertEqual(options_dict[opt], 'off',
                             'options are  not set to off')
        g.log.info('Option are set to off for volume %s: %s',
                   options, self.volname)

        # Creating IO on client side
        g.log.info("Generating data for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        # Create dir
        g.log.info('Creating dir...')
        command = ('/usr/bin/env python%d %s create_deep_dir -d 1 -l 0 -n 1 '
                   '%s/%s' % (
                       sys.version_info.major, self.script_upload_path,
                       self.mounts[0].mountpoint, test_dir))

        ret, _, err = g.run(self.mounts[0].client_system, command,
                            user=self.mounts[0].user)

        self.assertFalse(ret, err)
        g.log.info("IO is successful")

        # get the bricks for the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

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

        # Create file under dir test_dir
        g.log.info("Generating file for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        # Create file
        g.log.info('Creating file...')
        command = "/usr/bin/env python%d %s create_files -f 1 %s/%s" % (
            sys.version_info.major, self.script_upload_path,
            self.mounts[0].mountpoint, test_dir)

        ret, _, err = g.run(self.mounts[0].client_system, command,
                            user=self.mounts[0].user)

        self.assertFalse(ret, err)
        g.log.info("Created file successfully")

        # get md5sum for file
        g.log.info('Getting md5sum for file on %s', self.mounts[0].mountpoint)

        command = ("md5sum %s/%s/testfile0.txt | awk '{ print $1 }'"
                   % (self.mounts[0].mountpoint, test_dir))

        ret, md5sum, err = g.run(self.mounts[0].client_system, command,
                                 user=self.mounts[0].user)
        self.assertFalse(ret, err)
        g.log.info('md5sum: %s', md5sum)

        # Bring brick 2 offline
        bricks_to_bring_offline = [bricks_list[1]]
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

        # Bring 1-st brick online
        bricks_to_bring_online = [bricks_list[0]]
        g.log.info('Bringing bricks %s online...', bricks_to_bring_online)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_online)
        self.assertTrue(ret, 'Failed to bring bricks %s online'
                        % bricks_to_bring_online)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_online)

        # Rename file under test_dir
        g.log.info("Renaming file for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        command = "/usr/bin/env python%d %s mv %s/%s" % (
            sys.version_info.major, self.script_upload_path,
            self.mounts[0].mountpoint, test_dir)
        ret, _, err = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, err)
        g.log.info("Renaming file for %s:%s is successful",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)

        # Bring 2-nd brick online
        g.log.info('Bringing bricks %s online...', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Mount and unmount mounts
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, 'Failed to unmount %s' % self.volname)

        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, 'Unable to mount %s' % self.volname)

        # Enable client side healing
        g.log.info("Enable client side healing options")
        options = {"metadata-self-heal": "on",
                   "entry-self-heal": "on",
                   "data-self-heal": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)
        # Trigger heal from mount point
        g.log.info("Triggering heal for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        command = ("cd %s/%s ; find . | xargs getfattr -d -m . -e hex"
                   % (self.mounts[0].mountpoint,
                      test_dir))

        ret, _, err = g.run(self.mounts[0].client_system, command)
        self.assertFalse(ret, 'Failed to trigger heal using '
                              '"find . | xargs getfattr -d -m . -e hex" on %s'
                         % self.mounts[0].client_system)

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

        # Get md5sum for file on all nodes and compare with mountpoint
        for brick in bricks_list[0:2]:
            g.log.info('Getting md5sum for file on %s', brick)
            node, brick_path = brick.split(':')
            command = ("md5sum %s/%s/testfile0_a.txt  | awk '{ print $1 }'"
                       % (brick_path, test_dir))
            ret, md5sum_node, err = g.run(node, command,
                                          user=self.mounts[0].user)
            self.assertFalse(ret, err)
            g.log.info('md5sum for the node: %s', md5sum_node)

            # Comparing md5sum_node result with mountpoint
            g.log.info('Comparing md5sum result with mountpoint...')
            self.assertEqual(md5sum, md5sum_node, 'md5sums are not equal'
                                                  ' on %s and %s'
                             % (self.mounts[0].mountpoint, brick))
            g.log.info('md5sums are equal on %s and %s',
                       self.mounts[0].mountpoint, brick)
