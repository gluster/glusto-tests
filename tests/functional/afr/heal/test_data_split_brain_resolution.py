#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable=too-many-statements, too-many-locals

""" Description:
        Test cases in this module tests whether heal command for resolving
        split-brains will resolve all the files in data-split brains by using
        one of the method (bigger-file/latest-mtime/source-brick).
"""

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_volume_in_split_brain)
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           are_bricks_online)


@runs_on([['replicated'],
          ['glusterfs']])
class HealDataSplitBrain(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Override Volume
        if cls.volume_type == "replicated":
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 2,
                'transport': 'tcp'}

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
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

    def verify_brick_arequals(self):
        g.log.info("Fetching bricks for the volume: %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info('Getting arequal on bricks...')
        arequal_0 = 0
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
            if arequal_0 == 0:
                arequal_0 = brick_total
            else:
                self.assertEqual(brick_total, arequal_0, 'Arequal for %s and '
                                 '%s are not equal' % (bricks_list[0], brick))
        g.log.info('All arequals are equal on all the bricks')

    def test_data_split_brain_resolution(self):
        # Setting options
        g.log.info('Setting options...')
        options = {"metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "data-self-heal": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # Creating files and directories on client side
        g.log.info('Creating files and directories...')
        cmd = ("for i in `seq 1 10`; do mkdir %s/dir.$i; for j in `seq 1 5`;"
               "do dd if=/dev/urandom of=%s/dir.$i/file.$j bs=1K count=1;"
               "done; dd if=/dev/urandom of=%s/file.$i bs=1K count=1; done"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint,
                  self.mounts[0].mountpoint))

        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Creating files and directories failed")
        g.log.info("Files & directories created successfully")

        # Check arequals for all the bricks
        g.log.info('Getting arequal before getting bricks offline...')
        self.verify_brick_arequals()
        g.log.info('Getting arequal before getting bricks offline '
                   'is successful')

        # Set option self-heal-daemon to OFF
        g.log.info('Setting option self-heal-daemon to off...')
        options = {"self-heal-daemon": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Option 'self-heal-daemon' is set to 'off' successfully")

        bricks_list = get_all_bricks(self.mnode, self.volname)

        # Bring brick1 offline
        g.log.info('Bringing brick %s offline', bricks_list[0])
        ret = bring_bricks_offline(self.volname, bricks_list[0])
        self.assertTrue(ret, 'Failed to bring bricks %s offline'
                        % bricks_list[0])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[0]])
        self.assertTrue(ret, 'Brick %s is not offline'
                        % bricks_list[0])
        g.log.info('Bringing brick %s offline is successful',
                   bricks_list[0])

        # Modify the contents of the files
        cmd = ("for i in `seq 1 10`; do for j in `seq 1 5`;"
               "do dd if=/dev/urandom of=%s/dir.$i/file.$j bs=1M count=1;"
               "done; dd if=/dev/urandom of=%s/file.$i bs=1K count=1; done"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))

        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Updating file contents failed")
        g.log.info("File contents updated successfully")

        # Bricng brick1 online and check the status
        g.log.info('Bringing brick %s online', bricks_list[0])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[0]])
        self.assertTrue(ret, 'Failed to bring brick %s online' %
                        bricks_list[0])
        g.log.info('Bringing brick %s online is successful', bricks_list[0])

        g.log.info("Verifying if brick %s is online", bricks_list[0])
        ret = are_bricks_online(self.mnode, self.volname, bricks_list)
        self.assertTrue(ret, ("Brick %s did not come up", bricks_list[0]))
        g.log.info("Brick %s has come online.", bricks_list[0])

        # Bring brick2 offline
        g.log.info('Bringing brick %s offline', bricks_list[1])
        ret = bring_bricks_offline(self.volname, bricks_list[1])
        self.assertTrue(ret, 'Failed to bring bricks %s offline'
                        % bricks_list[1])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[1]])
        self.assertTrue(ret, 'Brick %s is not offline'
                        % bricks_list[1])
        g.log.info('Bringing brick %s offline is successful',
                   bricks_list[1])

        # Modify the contents of the files
        cmd = ("for i in `seq 1 10`; do for j in `seq 1 5`;"
               "do dd if=/dev/urandom of=%s/dir.$i/file.$j bs=1M count=2;"
               "done; dd if=/dev/urandom of=%s/file.$i bs=1K count=2; done"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))

        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Updating file contents failed")
        g.log.info("File contents updated successfully")

        # Bricng brick2 online and check the status
        g.log.info('Bringing brick %s online', bricks_list[1])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[1]])
        self.assertTrue(ret, 'Failed to bring brick %s online' %
                        bricks_list[1])
        g.log.info('Bringing brick %s online is successful', bricks_list[1])

        g.log.info("Verifying if brick %s is online", bricks_list[1])
        ret = are_bricks_online(self.mnode, self.volname, bricks_list)
        self.assertTrue(ret, ("Brick %s did not come up", bricks_list[1]))
        g.log.info("Brick %s has come online.", bricks_list[1])

        # Set option self-heal-daemon to ON
        g.log.info('Setting option self-heal-daemon to on...')
        options = {"self-heal-daemon": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Option 'self-heal-daemon' is set to 'on' successfully")

        g.log.info("Checking if files are in split-brain")
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertTrue(ret, "Unable to create split-brain scenario")
        g.log.info("Successfully created split brain scenario")

        g.log.info("Resolving split-brain by using the source-brick option "
                   "by choosing second brick as source for all the files")
        node, _ = bricks_list[1].split(':')
        command = ("gluster v heal " + self.volname + " split-brain "
                   "source-brick " + bricks_list[1])
        ret, _, _ = g.run(node, command)
        self.assertEqual(ret, 0, "Command execution not successful")

        # triggering heal
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, "Heal not triggered")

        # waiting for heal to complete
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=120)
        self.assertTrue(ret, "Heal not completed")

        # Try accessing the file content from the mount
        cmd = ("for i in `seq 1 10`; do cat %s/file.$i > /dev/null;"
               "for j in `seq 1 5` ; do cat %s/dir.$i/file.$j > /dev/null;"
               "done ; done"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Unable to access the file contents")
        g.log.info("File contents are accessible")

        # checking if file is in split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, "File still in split-brain")
        g.log.info("Successfully resolved split brain situation using "
                   "CLI based resolution")

        # Check arequals for all the bricks
        g.log.info('Getting arequal for all the bricks after heal...')
        self.verify_brick_arequals()
        g.log.info('Getting arequal after heal is successful')
