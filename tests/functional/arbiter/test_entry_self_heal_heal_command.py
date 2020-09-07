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
from glustolibs.gluster.brick_libs import (select_bricks_to_bring_offline,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline)
from glustolibs.gluster.heal_libs import (
    monitor_heal_completion,
    is_heal_complete,
    is_volume_in_split_brain,
    is_shd_daemonized)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal,
                                 list_all_files_and_dirs_mounts)


@runs_on([['arbiter', 'distributed-arbiter'],
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
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
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

    def test_entry_self_heal_heal_command(self):
        """
        Test Entry-Self-Heal (heal command)

        Description:
        - set the volume option
        "metadata-self-heal": "off"
        "entry-self-heal": "off"
        "data-self-heal": "off"
        - create IO
        - get arequal before getting bricks offline
        - set the volume option
        "self-heal-daemon": "off"
        - bring down all bricks processes from selected set
        - get arequal after getting bricks offline and compare with
        arequal after bringing bricks offline
        - modify the data
        - get arequal before getting bricks online
        - bring bricks online
        - set the volume option
        "self-heal-daemon": "on"
        - check daemons and start healing
        - check if heal is completed
        - check for split-brain
        - get arequal after getting bricks online and compare with
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
        g.log.info("Starting IO on %s:%s", self.mounts[0].client_system,
                   self.mounts[0].mountpoint)
        cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
               "--dir-length 2 "
               "--dir-depth 2 "
               "--max-num-of-dirs 2 "
               "--num-of-files 20 %s/files" % (
                   self.script_upload_path,
                   self.mounts[0].mountpoint))
        ret, _, err = g.run(self.mounts[0].client_system, cmd,
                            user=self.mounts[0].user)
        self.assertFalse(ret, 'Failed to create the data for %s: %s'
                         % (self.mounts[0].mountpoint, err))
        g.log.info('Created IO for %s is successfully',
                   self.mounts[0].mountpoint)

        # Command list to do different operations with data -
        # create, rename, copy and delete
        cmds = (
            "/usr/bin/env python %s create_files -f 20 %s/files",
            "/usr/bin/env python %s mv %s/files",
            # 'copy' command works incorrect. disable until fixed
            # "/usr/bin/env python %s copy --dest-dir %s/new_dir %s/files",
            "/usr/bin/env python %s delete %s",
        )
        for cmd in cmds:
            # Get arequal before getting bricks offline
            g.log.info('Getting arequal before getting bricks offline...')
            ret, arequals = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            result_before_offline = arequals[0].splitlines()[-1].split(':')[-1]
            g.log.info('Getting arequal before getting bricks offline '
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
            bricks_to_bring_offline = (
                bricks_to_bring_offline_dict['volume_bricks'])

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

            # Get arequal after getting bricks offline
            g.log.info('Getting arequal after getting bricks offline...')
            ret, arequals = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            result_after_offline = arequals[0].splitlines()[-1].split(':')[-1]
            g.log.info('Getting arequal after getting bricks offline '
                       'is successful')

            # Checking arequals before bringing bricks offline
            # and after bringing bricks offline
            self.assertEqual(result_before_offline, result_after_offline,
                             'Checksums are not equal')
            g.log.info('Checksums before bringing bricks offline '
                       'and after bringing bricks offline are equal')

            # Modify the data
            g.log.info("Start modifying IO on all mounts...")
            g.log.info("Modifying IO on %s:%s", self.mounts[0].client_system,
                       self.mounts[0].mountpoint)
            if 'copy --dest-dir' in cmd:
                parsed_cmd = cmd % (self.script_upload_path,
                                    self.mounts[0].mountpoint,
                                    self.mounts[0].mountpoint)
            else:
                parsed_cmd = cmd % (self.script_upload_path,
                                    self.mounts[0].mountpoint)
            ret, _, err = g.run(self.mounts[0].client_system, parsed_cmd,
                                user=self.mounts[0].user)
            self.assertFalse(ret, 'Failed to modify the data for %s: %s'
                             % (self.mounts[0].mountpoint, err))
            g.log.info('Modified IO for %s is successfully',
                       self.mounts[0].mountpoint)

            # Get arequal before getting bricks online
            g.log.info('Getting arequal before getting bricks online...')
            ret, arequals = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            result_before_online = arequals[0].splitlines()[-1].split(':')[-1]
            g.log.info('Getting arequal before getting bricks online '
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

            # Get arequal after getting bricks online
            g.log.info('Getting arequal after getting bricks online...')
            ret, arequals = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            result_after_online = arequals[0].splitlines()[-1].split(':')[-1]
            g.log.info('Getting arequal after getting bricks online '
                       'is successful')

            # List all files and dirs created
            g.log.info("List all files and directories:")
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

            # Checking arequals before bringing bricks online
            # and after bringing bricks online
            self.assertEqual(result_before_online, result_after_online,
                             'Checksums are not equal')
            g.log.info('Checksums before bringing bricks online '
                       'and after bringing bricks online are equal')
