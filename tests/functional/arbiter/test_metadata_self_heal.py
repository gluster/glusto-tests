#  Copyright (C) 2015-2021 Red Hat, Inc. <http://www.redhat.com>
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
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain,
                                          is_shd_daemonized)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal,
                                 wait_for_io_to_complete)


@runs_on([['arbiter', 'distributed-arbiter'],
          ['glusterfs', 'nfs']])
class TestMetadataSelfHeal(GlusterBaseClass):
    """
    Description:
        Test cases related to metadata delf heal
        in default configuration of the volume
    """

    @classmethod
    def create_user(cls, host, user):
        """
        Creates a user on a host
        """
        g.log.info("Creating user '%s' for %s...", user, host)
        command = "useradd %s" % user
        _, _, err = g.run(host, command)

        if 'already exists' in err:
            g.log.warn("User '%s' is already exists on %s", user, host)
        else:
            g.log.info("User '%s' is created successfully on %s", user, host)

    @classmethod
    def delete_user(cls, host, user):
        """
        Deletes a user on a host
        """
        g.log.info('Deleting user %s from %s...', user, host)
        command = "userdel -r %s" % user
        _, _, err = g.run(host, command)

        if 'does not exist' in err:
            g.log.warn('User %s is already deleted on %s', user, host)
        else:
            g.log.info('User %s successfully deleted on %s', user, host)

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

        # Create user qa
        for mount_object in self.mounts:
            self.create_user(mount_object.client_system, 'qa')

        for server in self.servers:
            self.create_user(server, 'qa')

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

        # Delete user
        for mount_object in self.mounts:
            self.delete_user(mount_object.client_system, 'qa')

        for server in self.servers:
            self.delete_user(server, 'qa')

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

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
        all_mounts_procs = []
        test_meta_data_self_heal_folder = 'test_meta_data_self_heal'
        g.log.info("Generating data for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)

        # Create files
        g.log.info('Creating files...')
        command = ("cd %s/ ; "
                   "mkdir %s ;"
                   "cd %s/ ;"
                   "for i in `seq 1 50` ; "
                   "do dd if=/dev/urandom of=test.$i bs=10k count=1 ; "
                   "done ;"
                   % (self.mounts[0].mountpoint,
                      test_meta_data_self_heal_folder,
                      test_meta_data_self_heal_folder))

        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # wait for io to complete
        self.assertTrue(
            wait_for_io_to_complete(all_mounts_procs, self.mounts),
            "Io failed to complete on some of the clients")

        # Setting options
        g.log.info('Setting options...')
        options = {"self-heal-daemon": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Option 'self-heal-daemon' is set to 'off' successfully")

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = bricks_to_bring_offline_dict['volume_bricks']

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
        g.log.info("Modifying data for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)

        # Change permissions to 444
        g.log.info('Changing permissions...')
        command = ("cd %s/%s/ ; "
                   "chmod -R 444 *"
                   % (self.mounts[0].mountpoint,
                      test_meta_data_self_heal_folder))
        ret, out, err = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, err)
        g.log.info('Permissions are changed successfully')

        # Change the ownership to qa
        g.log.info('Changing the ownership...')
        command = ("cd %s/%s/ ; "
                   "chown -R qa *"
                   % (self.mounts[0].mountpoint,
                      test_meta_data_self_heal_folder))
        ret, out, err = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, err)
        g.log.info('Ownership is changed successfully')

        # Change the group to qa
        g.log.info('Changing the group...')
        command = ("cd %s/%s/ ; "
                   "chgrp -R qa *"
                   % (self.mounts[0].mountpoint,
                      test_meta_data_self_heal_folder))
        ret, out, err = g.run(self.mounts[0].client_system, command)
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
        self.assertEqual(sorted(result_before_online),
                         sorted(result_after_online),
                         'Checksums are not equal')
        g.log.info('Checksums before bringing bricks online '
                   'and after bringing bricks online are equal')

        # Adding servers and client in single dict to check permissions
        nodes_to_check = {}
        all_bricks = get_all_bricks(self.mnode, self.volname)
        for brick in all_bricks:
            node, brick_path = brick.split(':')
            nodes_to_check[node] = brick_path
        nodes_to_check[self.mounts[0].client_system] = \
            self.mounts[0].mountpoint

        # Checking for user and group
        for node in nodes_to_check:
            # Get file list
            command = ("cd %s/%s/ ; "
                       "ls"
                       % (nodes_to_check[node],
                          test_meta_data_self_heal_folder))
            ret, out, err = g.run(node, command)
            file_list = out.split()

            for file_name in file_list:
                file_to_check = '%s/%s/%s' % (nodes_to_check[node],
                                              test_meta_data_self_heal_folder,
                                              file_name)

                g.log.info('Checking for permissions, user and group for %s',
                           file_name)

                # Check for permissions
                cmd = ("stat -c '%a %n' {} | awk '{{print $1}}'"
                       .format(file_to_check))
                ret, permissions, _ = g.run(node, cmd)
                self.assertEqual(permissions.split('\n')[0], '444',
                                 'Permissions %s is not equal to 444'
                                 % permissions)
                g.log.info("Permissions are '444' for %s", file_name)

                # Check for user
                cmd = ("ls -ld {} | awk '{{print $3}}'"
                       .format(file_to_check))
                ret, username, _ = g.run(node, cmd)
                self.assertEqual(username.split('\n')[0],
                                 'qa', 'User %s is not equal qa'
                                 % username)
                g.log.info("User is 'qa' for %s", file_name)

                # Check for group
                cmd = ("ls -ld {} | awk '{{print $4}}'"
                       .format(file_to_check))
                ret, groupname, _ = g.run(node, cmd)
                self.assertEqual(groupname.split('\n')[0],
                                 'qa', 'Group %s is not equal qa'
                                 % groupname)
                g.log.info("Group is 'qa' for %s", file_name)
