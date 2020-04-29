#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
# pylint: disable=too-many-locals,too-many-statements,too-many-branches

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.glusterdir import get_dir_contents
from glustolibs.gluster.glusterfile import get_file_stat
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain)
from glustolibs.gluster.lib_utils import (add_user, del_user,
                                          collect_bricks_arequal)
from glustolibs.gluster.mount_ops import (umount_volume,
                                          mount_volume)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete,
                                 collect_mounts_arequal)


@runs_on([['distributed-replicated', 'replicated'],
          ['glusterfs']])
class TestAFRMetaDataSelfHealClientSideHeal(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):

        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        if not upload_scripts(cls.clients, [cls.script_upload_path]):
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):

        self.get_super_method(self, 'setUp')()
        self.all_mounts_procs, self.io_validation_complete = [], False

        # Create users
        self.users = ['qa_func', 'qa_system', 'qa_perf', 'qa_all']
        for mount_object in self.mounts:
            for user in self.users:
                if not add_user(mount_object.client_system, user):
                    raise ExecutionError("Failed to create user "
                                         "{}".format(user))
        g.log.info("Successfully created all users.")

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status.
        Cleanup and umount volume
        """
        if not self.io_validation_complete:
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

            # List all files and dirs created
            if not list_all_files_and_dirs_mounts(self.mounts):
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

        # Delete user
        for mount_object in self.mounts:
            for user in self.users:
                if not del_user(mount_object.client_system, user):
                    raise ExecutionError("Failed to delete user: {}"
                                         .format(user))
        g.log.info("Successfully deleted all users")

        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        self.get_super_method(self, 'tearDown')()

    def trigger_heal_from_mount_point(self):
        """
        Trigger heal from mount point using read.
        """
        # Unmouting and remounting volume to update the volume graph
        # in client.
        ret, _, _ = umount_volume(
            self.mounts[0].client_system, self.mounts[0].mountpoint)
        self.assertFalse(ret, "Failed to unmount volume.")

        ret, _, _ = mount_volume(
            self.volname, 'glusterfs', self.mounts[0].mountpoint,
            self.mnode, self.mounts[0].client_system)
        self.assertFalse(ret, "Failed to remount volume.")
        g.log.info('Successfully umounted and remounted volume.')

        # Trigger heal from client side
        cmd = ("/usr/bin/env python {0} read {1}/{2}".format(
            self.script_upload_path, self.mounts[0].mountpoint,
            self.test_meta_data_self_heal_folder))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, 'Failed to trigger heal on %s'
                         % self.mounts[0].client_system)
        g.log.info("Successfully triggered heal from mount point.")

    def validate_io_on_clients(self):
        """
        Validate I/O on client mount points.
        """
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

    def check_arequal_from_mount_point_and_bricks(self):
        """
        Check if arequals of mount point and bricks are
        are the same.
        """
        # Check arequals for "replicated"
        all_bricks = get_all_bricks(self.mnode, self.volname)
        if self.volume_type == "replicated":
            # Get arequal before getting bricks offline
            ret, arequals = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            g.log.info('Getting arequal before getting bricks offline '
                       'is successful')
            mount_point_total = arequals[0].splitlines()[-1].split(':')[-1]

            # Get arequal on bricks and compare with mount_point_total
            ret, arequals = collect_bricks_arequal(all_bricks)
            self.assertTrue(ret, 'Failed to get arequal on bricks')
            for arequal in arequals:
                brick_total = arequal.splitlines()[-1].split(':')[-1]
                self.assertEqual(mount_point_total, brick_total,
                                 'Arequals for mountpoint and brick '
                                 'are not equal')
                g.log.info('Arequals for mountpoint and brick are equal')
            g.log.info('All arequals are equal for replicated')

        # Check arequals for "distributed-replicated"
        if self.volume_type == "distributed-replicated":
            # get the subvolumes
            subvols_dict = get_subvols(self.mnode, self.volname)
            num_subvols = len(subvols_dict['volume_subvols'])
            g.log.info("Number of subvolumes in volume %s:", num_subvols)

            # Get arequals and compare
            for i in range(0, num_subvols):
                # Get arequal for first brick
                subvol_brick_list = subvols_dict['volume_subvols'][i]
                ret, arequal = collect_bricks_arequal([subvol_brick_list[0]])
                self.assertTrue(ret, 'Failed to get arequal on first')

                # Get arequal for every brick and compare with first brick
                first_brick_total = arequal[0].splitlines()[-1].split(':')[-1]
                ret, arequals = collect_bricks_arequal(subvol_brick_list)
                self.assertTrue(ret, 'Failed to get arequal on bricks')
                for arequal in arequals:
                    brick_total = arequal.splitlines()[-1].split(':')[-1]
                    self.assertEqual(first_brick_total, brick_total,
                                     'Arequals for subvol and brick are '
                                     'not equal')
                    g.log.info('Arequals for subvol and brick are equal')
            g.log.info('All arequals are equal for distributed-replicated')

    def check_permssions_on_bricks(self, bricks_list):
        """
        Check permssions on a given set of bricks.
        """
        for brick in bricks_list:
            node, brick_path = brick.split(':')
            dir_list = get_dir_contents(node, "{}/{}".format(
                brick_path, self.test_meta_data_self_heal_folder))
            self.assertIsNotNone(dir_list, "Dir list from "
                                 "brick is empty")
            g.log.info("Successfully got dir list from bick")

            # Verify changes for dirs
            for folder in dir_list:
                ret = get_file_stat(node, "{}/{}/{}".format(
                    brick_path, self.test_meta_data_self_heal_folder, folder))

                self.assertEqual('555', ret['access'],
                                 "Permissions mismatch on node {}"
                                 .format(node))

                self.assertEqual('1003', ret['gid'],
                                 "Group mismatch on node {}"
                                 .format(node))

                # Get list of files for each dir
                file_list = get_dir_contents(node, "{}/{}/{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    folder))
                self.assertIsNotNone(file_list, "File list from "
                                     "brick is empty.")
                g.log.info("Successfully got file list from bick.")

                # Verify for group for each file
                if file_list:
                    for file_name in file_list:
                        ret = get_file_stat(node, "{}/{}/{}/{}".format(
                            brick_path, self.test_meta_data_self_heal_folder,
                            folder, file_name))

                        self.assertEqual('1003', ret['gid'],
                                         "Group mismatch on node {}"
                                         .format(node))

            # Verify permissions for files in dirs 1..50
            for i in range(1, 51):

                file_list = get_dir_contents(node, "{}/{}/dir.{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    str(i)))
                self.assertIsNotNone(file_list, "File list from "
                                     "brick is empty.")
                g.log.info("Successfully got file list from bick.")

                if file_list:
                    for file_name in file_list:

                        ret = get_file_stat(node, "{}/{}/dir.{}/{}".format(
                            brick_path, self.test_meta_data_self_heal_folder,
                            str(i), file_name))
                        self.assertEqual('666', ret['access'],
                                         "Permissions mismatch on node {}"
                                         .format(node))

            # Verify permissions for files in dirs 51..100
            for i in range(51, 101):

                file_list = get_dir_contents(node, "{}/{}/dir.{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    str(i)))
                self.assertIsNotNone(file_list, "File list from "
                                     "brick is empty.")
                g.log.info("Successfully got file list from bick.")

                if file_list:
                    for file_name in file_list:

                        ret = get_file_stat(node, "{}/{}/dir.{}/{}".format(
                            brick_path, self.test_meta_data_self_heal_folder,
                            str(i), file_name))
                        self.assertEqual('444', ret['access'],
                                         "Permissions mismatch on node {}"
                                         .format(node))

            # Verify ownership for dirs 1..35
            for i in range(1, 36):

                ret = get_file_stat(node, "{}/{}/dir.{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    str(i)))
                self.assertEqual('1000', ret['uid'],
                                 "User id mismatch on node {}"
                                 .format(node))

                # Verify ownership for files in dirs
                file_list = get_dir_contents(node, "{}/{}/dir.{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    str(i)))
                self.assertIsNotNone(file_list, "File list from "
                                     "brick is empty.")
                g.log.info("Successfully got file list from bick.")

                if file_list:
                    for file_name in file_list:

                        ret = get_file_stat(node, "{}/{}/dir.{}/{}".format(
                            brick_path, self.test_meta_data_self_heal_folder,
                            str(i), file_name))
                        self.assertEqual('1000', ret['uid'],
                                         "User id mismatch on node {}"
                                         .format(node))

            # Verify ownership for dirs 36..70
            for i in range(36, 71):

                ret = get_file_stat(node, "{}/{}/dir.{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    str(i)))
                self.assertEqual('1001', ret['uid'],
                                 "User id mismatch on node {}"
                                 .format(node))

                # Verify ownership for files in dirs
                file_list = get_dir_contents(node, "{}/{}/dir.{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    str(i)))
                self.assertIsNotNone(file_list, "File list from "
                                     "brick is empty.")
                g.log.info("Successfully got file list from bick.")

                if file_list:
                    for file_name in file_list:

                        ret = get_file_stat(node, "{}/{}/dir.{}/{}".format(
                            brick_path, self.test_meta_data_self_heal_folder,
                            str(i), file_name))
                        self.assertEqual('1001', ret['uid'],
                                         "User id mismatch on node {}"
                                         .format(node))

            # Verify ownership for dirs 71..100
            for i in range(71, 101):

                ret = get_file_stat(node, "{}/{}/dir.{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    str(i)))
                self.assertEqual('1002', ret['uid'],
                                 "User id mismatch on node {}"
                                 .format(node))

                # Verify ownership for files in dirs
                file_list = get_dir_contents(node, "{}/{}/dir.{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    str(i)))
                self.assertIsNotNone(file_list, "File list from "
                                     "brick is empty.")
                g.log.info("Successfully got file list from bick.")

                if file_list:
                    for file_name in file_list:

                        ret = get_file_stat(node, "{}/{}/dir.{}/{}".format(
                            brick_path, self.test_meta_data_self_heal_folder,
                            str(i), file_name))
                        self.assertEqual('1002', ret['uid'],
                                         "User id mismatch on node {}"
                                         .format(node))

    def test_metadata_self_heal_client_side_heal(self):
        """
        Testcase steps:
        1.Turn off the options self heal daemon
        2.Create IO
        3.Calculate arequal of the bricks and mount point
        4.Bring down "brick1" process
        5.Change the permissions of the directories and files
        6.Change the ownership of the directories and files
        7.Change the group of the directories and files
        8.Bring back the brick "brick1" process
        9.Execute "find . | xargs stat" from the mount point to trigger heal
        10.Verify the changes in permissions are not self healed on brick1
        11.Verify the changes in permissions on all bricks but brick1
        12.Verify the changes in ownership are not self healed on brick1
        13.Verify the changes in ownership on all the bricks but brick1
        14.Verify the changes in group are not successfully self-healed
           on brick1
        15.Verify the changes in group on all the bricks but brick1
        16.Turn on the option metadata-self-heal
        17.Execute "find . | xargs md5sum" from the mount point to trgger heal
        18.Wait for heal to complete
        19.Verify the changes in permissions are self-healed on brick1
        20.Verify the changes in ownership are successfully self-healed
           on brick1
        21.Verify the changes in group are successfully self-healed on brick1
        22.Calculate arequal check on all the bricks and mount point
        """
        # Setting options
        ret = set_volume_options(self.mnode, self.volname,
                                 {"self-heal-daemon": "off"})
        self.assertTrue(ret, 'Failed to set options self-heal-daemon '
                        'and metadata-self-heal to OFF')
        g.log.info("Options are set successfully")

        # Creating files on client side
        self.test_meta_data_self_heal_folder = 'test_meta_data_self_heal'
        for mount_object in self.mounts:
            command = ("cd {0}/ ; mkdir {1} ; cd {1}/ ;"
                       "for i in `seq 1 100` ; "
                       "do mkdir dir.$i ; "
                       "for j in `seq 1 5` ; "
                       "do dd if=/dev/urandom of=dir.$i/file.$j "
                       "bs=1K count=$j ; done ; done ;".format
                       (mount_object.mountpoint,
                        self.test_meta_data_self_heal_folder))
            proc = g.run_async(mount_object.client_system, command,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        self.validate_io_on_clients()

        # Calculate and check arequal of the bricks and mount point
        self.check_arequal_from_mount_point_and_bricks()

        # Select bricks to bring offline from a replica set
        subvols_dict = get_subvols(self.mnode, self.volname)
        subvols = subvols_dict['volume_subvols']
        bricks_to_bring_offline = []
        bricks_to_be_online = []
        for subvol in subvols:
            bricks_to_bring_offline.append(subvol[0])
            for brick in subvol[1:]:
                bricks_to_be_online.append(brick)

        # Bring bricks offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Change the permissions of the directories and files
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            command = ('cd {}/{}; '
                       'for i in `seq 1 100` ; '
                       'do chmod 555 dir.$i ; done ; '
                       'for i in `seq 1 50` ; '
                       'do for j in `seq 1 5` ; '
                       'do chmod 666 dir.$i/file.$j ; done ; done ; '
                       'for i in `seq 51 100` ; '
                       'do for j in `seq 1 5` ; '
                       'do chmod 444 dir.$i/file.$j ; done ; done ;'
                       .format(mount_obj.mountpoint,
                               self.test_meta_data_self_heal_folder))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        self.validate_io_on_clients()

        # Change the ownership of the directories and files
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            command = ('cd {}/{} ; '
                       'for i in `seq 1 35` ; '
                       'do chown -R qa_func dir.$i ; done ; '
                       'for i in `seq 36 70` ; '
                       'do chown -R qa_system dir.$i ; done ; '
                       'for i in `seq 71 100` ; '
                       'do chown -R qa_perf dir.$i ; done ;'
                       .format(mount_obj.mountpoint,
                               self.test_meta_data_self_heal_folder))
            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        self.validate_io_on_clients()

        # Change the group of the directories and files
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            command = ('cd {}/{}; '
                       'for i in `seq 1 100` ; '
                       'do chgrp -R qa_all dir.$i ; done ;'
                       .format(mount_obj.mountpoint,
                               self.test_meta_data_self_heal_folder))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        self.validate_io_on_clients()

        # Bring brick online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Trigger heal from mount point
        self.trigger_heal_from_mount_point()

        # Verify the changes are not self healed on brick1 for each subvol
        for brick in bricks_to_bring_offline:
            node, brick_path = brick.split(':')

            dir_list = get_dir_contents(node, "{}/{}".format(
                brick_path, self.test_meta_data_self_heal_folder))
            self.assertIsNotNone(dir_list, "Dir list from "
                                 "brick is empty")
            g.log.info("Successfully got dir list from bick")

            # Verify changes for dirs
            for folder in dir_list:

                ret = get_file_stat(node, "{}/{}/{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    folder))

                self.assertEqual('755', ret['access'],
                                 "Permissions mismatch on node {}"
                                 .format(node))

                self.assertEqual('root', ret['username'],
                                 "User id mismatch on node {}"
                                 .format(node))

                self.assertEqual('root', ret['groupname'],
                                 "Group id mismatch on node {}"
                                 .format(node))

                # Get list of files for each dir
                file_list = get_dir_contents(node, "{}/{}/{}".format(
                    brick_path, self.test_meta_data_self_heal_folder,
                    folder))
                self.assertIsNotNone(file_list, "File list from "
                                     "brick is empty.")
                g.log.info("Successfully got file list from bick.")

                if file_list:
                    for file_name in file_list:

                        ret = get_file_stat(node, "{}/{}/{}/{}".format(
                            brick_path, self.test_meta_data_self_heal_folder,
                            folder, file_name))

                        self.assertEqual('644', ret['access'],
                                         "Permissions mismatch on node"
                                         " {} for file {}".format(node,
                                                                  file_name))

                        self.assertEqual('root', ret['username'],
                                         "User id mismatch on node"
                                         " {} for file {}".format(node,
                                                                  file_name))

                        self.assertEqual('root', ret['groupname'],
                                         "Group id mismatch on node"
                                         " {} for file {}".format(node,
                                                                  file_name))

        # Verify the changes are self healed on all bricks except brick1
        # for each subvol
        self.check_permssions_on_bricks(bricks_to_be_online)

        # Setting options
        ret = set_volume_options(self.mnode, self.volname,
                                 {"metadata-self-heal": "on"})
        self.assertTrue(ret, 'Failed to set options to ON.')
        g.log.info("Options are set successfully")

        # Trigger heal from mount point
        self.trigger_heal_from_mount_point()

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

        # Verify the changes are self healed on brick1 for each subvol
        self.check_permssions_on_bricks(bricks_to_bring_offline)

        # Calculate and check arequal of the bricks and mount point
        self.check_arequal_from_mount_point_and_bricks()
