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
from glustolibs.gluster.lib_utils import collect_bricks_arequal
from glustolibs.gluster.heal_ops import trigger_heal_full
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain,
                                          is_shd_daemonized)
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online,
    get_subvols)
from glustolibs.gluster.brick_libs import (select_bricks_to_bring_offline,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete,
                                 validate_io_procs,
                                 collect_mounts_arequal)


@runs_on([['distributed-replicated', 'replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class TestAFRSelfHeal(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
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
        self.all_mounts_procs, self.io_validation_complete = [], False

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Checking if failure occure before I/O was complete
        if not self.io_validation_complete:
            ret = wait_for_io_to_complete(self.all_mounts_procs,
                                          self.mounts[0])
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

            # List all files and dirs created
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_replace_brick_self_heal_io_in_progress(self):
        """
        - Create directory on mount point and write files/dirs
        - Create another set of files (1K files)
        - While creation of files/dirs are in progress Kill one brick
        - Remove the contents of the killed brick(simulating disk replacement)
        - When the IO's are still in progress, restart glusterd on the nodes
          where we simulated disk replacement to bring back bricks online
        - Start volume heal
        - Wait for IO's to complete
        - Verify whether the files are self-healed
        - Calculate arequals of the mount point and all the bricks
        """
        # pylint: disable=too-many-locals,too-many-statements,too-many-branches
        # Create dirs with files
        g.log.info('Creating dirs with file...')
        command = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "-d 2 -l 2 -n 2 -f 10 %s"
                   % (self.script_upload_path, self.mounts[0].mountpoint))
        ret, _, err = g.run(self.mounts[0].client_system, command,
                            user=self.mounts[0].user)
        self.assertFalse(ret, err)
        g.log.info("IO is successful")

        # Creating another set of files (1K files)
        self.all_mounts_procs = []

        # Create dirs with files
        g.log.info('Creating 1K files...')
        command = ("/usr/bin/env python %s create_files "
                   "-f 1500 --fixed-file-size 10k %s"
                   % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts[0])
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = list(filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks'])))

        # Bring brick offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Remove the content of the killed bricks
        for brick in bricks_to_bring_offline:
            brick_node, brick_path = brick.split(':')

            # Removing files
            command = ('cd %s ; rm -rf *' % brick_path)
            ret, _, err = g.run(brick_node, command)
            self.assertFalse(ret, err)
            g.log.info('Files are deleted on brick %s', brick)

        # Bring brick online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode,
                                                   self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online"
                              % self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Wait for self-heal-daemons to be online
        ret = is_shd_daemonized(self.all_servers)
        self.assertTrue(ret, "Either No self heal daemon process found")
        g.log.info("All self-heal daemons are online")

        # Start healing
        ret = trigger_heal_full(self.mnode, self.volname)
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

        # Check arequals for "replicated"
        all_bricks = get_all_bricks(self.mnode, self.volname)
        if self.volume_type == "replicated":

            # Get arequal after bricks are online
            ret, arequals = collect_mounts_arequal(self.mounts)
            self.assertTrue(ret, 'Failed to get arequal')
            g.log.info('Getting arequal after successfully bringing'
                       'bricks online.')
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

        # Check arequals for "distributed-replicated"
        if self.volume_type == "distributed-replicated":

            # Get the subvolumes
            subvols_dict = get_subvols(self.mnode, self.volname)
            num_subvols = len(subvols_dict['volume_subvols'])
            g.log.info("Number of subvolumes in volume %s:", num_subvols)

            # Get arequals and compare
            for i in range(0, num_subvols):

                # Get arequal for first brick
                subvol_brick_list = subvols_dict['volume_subvols'][i]
                ret, arequal = collect_bricks_arequal(subvol_brick_list[0])
                self.assertTrue(ret, 'Failed to get arequal on first brick')
                first_brick_total = arequal[0].splitlines()[-1].split(':')[-1]

                # Get arequal for every brick and compare with first brick
                ret, arequals = collect_bricks_arequal(subvol_brick_list)
                self.assertTrue(ret, 'Failed to get arequal on bricks')
                for arequal in arequals:
                    brick_total = arequal.splitlines()[-1].split(':')[-1]
                    self.assertEqual(first_brick_total, brick_total,
                                     'Arequals for subvol and brick are '
                                     'not equal')
                    g.log.info('Arequals for subvol and brick are equal')
