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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from random import choice

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           are_bricks_online,
                                           get_all_bricks)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_volume_in_split_brain,
                                          is_heal_complete)
from glustolibs.gluster.lib_utils import collect_bricks_arequal
from glustolibs.gluster.volume_libs import (get_subvols,
                                            replace_brick_from_volume)
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed-replicated', 'replicated'], ['glusterfs']])
class TestHealWithLinkFiles(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to setup and mount volume")

        self.first_client = self.mounts[0].client_system
        self.mountpoint = self.mounts[0].mountpoint

    def tearDown(self):

        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _create_files_and_dirs_on_mount_point(self, second_attempt=False):
        """A function to create files and dirs on mount point"""
        # Create a parent directory test_link_self_heal on mount point
        if not second_attempt:
            ret = mkdir(self.first_client,
                        '{}/{}'.format(self.mountpoint,
                                       'test_link_self_heal'))
            self.assertTrue(ret, "Failed to create dir test_link_self_heal")

        # Create dirctories and files inside directory test_link_self_heal
        io_cmd = ("for i in `seq 1 5`; do mkdir dir.$i; "
                  "for j in `seq 1 10`; do dd if=/dev/random "
                  "of=dir.$i/file.$j bs=1k count=$j; done; done")
        if second_attempt:
            io_cmd = ("for i in `seq 1 5` ; do for j in `seq 1 10`; "
                      "do dd if=/dev/random of=sym_link_dir.$i/"
                      "new_file.$j bs=1k count=$j; done; done ")
        cmd = ("cd {}/test_link_self_heal;{}".format(self.mountpoint, io_cmd))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create dirs and files inside")

    def _create_soft_links_to_directories(self):
        """Create soft links to directories"""
        cmd = ("cd {}/test_link_self_heal; for i in `seq 1 5`; do ln -s "
               "dir.$i sym_link_dir.$i; done".format(self.mountpoint))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create soft links to dirs")

    def _verify_soft_links_to_dir(self, option=0):
        """Verify soft links to dir"""

        cmd_list = [
            ("for i in `seq 1 5`; do stat -c %F sym_link_dir.$i | "
             "grep -F 'symbolic link'; if [ $? -ne 0 ]; then exit 1;"
             " fi ; done; for i in `seq 1 5` ; do readlink sym_link_dir.$i | "
             "grep \"dir.$i\"; if [ $? -ne 0 ]; then exit 1; fi; done; "),
            ("for i in `seq 1 5`; do for j in `seq 1 10`; do ls "
             "dir.$i/new_file.$j; if [ $? -ne 0 ]; then exit 1; fi; done; "
             "done")]

        # Generate command to check according to option
        if option == 2:
            verify_cmd = "".join(cmd_list)
        else:
            verify_cmd = cmd_list[option]

        cmd = ("cd {}/test_link_self_heal; {}".format(self.mountpoint,
                                                      verify_cmd))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Symlinks aren't proper")

    def _create_hard_links_to_files(self, second_attempt=False):
        """Create hard links to files"""
        io_cmd = ("for i in `seq 1 5`;do for j in `seq 1 10`;do ln "
                  "dir.$i/file.$j dir.$i/link_file.$j;done; done")
        if second_attempt:
            io_cmd = ("for i in `seq 1 5`; do mkdir new_dir.$i; for j in "
                      "`seq 1 10`; do ln dir.$i/file.$j new_dir.$i/new_file."
                      "$j;done; done;")

        cmd = ("cd {}/test_link_self_heal;{}".format(self.mountpoint, io_cmd))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create hard links to files")

    def _verify_hard_links_to_files(self, second_set=False):
        """Verify if hard links to files"""
        file_to_compare = "dir.$i/link_file.$j"
        if second_set:
            file_to_compare = "new_dir.$i/new_file.$j"

        cmd = ("cd {}/test_link_self_heal;for i in `seq 1 5`; do for j in `seq"
               " 1 10`;do if [ `stat -c %i dir.$i/file.$j` -ne `stat -c %i "
               "{}` ];then exit 1; fi; done; done"
               .format(self.mountpoint, file_to_compare))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to verify hard links to files")

    def _bring_bricks_offline(self):
        """Brings bricks offline and confirms if they are offline"""
        # Select bricks to bring offline from a replica set
        subvols_dict = get_subvols(self.mnode, self.volname)
        subvols = subvols_dict['volume_subvols']
        self.bricks_to_bring_offline = []
        for subvol in subvols:
            self.bricks_to_bring_offline.append(subvol[0])

        # Bring bricks offline
        ret = bring_bricks_offline(self.volname, self.bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        self.bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 self.bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % self.bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   self.bricks_to_bring_offline)

    def _restart_volume_and_bring_all_offline_bricks_online(self):
        """Restart volume and bring all offline bricks online"""
        ret = bring_bricks_online(self.mnode, self.volname,
                                  self.bricks_to_bring_offline,
                                  bring_bricks_online_methods=[
                                      'volume_start_force'])
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        self.bricks_to_bring_offline)

        # Check if bricks are back online or not
        ret = are_bricks_online(self.mnode, self.volname,
                                self.bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks not online %s even after restart' %
                        self.bricks_to_bring_offline)

        g.log.info('Bringing bricks %s online is successful',
                   self.bricks_to_bring_offline)

    def _check_arequal_on_bricks_with_a_specific_arequal(self, arequal,
                                                         brick_list):
        """
        Compare an inital arequal checksum with bricks from a given brick list
        """
        init_val = arequal[0].splitlines()[-1].split(':')[-1]
        ret, arequals = collect_bricks_arequal(brick_list)
        self.assertTrue(ret, 'Failed to get arequal on bricks')
        for brick_arequal in arequals:
            brick_total = brick_arequal.splitlines()[-1].split(':')[-1]
            self.assertEqual(init_val, brick_total, 'Arequals not matching')

    def _check_arequal_checksum_for_the_volume(self):
        """
        Check if arequals of mount point and bricks are
        are the same.
        """
        if self.volume_type == "replicated":
            # Check arequals for "replicated"
            brick_list = get_all_bricks(self.mnode, self.volname)

            # Get arequal before getting bricks offline
            ret, arequals = collect_mounts_arequal([self.mounts[0]])
            self.assertTrue(ret, 'Failed to get arequal')
            g.log.info('Getting arequal before getting bricks offline '
                       'is successful')

            # Get arequal on bricks and compare with mount_point_total
            self._check_arequal_on_bricks_with_a_specific_arequal(
                arequals, brick_list)

        # Check arequals for "distributed-replicated"
        if self.volume_type == "distributed-replicated":
            # Get the subvolumes
            subvols_dict = get_subvols(self.mnode, self.volname)
            num_subvols = len(subvols_dict['volume_subvols'])

            # Get arequals and compare
            for i in range(0, num_subvols):
                # Get arequal for first brick
                brick_list = subvols_dict['volume_subvols'][i]
                ret, arequals = collect_bricks_arequal([brick_list[0]])
                self.assertTrue(ret, 'Failed to get arequal on first brick')

                # Get arequal for every brick and compare with first brick
                self._check_arequal_on_bricks_with_a_specific_arequal(
                    arequals, brick_list)

    def _check_heal_is_completed_and_not_in_split_brain(self):
        """Check if heal is completed and volume not in split brain"""
        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Check if volume is in split brian or not
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')

    def _check_if_there_are_files_and_dirs_to_be_healed(self):
        """Check if there are files and dirs to be healed"""
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertFalse(ret, 'Heal is completed')
        g.log.info('Heal is pending')

    def _wait_for_heal_is_completed(self):
        """Check if heal is completed"""
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=3600)
        self.assertTrue(ret, 'Heal has not yet completed')

    def _replace_one_random_brick(self):
        """Replace one random brick from the volume"""
        brick = choice(get_all_bricks(self.mnode, self.volname))
        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        self.servers, self.all_servers_info,
                                        src_brick=brick)
        self.assertTrue(ret, "Failed to replace brick %s " % brick)
        g.log.info("Successfully replaced brick %s", brick)

    def test_self_heal_of_hard_links(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create a directory and create files and directories inside it
           on mount point.
        3. Collect and compare arequal-checksum according to the volume type
           for bricks.
        4. Bring down brick processes accoding to the volume type.
        5. Create hard links for the files created in step 2.
        6. Check if heal info is showing all the files and dirs to be healed.
        7. Bring brack all brick processes which were killed.
        8. Wait for heal to complete on the volume.
        9. Check if heal is complete and check if volume is in split brain.
        10. Collect and compare arequal-checksum according to the volume type
            for bricks.
        11. Verify if hard links are proper or not.
        12. Do a lookup on mount point.
        13. Bring down brick processes accoding to the volume type.
        14. Create a second set of hard links to the files.
        15. Check if heal info is showing all the files and dirs to be healed.
        16. Bring brack all brick processes which were killed.
        17. Wait for heal to complete on the volume.
        18. Check if heal is complete and check if volume is in split brain.
        19. Collect and compare arequal-checksum according to the volume type
            for bricks.
        20. Verify both set of hard links are proper or not.
        21. Do a lookup on mount point.
        22. Pick a random brick and replace it.
        23. Wait for heal to complete on the volume.
        24. Check if heal is complete and check if volume is in split brain.
        25. Collect and compare arequal-checksum according to the volume type
            for bricks.
        26. Verify both set of hard links are proper or not.
        27. Do a lookup on mount point.
        """
        # Create a directory and create files and directories inside it
        # on mount point
        self._create_files_and_dirs_on_mount_point()

        # Collect and compare arequal-checksum according to the volume type
        # for bricks
        self._check_arequal_checksum_for_the_volume()
        for attempt in (False, True):

            # Bring down brick processes accoding to the volume type
            self._bring_bricks_offline()

            # Create hardlinks for the files created in step 2
            self._create_hard_links_to_files(second_attempt=attempt)

            # Check if heal info is showing all the files and dirs to
            # be healed
            self._check_if_there_are_files_and_dirs_to_be_healed()

            # Bring back all brick processes which were killed
            self._restart_volume_and_bring_all_offline_bricks_online()

            # Wait for heal to complete on the volume
            self._wait_for_heal_is_completed()

            # Check if heal is complete and check if volume is in split brain
            self._check_heal_is_completed_and_not_in_split_brain()

            # Collect and compare arequal-checksum according to the volume
            # type for bricks
            self._check_arequal_checksum_for_the_volume()

            # Verify if hard links are proper or not
            self._verify_hard_links_to_files()
            if attempt:
                self._verify_hard_links_to_files(second_set=attempt)

        # Pick a random brick and replace it
        self._replace_one_random_brick()

        # Wait for heal to complete on the volume
        self._wait_for_heal_is_completed()

        # Check if heal is complete and check if volume is in split brain
        self._check_heal_is_completed_and_not_in_split_brain()

        # Collect and compare arequal-checksum according to the volume
        # type for bricks
        self._check_arequal_checksum_for_the_volume()

        # Verify if hard links are proper or not
        self._verify_hard_links_to_files()
        self._verify_hard_links_to_files(second_set=True)

    def test_self_heal_of_soft_links(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create a directory and create files and directories inside it
           on mount point.
        3. Collect and compare arequal-checksum according to the volume type
           for bricks.
        4. Bring down brick processes accoding to the volume type.
        5. Create soft links for the dirs created in step 2.
        6. Verify if soft links are proper or not.
        7. Add files through the soft links.
        8. Verify if the soft links are proper or not.
        9. Check if heal info is showing all the files and dirs to be healed.
        10. Bring brack all brick processes which were killed.
        11. Wait for heal to complete on the volume.
        12. Check if heal is complete and check if volume is in split brain.
        13. Collect and compare arequal-checksum according to the volume type
            for bricks.
        14. Verify if soft links are proper or not.
        15. Do a lookup on mount point.
        """
        # Create a directory and create files and directories inside it
        # on mount point
        self._create_files_and_dirs_on_mount_point()

        # Collect and compare arequal-checksum according to the volume type
        # for bricks
        self._check_arequal_checksum_for_the_volume()

        # Bring down brick processes accoding to the volume type
        self._bring_bricks_offline()

        # Create soft links for the dirs created in step 2
        self._create_soft_links_to_directories()

        # Verify if soft links are proper or not
        self._verify_soft_links_to_dir()

        # Add files through the soft links
        self._create_files_and_dirs_on_mount_point(second_attempt=True)

        # Verify if the soft links are proper or not
        self._verify_soft_links_to_dir(option=1)

        # Check if heal info is showing all the files and dirs to
        # be healed
        self._check_if_there_are_files_and_dirs_to_be_healed()

        # Bring back all brick processes which were killed
        self._restart_volume_and_bring_all_offline_bricks_online()

        # Wait for heal to complete on the volume
        self._wait_for_heal_is_completed()

        # Check if heal is complete and check if volume is in split brain
        self._check_heal_is_completed_and_not_in_split_brain()

        # Verify if soft links are proper or not
        self._verify_soft_links_to_dir(option=2)
