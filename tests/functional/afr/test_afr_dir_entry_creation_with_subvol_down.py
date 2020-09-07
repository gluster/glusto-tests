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

from time import sleep

from glusto.core import Glusto as g
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           are_bricks_offline)
from glustolibs.gluster.dht_test_utils import (create_brickobjectlist,
                                               find_specific_hashed)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import file_exists
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online, get_subvols)
from glustolibs.gluster.mount_ops import umount_volume, mount_volume


@runs_on([['distributed-arbiter', 'distributed-replicated'], ['glusterfs']])
class TestAfrDirEntryCreationWithSubvolDown(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Changing the distrubte count to 3 as per the test.
        self.volume['voltype']['dist_count'] = 3
        # Setup volume and mount it on three clients.
        if not self.setup_volume_and_mount_volume(mounts=[self.mounts[0]]):
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    def tearDown(self):

        # Unmount and cleanup the volume
        if not self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]]):
            raise ExecutionError("Unable to unmount and cleanup volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _check_file_exists(self, subvol, directory, exists=True):
        """ Validates given directory present on brick path of each subvol """
        for each_brick in subvol:
            node, brick_path = each_brick.split(":")
            path = brick_path + directory
            ret = file_exists(node, path)
            self.assertEqual(exists, ret, "Unexpected behaviour, existence "
                             "check of directory {} on brick returned"
                             " {}".format(directory, each_brick))

    def _create_file(self, location, file_name):
        """ Creates a file with file_name on the specified location"""
        source_file = "{}/{}".format(location, file_name)
        ret, _, err = g.run(self.mounts[0].client_system,
                            ("touch %s" % source_file))
        self.assertEqual(ret, 0, ("Failed to create {} on {}: err"
                                  " {}".format(source_file, location, err)))
        g.log.info("Successfully created %s on: %s", file_name, location)

    def _create_number_of_files_on_the_subvol(self, subvol_object, directory,
                                              number_of_files, mountpath):
        """Creates number of files specified on the given subvol"""
        name = None
        for _ in range(number_of_files):
            hashed = find_specific_hashed(self.subvols, directory,
                                          subvol_object, existing_names=name)
            self.assertIsNotNone(hashed, "Couldn't find a subvol to "
                                 "create a file.")
            self._create_file(mountpath, hashed.newname)
            name = hashed.newname

    def test_afr_dir_entry_creation_with_subvol_down(self):
        """
        1. Create a distributed-replicated(3X3)/distributed-arbiter(3X(2+1))
           and mount it on one client
        2. Kill 3 bricks corresponding to the 1st subvol
        3. Unmount and remount the volume on the same client
        4. Create deep dir from mount point
           mkdir -p dir1/subdir1/deepdir1
        5. Create files under dir1/subdir1/deepdir1; touch <filename>
        6. Now bring all sub-vols up by volume start force
        7. Validate backend bricks for dir creation, the subvol which is
           offline will have no dirs created, whereas other subvols will have
           dirs created from step 4
        8. Trigger heal from client by "#find . | xargs stat"
        9. Verify that the directory entries are created on all back-end bricks
        10. Create new dir (dir2) on location dir1/subdir1/deepdir1
        11. Trigger rebalance and wait for the completion
        12. Check backend bricks for all entries of dirs
        13. Check if files are getting created on the subvol which was offline
        """
        # Bring down first subvol of bricks offline
        self.subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        first_subvol = self.subvols[0]
        ret = bring_bricks_offline(self.volname, first_subvol)
        self.assertTrue(ret, "Unable to bring {} bricks offline".
                        format(first_subvol))

        # Check bricks are offline or not
        ret = are_bricks_offline(self.mnode, self.volname, first_subvol)
        self.assertTrue(ret, "Bricks {} are still online".format(first_subvol))

        # Unmount and remount the volume
        ret, _, _ = umount_volume(
            self.mounts[0].client_system, self.mounts[0].mountpoint)
        self.assertFalse(ret, "Failed to unmount volume.")
        ret, _, _ = mount_volume(self.volname, self.mount_type,
                                 self.mounts[0].mountpoint, self.mnode,
                                 self.mounts[0].client_system)
        self.assertFalse(ret, "Failed to remount volume.")
        g.log.info('Successfully umounted and remounted volume.')

        # At this step, sleep is must otherwise file creation will fail
        sleep(2)

        # Create dir `dir1/subdir1/deepdir1` on mountpont
        directory1 = "dir1/subdir1/deepdir1"
        path = self.mounts[0].mountpoint + "/" + directory1
        ret = mkdir(self.mounts[0].client_system, path, parents=True)
        self.assertTrue(ret, "Directory {} creation failed".format(path))

        # Create files on the 2nd and 3rd subvols which are online
        brickobject = create_brickobjectlist(self.subvols, directory1)
        self.assertIsNotNone(brickobject, "Failed to get brick object list")
        self._create_number_of_files_on_the_subvol(
            brickobject[1], directory1, 5, mountpath=path)
        self._create_number_of_files_on_the_subvol(
            brickobject[2], directory1, 5, mountpath=path)

        # Bring bricks online using volume start force
        ret, _, err = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, err)
        g.log.info("Volume: %s started successfully", self.volname)

        # Check all bricks are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, "Few process after volume start are offline for "
                             "volume: {}".format(self.volname))

        # Validate Directory is not created on the bricks of the subvol which
        # is offline
        for subvol in self.subvols:
            self._check_file_exists(subvol, "/" + directory1,
                                    exists=(subvol != first_subvol))

        # Trigger heal from the client
        cmd = "cd {}; find . | xargs stat".format(self.mounts[0].mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, err)

        # Validate the directory1 is present on all the bricks
        for subvol in self.subvols:
            self._check_file_exists(subvol, "/" + directory1, exists=True)

        # Create new dir (dir2) on location dir1/subdir1/deepdir1
        directory2 = "/" + directory1 + '/dir2'
        path = self.mounts[0].mountpoint + directory2
        ret = mkdir(self.mounts[0].client_system, path, parents=True)
        self.assertTrue(ret, "Directory {} creation failed".format(path))

        # Trigger rebalance and validate the completion
        ret, _, err = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, err)
        g.log.info("Rebalance on volume %s started successfully", self.volname)
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, "Rebalance didn't complete on the volume: "
                             "{}".format(self.volname))

        # Validate all dirs are present on all bricks in each subvols
        for subvol in self.subvols:
            for each_dir in ("/" + directory1, directory2):
                self._check_file_exists(subvol, each_dir, exists=True)

        # Validate if files are getting created on the subvol which was
        # offline
        self._create_number_of_files_on_the_subvol(
            brickobject[0], directory1, 5, mountpath=path)
