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
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.lib_utils import get_size_of_mountpoint


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed'], ['glusterfs']])
class TestSparseFileCreationAndDeletion(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Changing dist_count to 5
        self.volume['voltype']['dist_count'] = 5

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

        # Assign a variable for the first_client
        self.first_client = self.mounts[0].client_system

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def _create_two_sparse_files(self):
        """Create 2 sparse files from /dev/zero and /dev/null"""

        # Create a tuple to hold both the file names
        self.sparse_file_tuple = (
            "{}/sparse_file_zero".format(self.mounts[0].mountpoint),
            "{}/sparse_file_null".format(self.mounts[0].mountpoint)
            )

        # Create 2 spares file where one is created from /dev/zero and
        # another is created from /dev/null
        for filename, input_file in ((self.sparse_file_tuple[0], "/dev/zero"),
                                     (self.sparse_file_tuple[1], "/dev/null")):
            cmd = ("dd if={} of={} bs=1M seek=5120 count=1000"
                   .format(input_file, filename))
            ret, _, _ = g.run(self.first_client, cmd)
            self.assertEqual(ret, 0, 'Failed to create %s ' % filename)

        g.log.info("Successfully created sparse_file_zero and"
                   " sparse_file_null")

    def _check_du_and_ls_of_sparse_file(self):
        """Check du and ls -lks on spare files"""

        for filename in self.sparse_file_tuple:

            # Fetch output of ls -lks for the sparse file
            cmd = "ls -lks {}".format(filename)
            ret, out, _ = g.run(self.first_client, cmd)
            self.assertEqual(ret, 0, "Failed to get ls -lks for file %s "
                             % filename)
            ls_value = out.split(" ")[5]

            # Fetch output of du for the sparse file
            cmd = "du --block-size=1 {}".format(filename)
            ret, out, _ = g.run(self.first_client, cmd)
            self.assertEqual(ret, 0, "Failed to get du for file %s "
                             % filename)
            du_value = out.split("\t")[0]

            # Compare du and ls -lks value
            self. assertNotEqual(ls_value, du_value,
                                 "Unexpected: Sparse file size coming up same "
                                 "for du and ls -lks")

        g.log.info("Successfully checked sparse file size using ls and du")

    def _delete_two_sparse_files(self):
        """Delete sparse files"""

        for filename in self.sparse_file_tuple:
            cmd = "rm -rf {}".format(filename)
            ret, _, _ = g.run(self.first_client, cmd)
            self.assertEqual(ret, 0, 'Failed to delete %s ' % filename)

        g.log.info("Successfully remove both sparse files")

    def test_sparse_file_creation_and_deletion(self):
        """
        Test case:
        1. Create volume with 5 sub-volumes, start and mount it.
        2. Check df -h for available size.
        3. Create 2 sparse file one from /dev/null and one from /dev/zero.
        4. Find out size of files and compare them through du and ls.
           (They shouldn't match.)
        5. Check df -h for available size.(It should be less than step 2.)
        6. Remove the files using rm -rf.
        """
        # Check df -h for avaliable size
        available_space_at_start = get_size_of_mountpoint(
            self.first_client, self.mounts[0].mountpoint)
        self.assertIsNotNone(available_space_at_start,
                             "Failed to get available space on mount point")

        # Create 2 sparse file one from /dev/null and one from /dev/zero
        self._create_two_sparse_files()

        # Find out size of files and compare them through du and ls
        # (They shouldn't match)
        self._check_du_and_ls_of_sparse_file()

        # Check df -h for avaliable size(It should be less than step 2)
        available_space_now = get_size_of_mountpoint(
            self.first_client, self.mounts[0].mountpoint)
        self.assertIsNotNone(available_space_now,
                             "Failed to get avaliable space on mount point")
        ret = (int(available_space_at_start) > int(available_space_now))
        self.assertTrue(ret, "Available space at start not less than "
                        "available space now")

        # Remove the files using rm -rf
        self._delete_two_sparse_files()

        # Sleep for 180 seconds for the meta data in .glusterfs directory
        # to be removed
        sleep(180)

        # Check df -h after removing sparse files
        available_space_now = get_size_of_mountpoint(
            self.first_client, self.mounts[0].mountpoint)
        self.assertIsNotNone(available_space_now,
                             "Failed to get avaliable space on mount point")
        ret = int(available_space_at_start) - int(available_space_now) < 1500
        self.assertTrue(ret, "Available space at start and available space now"
                        " is not equal")
