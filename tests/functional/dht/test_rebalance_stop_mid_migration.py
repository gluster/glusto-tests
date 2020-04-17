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
"""
Description:
    This test case creates a large file at mount point,
    adds extra brick and initiates rebalance. While
    migration is in progress, it stops rebalance process
    and checks if it has stopped.
"""

from glusto.core import Glusto as g
from glustolibs.gluster import constants as k
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.volume_libs import form_bricks_list_to_add_brick
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import get_pathinfo
from glustolibs.gluster.rebalance_ops import (rebalance_start, rebalance_stop)
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'arbiter', 'distributed-arbiter', 'disperse',
           'distributed-dispersed'],
          ['glusterfs']])
class TestDhtClass(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to create volume")
        g.log.info("Successful in cleaning up Volume %s", self.volname)

    def tearDown(self):

        # Cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Successful in cleaning up Volume %s", self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_rebalance_stop_with_large_file(self):
        """
        Testcase Steps:
        1. Create and start a volume.
        2. Mount volume on client and create a large file.
        3. Add bricks to the volume and check layout
        4. Rename the file such that it hashs to different
           subvol.
        5. Start rebalance on volume.
        6. Stop rebalance on volume.
        """
        # Create file BIG1.
        command = ("dd if=/dev/urandom of={}/BIG1 bs=1024K count=10000"
                   .format(self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Unable to create file I/O failed")
        g.log.info('Successfully created file BIG1.')

        # Checking if file created on correct subvol or not.
        ret = validate_files_in_dir(
            self.mounts[0].client_system,
            self.mounts[0].mountpoint,
            file_type=k.FILETYPE_FILES,
            test_type=k.TEST_FILE_EXISTS_ON_HASHED_BRICKS)
        self.assertTrue(ret, "Files not created on correct subvol.")
        g.log.info("File BIG1 is on correct subvol according to "
                   "the hash value")

        # Adding brick to volume
        add_brick_list = form_bricks_list_to_add_brick(self.mnode,
                                                       self.volname,
                                                       self.servers,
                                                       self.all_servers_info)
        ret, _, _ = add_brick(self.mnode, self.volname, add_brick_list)
        self.assertEqual(ret, 0, "Unable to add bricks to volume")
        g.log.info("Successfully added bricks to volume.")

        # Check if brick is added successfully or not.
        current_bricks = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(current_bricks, "Unable to get "
                             "current active bricks of volume")
        g.log.info("Successfully got active bricks of volume.")
        for brick in add_brick_list:
            self.assertIn(brick, current_bricks,
                          ("Brick %s is not added to volume" % brick))

        # Create directory testdir.
        ret = mkdir(self.mounts[0].client_system,
                    self.mounts[0].mountpoint + '/testdir')
        self.assertTrue(ret, "Failed to create testdir directory")
        g.log.info("Successfuly created testdir directory.")

        # Layout should be set on the new brick and should be
        # continous and complete
        ret = validate_files_in_dir(self.mounts[0].client_system,
                                    self.mounts[0].mountpoint + '/testdir',
                                    test_type=k.TEST_LAYOUT_IS_COMPLETE)
        self.assertTrue(ret, "Layout not set for the new subvol")
        g.log.info("New subvol has been added successfully")

        # Rename file so that it gets hashed to different subvol
        file_index = 0
        path_info_dict = get_pathinfo(self.mounts[0].client_system,
                                      self.mounts[0].mountpoint + '/BIG1')
        initial_brick_set = path_info_dict['brickdir_paths']

        while True:
            # Calculate old_filename and new_filename and rename.
            file_index += 1
            old_filename = "{}/BIG{}".format(self.mounts[0].mountpoint,
                                             file_index)
            new_filename = "{}/BIG{}".format(self.mounts[0].mountpoint,
                                             (file_index+1))
            ret, _, _ = g.run(self.mounts[0].client_system,
                              "mv {} {}".format(old_filename, new_filename))
            self.assertEqual(ret, 0, "Rename not successful")

            # Checking if it was moved to new subvol or not.
            path_info_dict = get_pathinfo(self.mounts[0].client_system,
                                          self.mounts[0].mountpoint +
                                          '/BIG%d' % (file_index+1))
            if path_info_dict['brickdir_paths'] != initial_brick_set:
                break
        g.log.info("file renamed successfully")

        # Start rebalance on volume
        ret, _, _ = rebalance_start(self.mnode, self.volname, fix_layout=False)
        self.assertEqual(ret, 0, "Rebalance did not start")
        g.log.info("Rebalance started successfully on volume %s", self.volname)

        # Stop rebelance on volume
        ret, _, _ = rebalance_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Rebalance stop command did not execute.")
        g.log.info("Rebalance stopped successfully on volume %s",
                   self.volname)

        # Get rebalance status in xml
        command = ("gluster volume rebalance {} status --xml"
                   .format(self.volname))
        ret, _, _ = g.run(self.mnode, command)
        self.assertEqual(ret, 1,
                         "Unexpected: Rebalance still running "
                         "even after stop.")
        g.log.info("Rebalance is not running after stop.")
