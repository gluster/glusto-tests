#  Copyright (C) 2015-2018  Red Hat, Inc. <http://www.redhat.com>
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

import time
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.io.utils import wait_for_io_to_complete
from glustolibs.gluster.glusterfile import (get_fattr, get_file_stat)


@runs_on([['replicated'],
          ['glusterfs']])
class HealGfidTest(GlusterBaseClass):
    """
     Description:
     Verify that files created from the backend without gfid gets assigned one
     and are healed when accessed from the client.
    """
    @classmethod
    def setUpClass(cls):

        GlusterBaseClass.setUpClass.im_func(cls)

        # Override Volumes
        if cls.volume_type == "replicated":
            # Define 1x3 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp'}

    def setUp(self):
        GlusterBaseClass.setUp.im_func(self)
        g.log.info("Starting to Setup Volume %s", self.volname)

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Cleanup and umount volume
        """
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    def verify_gfid_and_link_count(self, dirname, filename):
        """
        check that the dir and all files under it have the same gfid on all 3
        bricks and that they have the .glusterfs entry as well.
        """
        dir_gfids = dict()
        file_gfids = dict()
        bricks_list = get_all_bricks(self.mnode, self.volname)
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")

            ret = get_fattr(brick_node, '%s/%s' % (brick_path, dirname),
                            'trusted.gfid')
            self.assertIsNotNone(ret, "trusted.gfid is not presented "
                                 "on %s/%s" % (brick_path, dirname))
            dir_gfids.setdefault(dirname, []).append(ret)

            ret = get_fattr(brick_node, '%s/%s/%s' %
                            (brick_path, dirname, filename), 'trusted.gfid')
            self.assertIsNotNone(ret, "trusted.gfid is not presented on "
                                 "%s/%s/%s" % (brick_path, dirname, filename))
            file_gfids.setdefault(filename, []).append(ret)

            stat_data = get_file_stat(brick_node, "%s/%s/%s" %
                                      (brick_path, dirname, filename))
            self.assertEqual(stat_data["links"], "2", 'Link count is not 2')

        for key in dir_gfids:
            self.assertTrue(all(value == dir_gfids[key][0]
                                for value in dir_gfids[key]), 'gfids do not '
                            'match for %s on all bricks' % dirname)
        for key in file_gfids:
            self.assertTrue(all(value == file_gfids[key][0]
                                for value in file_gfids[key]), 'gfids do not '
                            'match for %s/%s on all bricks' % (dirname,
                                                               filename))

    def test_gfid_heal(self):
        """
        - Create a 1x3 volume and fuse mount it.
        - Create 1 directory with 1 file inside it directly on each brick.
        - Access the directories from the mount.
        - Launch heals and verify that the heals are over.
        - Verify that the files and directories have gfid assigned.
        """
        # pylint: disable=too-many-statements

        # Create data on the bricks.
        g.log.info("Creating directories and files on the backend.")
        bricks_list = get_all_bricks(self.mnode, self.volname)
        i = 0
        for brick in bricks_list:
            i += 1
            brick_node, brick_path = brick.split(":")
            ret, _, _ = g.run(brick_node, "mkdir %s/dir%d" % (brick_path, i))
            self.assertEqual(ret, 0, "Dir creation failed on %s" % brick_path)
            ret, _, _ = g.run(brick_node, "touch %s/dir%d/file%d"
                              % (brick_path, i, i))
            self.assertEqual(ret, 0, "file creation failed on %s" % brick_path)
        g.log.info("Created directories and files on the backend.")

        # To circumvent is_fresh_file() check in glusterfs code.
        time.sleep(2)

        # Access files from mount
        for i in range(1, 4):
            cmd = ("ls %s/dir%d/file%d" % (self.mounts[0].mountpoint, i, i))
            ret, _, _ = g.run(self.clients[0], cmd)
            self.assertEqual(ret, 0, "Failed to access dir%d/file%d on %s"
                             % (i, i, self.mounts[0].mountpoint))

        # Trigger heal
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Starting heal failed')
        g.log.info('Index heal launched')

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Verify gfid and links at the backend.
        self.verify_gfid_and_link_count("dir1", "file1")
        self.verify_gfid_and_link_count("dir2", "file2")
        self.verify_gfid_and_link_count("dir3", "file3")
