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
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.rebalance_ops import (
    wait_for_remove_brick_to_complete, get_remove_brick_status)
from glustolibs.gluster.volume_libs import form_bricks_list_to_remove_brick
from glustolibs.misc.misc_libs import upload_scripts, kill_process
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed-replicated', 'distributed-arbiter'], ['glusterfs']])
class TestKillBrickWithRemoveBrick(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Changing dist_count to 3
        self.volume['voltype']['dist_count'] = 3

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_kill_brick_with_remove_brick(self):
        """
        Test case:
        1. Create a volume, start it and mount it.
        2. Create some data on the volume.
        3. Start remove-brick on the volume.
        4. When remove-brick is in progress kill brick process of a brick
           which is being remove.
        5. Remove-brick should complete without any failures.
        """
        # Start I/O from clients on the volume
        counter = 1
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d --dir-depth 2 "
                   "--dir-length 10 --max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path,
                       counter, mount_obj.mountpoint))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, "Failed to create datat on volume")
            counter += 10

        # Collect arequal checksum before ops
        arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        # Start remove-brick on the volume
        brick_list = form_bricks_list_to_remove_brick(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Brick list is empty")

        ret, _, _ = remove_brick(self.mnode, self.volname, brick_list, 'start')
        self.assertFalse(ret, "Failed to start remove-brick on volume")
        g.log.info("Successfully started remove-brick on volume")

        # Check rebalance is in progress
        ret = get_remove_brick_status(self.mnode, self.volname, brick_list)
        ret = ret['aggregate']['statusStr']
        self.assertEqual(ret, "in progress", ("Rebalance is not in "
                                              "'in progress' state, either "
                                              "rebalance is in completed state"
                                              " or failed to get rebalance "
                                              "status"))

        # kill brick process of a brick which is being removed
        brick = choice(brick_list)
        node, _ = brick.split(":")
        ret = kill_process(node, process_names="glusterfsd")
        self.assertTrue(ret, "Failed to kill brick process of brick %s"
                        % brick)

        # Wait for remove-brick to complete on the volume
        ret = wait_for_remove_brick_to_complete(self.mnode, self.volname,
                                                brick_list, timeout=1200)
        self.assertTrue(ret, "Remove-brick didn't complete")
        g.log.info("Remove brick completed successfully")

        # Check for data loss by comparing arequal before and after ops
        arequal_checksum_after = collect_mounts_arequal(self.mounts[0])
        self.assertEqual(arequal_checksum_before, arequal_checksum_after,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")
