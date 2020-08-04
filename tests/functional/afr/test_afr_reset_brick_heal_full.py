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

from random import choice

from glusto.core import Glusto as g
from glustolibs.gluster.brick_ops import reset_brick
from glustolibs.gluster.brick_libs import (get_all_bricks, are_bricks_offline)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import rmdir
from glustolibs.gluster.glusterfile import remove_file
from glustolibs.gluster.heal_ops import trigger_heal_full
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.lib_utils import collect_bricks_arequal
from glustolibs.gluster.volume_libs import (
    get_subvols, wait_for_volume_process_to_be_online)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs, wait_for_io_to_complete)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs']])
class TestAfrResetBrickHeal(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload IO scripts for running IO on mounts
        cls.script_upload_path = (
            "/usr/share/glustolibs/io/scripts/file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients {}".
                                 format(cls.clients))

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup volume and mount it.
        if not self.setup_volume_and_mount_volume(self.mounts):
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    def tearDown(self):
        # Wait if any IOs are pending from the test
        if self.all_mounts_procs:
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if ret:
                raise ExecutionError(
                    "Wait for IO completion failed on some of the clients")

        # Unmount and cleanup the volume
        if not self.unmount_volume_and_cleanup_volume(self.mounts):
            raise ExecutionError("Unable to unmount and cleanup volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    @classmethod
    def tearDownClass(cls):
        for each_client in cls.clients:
            ret = remove_file(each_client, cls.script_upload_path)
            if not ret:
                raise ExecutionError("Failed to delete file {}".
                                     format(cls.script_upload_path))

        cls.get_super_method(cls, 'tearDownClass')()

    def test_afr_reset_brick_heal_full(self):
        """
         1. Create files/dirs from mount point
         2. With IO in progress execute reset-brick start
         3. Now format the disk from back-end, using rm -rf <brick path>
         4. Execute reset brick commit and check for the brick is online.
         5. Issue volume heal using "gluster vol heal <volname> full"
         6. Check arequal for all bricks to verify all backend bricks
            including the resetted brick have same data
        """
        self.all_mounts_procs = []
        for count, mount_obj in enumerate(self.mounts):
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d --dir-depth 3 --dir-length 5 "
                   "--max-num-of-dirs 5 --num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        all_bricks = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(all_bricks, "Unable to fetch bricks of volume")
        brick_to_reset = choice(all_bricks)

        # Start reset brick
        ret, _, err = reset_brick(self.mnode, self.volname,
                                  src_brick=brick_to_reset, option="start")
        self.assertEqual(ret, 0, err)
        g.log.info("Reset brick: %s started", brick_to_reset)

        # Validate the brick is offline
        ret = are_bricks_offline(self.mnode, self.volname, [brick_to_reset])
        self.assertTrue(ret, "Brick:{} is still online".format(brick_to_reset))

        # rm -rf of the brick directory
        node, brick_path = brick_to_reset.split(":")
        ret = rmdir(node, brick_path, force=True)
        self.assertTrue(ret, "Unable to delete the brick {} on "
                             "node {}".format(brick_path, node))

        # Reset brick commit
        ret, _, err = reset_brick(self.mnode, self.volname,
                                  src_brick=brick_to_reset, option="commit")
        self.assertEqual(ret, 0, err)
        g.log.info("Reset brick committed successfully")

        # Check the brick is online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Few volume processess are offline for the "
                             "volume: {}".format(self.volname))

        # Trigger full heal
        ret = trigger_heal_full(self.mnode, self.volname)
        self.assertTrue(ret, "Unable  to trigger the heal full command")

        # Wait for the heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "Heal didn't complete in 20 mins time")

        # Validate io on the clients
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on the mounts")
        self.all_mounts_procs *= 0

        # Check arequal of the back-end bricks after heal completion
        all_subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        for subvol in all_subvols:
            ret, arequal_from_subvol = collect_bricks_arequal(subvol)
            self.assertTrue(ret, "Arequal is collected successfully across the"
                            " bricks in the subvol {}".format(subvol))
            self.assertEqual(len(set(arequal_from_subvol)), 1, "Arequal is "
                             "same on all the bricks in the subvol")
