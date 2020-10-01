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
from random import sample

from glusto.core import Glusto as g
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           are_bricks_offline)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.heal_libs import (is_volume_in_split_brain,
                                          monitor_heal_completion)
from glustolibs.gluster.lib_utils import collect_bricks_arequal
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online, get_subvols, expand_volume,
    wait_for_volume_process_to_be_online)
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['arbiter', 'distributed-arbiter', 'replicated',
           'distributed-replicated'], ['glusterfs']])
class TestAfrSelfHealAddBrickRebalance(GlusterBaseClass):

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
        g.log.info("Successfully uploaded IO scripts to clients % s",
                   cls.clients)

    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        if not self.setup_volume_and_mount_volume(self.mounts):
            raise ExecutionError("Unable to setup and mount volume")

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

        # Calling GlusterBaseClass Teardown
        self.get_super_method(self, 'tearDown')()

    def test_afr_self_heal_add_brick_rebalance(self):
        """
        Test Steps:
        1. Create a replicated/distributed-replicate volume and mount it
        2. Start IO from the clients
        3. Bring down a brick from the subvol and validate it is offline
        4. Bring back the brick online and wait for heal to complete
        5. Once the heal is completed, expand the volume.
        6. Trigger rebalance and wait for rebalance to complete
        7. Validate IO, no errors during the steps performed from step 2
        8. Check arequal of the subvol and all the brick in the same subvol
        should have same checksum
        """
        # Start IO from the clients
        self.all_mounts_procs = []
        for count, mount_obj in enumerate(self.mounts):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d --dir-depth 3 --dir-length 5 "
                   "--max-num-of-dirs 5 --num-of-files 30 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # List a brick in each subvol and bring them offline
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        brick_to_bring_offline = []
        for subvol in subvols:
            self.assertTrue(subvol, "List is empty")
            brick_to_bring_offline.extend(sample(subvol, 1))

        ret = bring_bricks_offline(self.volname, brick_to_bring_offline)
        self.assertTrue(ret, "Unable to bring brick: {} offline".format(
            brick_to_bring_offline))

        # Validate the brick is offline
        ret = are_bricks_offline(self.mnode, self.volname,
                                 brick_to_bring_offline)
        self.assertTrue(ret, "Brick:{} is still online".format(
            brick_to_bring_offline))

        # Wait for 10 seconds for IO to be generated
        sleep(10)

        # Start volume with force to bring all bricks online
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Volume start with force failed")
        g.log.info("Volume: %s started successfully", self.volname)

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))

        # Monitor heal completion
        self.assertTrue(monitor_heal_completion(self.mnode, self.volname,
                                                interval_check=10),
                        "Heal failed after 20 mins")

        # Check are there any files in split-brain and heal completion
        self.assertFalse(is_volume_in_split_brain(self.mnode, self.volname),
                         "Some files are in split brain for "
                         "volume: {}".format(self.volname))

        # Expanding volume by adding bricks to the volume when IO in progress
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume when IO in "
                              "progress on volume %s", self.volname))

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))

        # Start Rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the "
                   "volume %s", self.volname)

        # Without sleep the next step will fail with Glusterd Syncop locking.
        sleep(2)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1800)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on "
                   "the volume %s", self.volname)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        self.all_mounts_procs *= 0

        # List all files and dirs created
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")

        # Check arequal checksum of all the bricks is same
        for subvol in subvols:
            ret, arequal_from_the_bricks = collect_bricks_arequal(subvol)
            self.assertTrue(ret, "Arequal is collected successfully across "
                                 "the bricks in the subvol {}".format(subvol))
            cmd = len(set(arequal_from_the_bricks))
            if (self.volume_type == "arbiter" or
                    self.volume_type == "distributed-arbiter"):
                cmd = len(set(arequal_from_the_bricks[:2]))
            self.assertEqual(cmd, 1, "Arequal"
                             " is same on all the bricks in the subvol")
