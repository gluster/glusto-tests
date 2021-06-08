#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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
    Test self heal when data-self-heal-algorithm option is set to full.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           are_bricks_offline)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.heal_libs import (is_volume_in_split_brain,
                                          monitor_heal_completion)
from glustolibs.gluster.lib_utils import collect_bricks_arequal
from glustolibs.gluster.volume_ops import (volume_start,
                                           set_volume_options)
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online, get_subvols)

# pylint: disable=too-many-statements


@runs_on([['arbiter', 'distributed-arbiter', 'replicated',
           'distributed-replicated'], ['glusterfs']])
class TestSelfHealWithFullAlgorithm(GlusterBaseClass):
    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        if not self.setup_volume_and_mount_volume(self.mounts):
            raise ExecutionError("Unable to setup and mount volume")
        g.log.info("Volume created and mounted successfully")

    def tearDown(self):

        # Unmount and cleanup the volume
        if not self.unmount_volume_and_cleanup_volume(self.mounts):
            raise ExecutionError("Unable to unmount and cleanup volume")
        g.log.info("Volume unmounted and deleted successfully")

        # Calling GlusterBaseClass Teardown
        self.get_super_method(self, 'tearDown')()

    def check_full_data_heal(self, brick_set):
        # Bring specified set of bricks in each subvolume offline, pump I/O,
        # bring the bricks online, wait for heal and check arequal.
        mountpoint = self.mounts[0].mountpoint
        client = self.mounts[0].client_system
        bricks_to_bring_offline = []
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        bricks_list = zip(*subvols)

        bricks_to_bring_offline = list(bricks_list[brick_set])

        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, "Unable to bring brick: {} offline".format(
            bricks_to_bring_offline))

        # Validate the bricks are offline
        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, "Brick:{} is still online".format(
            bricks_to_bring_offline))

        if brick_set == 0:
            # Create few files under the directory with data
            cmd = ("cd %s/test_full_self_heal ; for i in `seq 1 100` ; "
                   "do dd if=/dev/urandom of=file.$i bs=1M count=1; done;"
                   % (mountpoint))
        else:
            # Modify files under test_full_self_heal directory
            cmd = ("cd %s/test_full_self_heal ; for i in `seq 1 100` ; "
                   "do truncate -s 0 file.$i ; truncate -s 2M file.$i ; done;"
                   % (mountpoint))

        ret, _, _ = g.run(client, cmd)
        self.assertFalse(ret, "Failed to create file on mountpoint")
        g.log.info("Successfully created files on mountpoint")

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
                                                interval_check=10), "Heal "
                        "failed after 20 mins")

        # Check are there any files in split-brain
        self.assertFalse(is_volume_in_split_brain(self.mnode, self.volname),
                         "Some files are in split brain for volume : {}"
                         .format(self.volname))

        # Check arequal checksum of all the bricks are same
        for subvol in subvols:
            ret, arequal_from_the_bricks = collect_bricks_arequal(subvol)
            self.assertFalse(ret, "Failed to collect arequal on the bricks in "
                                  "subvol {}".format(subvol))
            cmd = len(set(arequal_from_the_bricks))
            if (self.volume_type == "arbiter" or
                    self.volume_type == "distributed-arbiter"):
                cmd = len(set(arequal_from_the_bricks[:2]))
            self.assertEqual(cmd, 1, "Arequal is same on all the bricks in the"
                                     " subvol")

    def test_self_heal_with_full_algorithm(self):
        """
        Test Steps:
        1. Create a replicated/distributed-replicate volume and mount it
        2. Set data/metadata/entry-self-heal to off and
           data-self-heal-algorithm to full
        3. Create a directory from the mount point
        4. Bring down all bricks from a selected set
        5. Create few files inside the directory with some data
        6. Bring back the bricks online and wait for heal to complete
        7. Check arequal of the subvol and all the bricks in the subvol should
           have same checksum
        8. Bring down all bricks from another set
        9. Modify the data of existing files under the directory
        10. Bring back the bricks online and wait for heal to complete
        11. Check arequal of the subvol and all the brick in the same subvol
            should have same checksum
        """

        # Setting options
        for key, value in (("data-self-heal", "off"),
                           ("metadata-self-heal", "off"),
                           ("entry-self-heal", "off"),
                           ("data-self-heal-algorithm", "full")):
            ret = set_volume_options(self.mnode, self.volname, {key: value})
            self.assertTrue(ret, 'Failed to set %s to %s.' % (key, value))
            g.log.info("%s set to %s successfully", key, value)

        # Create a directory under the mount point
        mountpoint = self.mounts[0].mountpoint
        client = self.mounts[0].client_system

        cmd = ("mkdir %s/test_full_self_heal" % (mountpoint))
        ret, _, _ = g.run(client, cmd)
        self.assertFalse(ret, "Failed to create directory on mountpoint")
        g.log.info("Successfully created directory on mountpoint")

        # Test full data heal
        self.check_full_data_heal(0)
        self.check_full_data_heal(1)
