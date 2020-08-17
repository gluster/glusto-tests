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
from glustolibs.gluster.brick_libs import (get_all_bricks, are_bricks_offline,
                                           bring_bricks_offline,
                                           get_online_bricks_list,
                                           are_bricks_online)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.gluster_init import restart_glusterd
from glustolibs.gluster.glusterfile import set_fattr, get_fattr
from glustolibs.gluster.heal_libs import (is_volume_in_split_brain,
                                          monitor_heal_completion)
from glustolibs.gluster.lib_utils import collect_bricks_arequal


@runs_on([['replicated'], ['glusterfs']])
class TestHealForConservativeMergeWithTwoBricksBlame(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup volume and mount it.
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    def tearDown(self):
        # Unmount and cleanup the volume
        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Unable to unmount and cleanup volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _bring_brick_offline_and_check(self, brick):
        """Brings brick offline an checks if it is offline or not"""
        ret = bring_bricks_offline(self.volname, [brick])
        self.assertTrue(ret, "Unable to bring brick: {} offline".format(brick))

        # Validate the brick is offline
        ret = are_bricks_offline(self.mnode, self.volname, [brick])
        self.assertTrue(ret, "Brick:{} is still online".format(brick))

    def _get_fattr_for_the_brick(self, brick):
        """Get xattr of trusted.afr.volname-client-0 for the given brick"""
        host, fqpath = brick.split(":")
        fqpath = fqpath + "/dir1"
        fattr = "trusted.afr.{}-client-0".format(self.volname)
        return get_fattr(host, fqpath, fattr, encode="hex")

    def _check_peers_status(self):
        """Validates peers are connected or not"""
        count = 0
        while count < 4:
            if self.validate_peers_are_connected():
                return
            sleep(5)
            count += 1
        self.fail("Peers are not in connected state")

    def test_heal_for_conservative_merge_with_two_bricks_blame(self):
        """
        1) Create 1x3 volume and fuse mount the volume
        2) On mount created a dir dir1
        3) Pkill glusterfsd on node n1 (b2 on node2 and b3 and node3 up)
        4) touch f{1..10} on the mountpoint
        5) b2 and b3 xattrs would be blaming b1 as files are created while
           b1 is down
        6) Reset the b3 xattrs to NOT blame b1 by using setattr
        7) Now pkill glusterfsd of b2 on node2
        8) Restart glusterd on node1 to bring up b1
        9) Now bricks b1 online , b2 down, b3 online
        10) touch x{1..10} under dir1 itself
        11) Again reset xattr on node3 of b3 so that it doesn't blame b2,
        as done for b1 in step 6
        12) Do restart glusterd on node2 hosting b2 to bring all bricks online
        13) Check for heal info, split-brain and arequal for the bricks
        """
        # pylint: disable=too-many-locals
        # Create dir `dir1/` on mountpont
        path = self.mounts[0].mountpoint + "/dir1"
        ret = mkdir(self.mounts[0].client_system, path, parents=True)
        self.assertTrue(ret, "Directory {} creation failed".format(path))

        all_bricks = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(all_bricks, "Unable to fetch bricks of volume")
        brick1, brick2, brick3 = all_bricks

        # Bring first brick offline
        self._bring_brick_offline_and_check(brick1)

        # touch f{1..10} files on the mountpoint
        cmd = ("cd {mpt}; for i in `seq 1 10`; do touch f$i"
               "; done".format(mpt=path))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Unable to create files on mountpoint")

        # Check b2 and b3 xattrs are blaming b1 and are same
        self.assertEqual(self._get_fattr_for_the_brick(brick2),
                         self._get_fattr_for_the_brick(brick3),
                         "Both the bricks xattrs are not blaming "
                         "brick: {}".format(brick1))

        # Reset the xattrs of dir1 on b3 for brick b1
        first_xattr_to_reset = "trusted.afr.{}-client-0".format(self.volname)
        xattr_value = "0x000000000000000000000000"
        host, brick_path = brick3.split(":")
        brick_path = brick_path + "/dir1"
        ret = set_fattr(host, brick_path, first_xattr_to_reset, xattr_value)
        self.assertTrue(ret, "Unable to set xattr for the directory")

        # Kill brick2 on the node2
        self._bring_brick_offline_and_check(brick2)

        # Restart glusterd on node1 to bring the brick1 online
        self.assertTrue(restart_glusterd([brick1.split(":")[0]]), "Unable to "
                        "restart glusterd")
        # checking for peer status post glusterd restart
        self._check_peers_status()

        # Check if the brick b1 on node1 is online or not
        online_bricks = get_online_bricks_list(self.mnode, self.volname)
        self.assertIsNotNone(online_bricks, "Unable to fetch online bricks")
        self.assertIn(brick1, online_bricks, "Brick:{} is still offline after "
                                             "glusterd restart".format(brick1))

        # Create 10 files under dir1 naming x{1..10}
        cmd = ("cd {mpt}; for i in `seq 1 10`; do touch x$i"
               "; done".format(mpt=path))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Unable to create files on mountpoint")

        # Reset the xattrs from brick3 on to brick2
        second_xattr_to_reset = "trusted.afr.{}-client-1".format(self.volname)
        ret = set_fattr(host, brick_path, second_xattr_to_reset, xattr_value)
        self.assertTrue(ret, "Unable to set xattr for the directory")

        # Bring brick2 online
        self.assertTrue(restart_glusterd([brick2.split(":")[0]]), "Unable to "
                        "restart glusterd")
        self._check_peers_status()

        self.assertTrue(are_bricks_online(self.mnode, self.volname, [brick2]))

        # Check are there any files in split-brain and heal completion
        self.assertFalse(is_volume_in_split_brain(self.mnode, self.volname),
                         "Some files are in split brain for "
                         "volume: {}".format(self.volname))
        self.assertTrue(monitor_heal_completion(self.mnode, self.volname),
                        "Conservative merge of files failed")

        # Check arequal checksum of all the bricks is same
        ret, arequal_from_the_bricks = collect_bricks_arequal(all_bricks)
        self.assertTrue(ret, "Arequal is collected successfully across the"
                        " bricks in the subvol {}".format(all_bricks))
        self.assertEqual(len(set(arequal_from_the_bricks)), 1, "Arequal is "
                         "same on all the bricks in the subvol")
