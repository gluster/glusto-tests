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

# pylint: disable=protected-access
# pylint: disable=too-many-statements

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfile import (get_fattr, set_fattr,
                                            delete_fattr)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.dht_test_utils import (find_hashed_subvol,
                                               find_new_hashed)
from glustolibs.gluster.brick_libs import (get_online_bricks_list,
                                           bring_bricks_offline)
from glustolibs.gluster.volume_ops import volume_start


@runs_on([['distributed', 'distributed-dispersed',
           'distributed-arbiter', 'distribited-replicated'],
          ['glusterfs']])
class TestCustomXattrHealingForDir(GlusterBaseClass):
    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        self.client, self.m_point = (self.mounts[0].client_system,
                                     self.mounts[0].mountpoint)

    def tearDown(self):

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _set_xattr_value(self, fattr_value="bar2"):
        """Set the xattr 'user.foo' as per the value on dir1"""
        # Set the xattr on the dir1
        ret = set_fattr(self.client, '{}/dir1'.format(self.m_point),
                        'user.foo', fattr_value)
        self.assertTrue(ret, "Failed to set the xattr on dir1")
        g.log.info("Successfully set the xattr user.foo with value:"
                   " %s on dir1", fattr_value)

    def _check_xattr_value_on_mnt(self, expected_value=None):
        """Check if the expected value for 'user.foo'
        is present for dir1 on mountpoint"""
        ret = get_fattr(self.client, '{}/dir1'.format(self.m_point),
                        'user.foo', encode="text")
        self.assertEqual(ret, expected_value, "Failed to get the xattr"
                         " on:{}".format(self.client))
        g.log.info(
            "The xattr user.foo for dir1 is displayed on mointpoint"
            " and has value:%s", expected_value)

    def _check_xattr_value_on_bricks(self, online_bricks, expected_value=None):
        """Check if the expected value for 'user.foo'is present
        for dir1 on backend bricks"""
        for brick in online_bricks:
            host, brick_path = brick.split(':')
            ret = get_fattr(host, '{}/dir1'.format(brick_path),
                            'user.foo', encode="text")
            self.assertEqual(ret, expected_value, "Failed to get the xattr"
                                                  " on:{}".format(brick_path))
            g.log.info("The xattr user.foo is displayed for dir1 on "
                       "brick:%s and has value:%s",
                       brick_path, expected_value)

    def _create_dir(self, dir_name=None):
        """Create a directory on the mountpoint"""
        ret = mkdir(self.client, "{}/{}".format(self.m_point, dir_name))
        self.assertTrue(ret, "mkdir of {} failed".format(dir_name))

    def _perform_lookup(self):
        """Perform lookup on mountpoint"""
        cmd = ("ls -lR {}/dir1".format(self.m_point))
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, "Failed to lookup")
        g.log.info("Lookup successful")
        sleep(5)

    def _create_xattr_check_self_heal(self):
        """Create custom xattr and check if its healed"""
        # Set the xattr on the dir1
        self._set_xattr_value(fattr_value="bar2")

        # Get online brick list
        online_bricks = get_online_bricks_list(self.mnode, self.volname)
        self.assertIsNotNone(online_bricks, "Failed to get online bricks")

        # Check if the custom xattr is being displayed on the
        # mount-point for dir1
        self._check_xattr_value_on_mnt(expected_value="bar2")

        # Check if the xattr is being displayed on the online-bricks
        # for dir1
        self._check_xattr_value_on_bricks(online_bricks, expected_value="bar2")

        # Modify custom xattr value on dir1
        self._set_xattr_value(fattr_value="ABC")

        # Lookup on moint-point to refresh the value of xattr
        self._perform_lookup()

        # Check if the modified custom xattr is being displayed
        # on the mount-point for dir1
        self._check_xattr_value_on_mnt(expected_value="ABC")

        # Check if the modified custom xattr is being
        # displayed on the bricks for dir1
        self._check_xattr_value_on_bricks(online_bricks, expected_value="ABC")

        # Remove the custom xattr from the mount point for dir1
        ret = delete_fattr(self.client,
                           '{}/dir1'.format(self.m_point), 'user.foo')
        self.assertTrue(ret, "Failed to delete the xattr for "
                             "dir1 on mountpoint")
        g.log.info(
            "Successfully deleted the xattr for dir1 from mountpoint")

        # Lookup on moint-point to refresh the value of xattr
        self._perform_lookup()

        # Check that the custom xattr is not displayed on the
        # for dir1 on mountpoint
        ret = get_fattr(self.client, '{}/dir1'.format(self.m_point),
                        'user.foo', encode="text")
        self.assertEqual(ret, None, "Xattr for dir1 is not removed"
                         " on:{}".format(self.client))
        g.log.info("Success: xattr is removed for dir1 on mointpoint")

        # Check that the custom xattr is not displayed on the
        # for dir1 on the backend bricks
        for brick in online_bricks:
            host, brick_path = brick.split(':')
            ret = get_fattr(host, '{}/dir1'.format(brick_path),
                            'user.foo', encode="text")
            self.assertEqual(ret, None, "Xattr for dir1 is not removed"
                                        " on:{}".format(brick_path))
            g.log.info("Xattr for dir1 is removed from "
                       "brick:%s", brick_path)

        # Check if the trusted.glusterfs.pathinfo is displayed
        # for dir1 on mointpoint
        ret = get_fattr(self.client, '{}/dir1'.format(self.m_point),
                        'trusted.glusterfs.pathinfo')
        self.assertIsNotNone(ret, "Failed to get the xattr"
                             " on:{}".format(self.client))
        g.log.info("The xattr trusted.glusterfs.pathinfo"
                   " is displayed on mointpoint for dir1")

        # Set the xattr on the dir1
        self._set_xattr_value(fattr_value="star1")

        # Bring back the bricks online
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertFalse(ret, 'Failed to start volume %s with "force" option'
                         % self.volname)
        g.log.info('Successfully started volume %s with "force" option',
                   self.volname)

        # Execute lookup on the mointpoint
        self._perform_lookup()

        # Get online brick list
        online_bricks = get_online_bricks_list(self.mnode, self.volname)
        self.assertIsNotNone(online_bricks, "Failed to get online bricks")

        # Check if the custom xattr is being displayed
        # on the mount-point for dir1
        self._check_xattr_value_on_mnt(expected_value="star1")

        # Check if the custom xattr is displayed on all the bricks
        self._check_xattr_value_on_bricks(online_bricks,
                                          expected_value="star1")

    def test_custom_xattr_with_subvol_down_dir_exists(self):
        """
        Description:
        Steps:
        1) Create directories from mount point.
        2) Bring one or more(not all) dht sub-volume(s) down by killing
           processes on that server
        3) Create a custom xattr for dir hashed to down sub-volume and also for
           another dir not hashing to down sub-volumes
           # setfattr -n user.foo -v bar2 <dir>
        4) Verify that custom xattr for directory is displayed on mount point
           and bricks for both directories
           # getfattr -n user.foo <dir>
           # getfattr -n user.foo <brick_path>/<dir>
        5) Modify custom xattr value and verify that custom xattr for directory
           is displayed on mount point and all up bricks
           # setfattr -n user.foo -v ABC <dir>
        6) Verify that custom xattr is not displayed once you remove it on
           mount point and all up bricks
        7) Verify that mount point shows pathinfo xattr for dir hashed to down
           sub-volume and also for dir not hashed to down sub-volumes
           # getfattr -n trusted.glusterfs.pathinfo <dir>
        8) Again create a custom xattr for dir not hashing to down sub-volumes
           # setfattr -n user.foo -v star1 <dir>
        9) Bring up the sub-volumes
        10) Execute lookup on parent directory of both <dir> from mount point
        11) Verify Custom extended attributes for dir1 on all bricks
        """
        # pylint: disable=protected-access
        # Create dir1 on client0
        self._create_dir(dir_name="dir1")

        # Get subvol list
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(subvols, "Failed to get subvols")

        # Finding a dir name such that it hashes to a different subvol
        newhash = find_new_hashed(subvols, "/", "dir1")
        new_name = str(newhash.newname)
        new_subvol_count = newhash.subvol_count

        # Create a dir with the new name
        self._create_dir(dir_name=new_name)

        # Kill the brick/subvol to which the new dir hashes
        ret = bring_bricks_offline(
            self.volname, subvols[new_subvol_count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[new_subvol_count]))
        g.log.info('DHT subvol %s is offline', subvols[new_subvol_count])

        # Set the xattr on dir hashing to down subvol
        ret = set_fattr(self.client, '{}/{}'.format(self.m_point, new_name),
                        'user.foo', 'bar2')
        self.assertFalse(ret, "Unexpected: custom xattr set successfully"
                              " for dir hashing to down subvol")
        g.log.info("Expected: Failed to set xattr on dir:%s"
                   " which hashes to down subvol due to error: Transport"
                   " endpoint not connected", new_name)

        # Check if the trusted.glusterfs.pathinfo is displayed
        # for dir hashing to down subvol on mointpoint
        ret = get_fattr(self.client, '{}/{}'.format(
            self.m_point, new_name), 'trusted.glusterfs.pathinfo')
        self.assertIsNotNone(ret, "Failed to get the xattr"
                             " on:{}".format(self.client))
        g.log.info("The xattr trusted.glusterfs.pathinfo"
                   " is displayed on mointpoint for %s", new_name)

        # Set the xattr on dir hashing to down subvol
        ret = set_fattr(self.client, '{}/{}'.format(self.m_point, new_name),
                        'user.foo', 'star1')
        self.assertFalse(ret, "Unexpected: custom xattr set successfully"
                              " for dir hashing to down subvol")
        g.log.info("Expected: Tansport endpoint not connected")

        # Calling the local function
        self._create_xattr_check_self_heal()

    def test_custom_xattr_with_subvol_down_dir_doesnt_exists(self):
        """
        Description:
        Steps:
        1) Bring one or more(not all) dht sub-volume(s) down by killing
           processes on that server
        2) Create a directory from mount point such that it
           hashes to up subvol.
        3) Create a custom xattr for dir
           # setfattr -n user.foo -v bar2 <dir>
        4) Verify that custom xattr for directory is displayed on mount point
           and bricks for directory
           # getfattr -n user.foo <dir>
           # getfattr -n user.foo <brick_path>/<dir>
        5) Modify custom xattr value and verify that custom xattr for directory
           is displayed on mount point and all up bricks
           # setfattr -n user.foo -v ABC <dir>
        6) Verify that custom xattr is not displayed once you remove it on
           mount point and all up bricks
        7) Verify that mount point shows pathinfo xattr for dir
        8) Again create a custom xattr for dir
           # setfattr -n user.foo -v star1 <dir>
        9) Bring up the sub-volumes
        10) Execute lookup on parent directory of both <dir> from mount point
        11) Verify Custom extended attributes for dir1 on all bricks
        """
        # Get subvol list
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(subvols, "Failed to get subvols")

        # Find out the hashed subvol for dir1
        hashed_subvol, subvol_count = find_hashed_subvol(subvols, "/", "dir1")
        self.assertIsNotNone(hashed_subvol, "Could not find srchashed")
        g.log.info("Hashed subvol for dir1 is %s", hashed_subvol._path)

        # Remove the hashed_subvol from subvol list
        subvols.remove(subvols[subvol_count])

        # Bring down a dht subvol
        ret = bring_bricks_offline(self.volname, subvols[0])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[0]))
        g.log.info('DHT subvol %s is offline', subvols[0])

        # Create the dir1
        self._create_dir(dir_name="dir1")

        # Calling the local function
        self._create_xattr_check_self_heal()
