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
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-131 USA.

"""
Description:
    Rebalance: permissions check as non root user
"""

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    rebalance_start,
    wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    log_volume_info_and_status)
from glustolibs.io.utils import (collect_mounts_arequal)
from glustolibs.gluster.lib_utils import (add_user, del_user)
from glustolibs.gluster.glusterfile import (
    get_file_stat,
    set_file_permissions)


@runs_on([['distributed', 'distributed-replicated'],
          ['glusterfs']])
class TestRebalancePreserveUserPermissions(GlusterBaseClass):
    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        self.user = "glusto_user"
        self.client = self.mounts[0].client_system
        self.mountpoint = self.mounts[0].mountpoint
        # Add new user on the client node
        ret = add_user(self.client, self.user)
        if not ret:
            raise ExecutionError("Failed to add user")

    def tearDown(self):
        ret = del_user(self.client, self.user)
        if not ret:
            raise ExecutionError("Failed to delete user")
        # Unmount Volume and cleanup.
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and "
                                 "Cleanup Volume")
        g.log.info("Successful in Unmount Volume and cleanup.")

        self.get_super_method(self, 'tearDown')()

    def _start_rebalance_and_wait(self):
        """Start rebalance and wait"""
        # Start Rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

    def _get_arequal_and_check_if_equal_to_before(self):
        """Check if arequal checksum is equal or not"""
        self.arequal_checksum_after = collect_mounts_arequal(self.mounts[0])
        self.assertEqual(
            self.arequal_checksum_before, self.arequal_checksum_after,
            "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")

    def _logged_vol_info(self):
        """Log volume info and status"""
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))

    def _check_user_permission(self):
        """
        Verify permissions on MP and file
        """
        stat_mp_dict = get_file_stat(self.client, self.mountpoint)
        self.assertIsNotNone(stat_mp_dict, "stat on %s failed"
                             % self.mountpoint)
        self.assertEqual(stat_mp_dict['access'], '777',
                         "Expected 777 "
                         "but found %s" % stat_mp_dict['access'])
        g.log.info("File permissions for mountpoint is 777 as expected")

        # check owner and group of random file
        fpath = self.mountpoint + "/d1/f.1"
        stat_dict = get_file_stat(self.client, fpath)
        self.assertIsNotNone(stat_dict, "stat on %s failed" % fpath)
        self.assertEqual(stat_dict['username'], self.user,
                         "Expected %s but found %s"
                         % (self.user, stat_dict['username']))
        self.assertEqual(stat_dict['groupname'], self.user,
                         "Expected %s but found %s"
                         % (self.user, stat_dict['groupname']))
        g.log.info("User and Group are %s as expected", self.user)

    def _testcase(self, number_of_expands=1):
        """
        Test case:
        1. Create a volume start it and mount on the client.
        2. Set full permission on the mount point.
        3. Add new user to the client.
        4. As the new user create dirs/files.
        5. Compute arequal checksum and check permission on / and subdir.
        6. expand cluster according to number_of_expands and start rebalance.
        7. After rebalance is completed:
        7.1 check arequal checksum
        7.2 verfiy no change in / and sub dir permissions.
        7.3 As the new user create and delete file/dir.
        """
        # Set full permissions on the mount point.
        ret = set_file_permissions(self.clients[0], self.mountpoint, "-R 777")
        self.assertTrue(ret, "Failed to set permissions on the mount point")
        g.log.info("Set full permissions on the mount point")

        # Create dirs/files as self.test_user
        cmd = (r'su -l %s -c "cd %s;'
               r'for i in {0..9}; do mkdir d\$i; done;'
               r'for i in {0..99}; do let x=\$i%%10;'
               r'dd if=/dev/urandom of=d\$x/f.\$i bs=1024 count=1; done"'
               % (self.user, self.mountpoint))
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, ("Failed to create files as %s", self.user))
        g.log.info("IO as %s is successful", self.user)

        # check permission on / and subdir
        self._check_user_permission()

        # get arequal checksum before expand
        self.arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        self._logged_vol_info()

        # expand the volume
        for i in range(number_of_expands):
            ret = expand_volume(self.mnode, self.volname, self.servers,
                                self.all_servers_info)
            self.assertTrue(ret, ("Failed to expand iter %d volume %s",
                                  i, self.volname))

        self._logged_vol_info()
        # Start Rebalance and wait for completion
        self._start_rebalance_and_wait()

        # compare arequals checksum before and after rebalance
        self._get_arequal_and_check_if_equal_to_before()

        # permissions check on / and sub dir
        self._check_user_permission()

        # Create/Delete file as self.test_user
        cmd = ('su -l %s -c '
               '"cd %s; touch file.test;'
               'find . -mindepth 1 -maxdepth 1 -type d | xargs rm -rf"'
               % (self.user, self.mountpoint))
        ret, _, _ = g.run(self.client, cmd)

        self.assertEqual(ret, 0, ("User %s failed to create files", self.user))
        g.log.info("IO as %s is successful", self.user)

    def test_rebalance_preserve_user_permissions(self):
        self._testcase()

    def test_rebalance_preserve_user_permissions_multi_expands(self):
        self._testcase(2)
