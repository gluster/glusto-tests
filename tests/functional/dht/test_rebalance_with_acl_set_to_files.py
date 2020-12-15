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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.glusterfile import set_acl, get_acl
from glustolibs.gluster.lib_utils import add_user, del_user
from glustolibs.gluster.mount_ops import mount_volume
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed-replicated', 'distributed-arbiter', 'distributed',
           'replicated', 'arbiter', 'distributed-dispersed',
           'dispersed'], ['glusterfs']])
class TestRebalanceWithAclSetToFiles(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume():
            raise ExecutionError("Failed to Setup volume")

        self.first_client = self.mounts[0].client_system
        self.mount_point = self.mounts[0].mountpoint

        # Mount volume with -o acl option
        ret, _, _ = mount_volume(self.volname, self.mount_type,
                                 self.mount_point, self.mnode,
                                 self.first_client, options='acl')
        if ret:
            raise ExecutionError("Failed to mount volume")

        # Create a non-root user
        if not add_user(self.first_client, 'joker'):
            raise ExecutionError("Failed to create user joker")

    def tearDown(self):

        # Remove non-root user created for test
        if not del_user(self.first_client, 'joker'):
            raise ExecutionError("Failed to remove user joker")

        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _check_acl_set_to_files(self):
        """Check acl values set to files"""
        for number in range(1, 11):
            ret = get_acl(self.first_client, self.mount_point,
                          'file{}'.format(str(number)))
            self.assertIn('user:joker:rwx', ret['rules'],
                          "Rule not present in getfacl output")

    def test_add_brick_rebalance_with_acl_set_to_files(self):
        """
        Test case:
        1. Create a volume, start it and mount it to a client.
        2. Create 10 files on the mount point and set acls on the files.
        3. Check the acl value and collect arequal-checksum.
        4. Add bricks to the volume and start rebalance.
        5. Check the value of acl(it should be same as step 3),
           collect and compare arequal-checksum with the one collected
           in step 3
        """
        # Create 10 files on the mount point.
        cmd = ("cd {}; for i in `seq 1 10`;do touch file$i;done"
               .format(self.mount_point))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create files on mount point")

        for number in range(1, 11):
            ret = set_acl(self.first_client, 'u:joker:rwx', '{}/file{}'
                          .format(self.mount_point, str(number)))
            self.assertTrue(ret, "Failed to set acl on files")

        # Collect arequal on mount point and check acl value
        arequal_checksum_before = collect_mounts_arequal(self.mounts[0])
        self._check_acl_set_to_files()
        g.log.info("Files created and acl set to files properly")

        # Add brick to volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)

        # Trigger rebalance and wait for it to complete
        ret, _, _ = rebalance_start(self.mnode, self.volname,
                                    force=True)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1200)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

        # Check acl value if it's same as before rebalance
        self._check_acl_set_to_files()

        # Check for data loss by comparing arequal before and after ops
        arequal_checksum_after = collect_mounts_arequal(self.mounts[0])
        self.assertEqual(arequal_checksum_before, arequal_checksum_after,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum and acl value are SAME")
