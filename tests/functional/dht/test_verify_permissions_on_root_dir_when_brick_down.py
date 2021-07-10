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

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfile import set_file_permissions
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline,
                                           bring_bricks_online)


@runs_on([['distributed', 'distributed-replicated', 'distributed-dispersed',
           'distributed-arbiter'],
          ['glusterfs']])
class TestVerifyPermissionChanges(GlusterBaseClass):
    def setUp(self):
        """
        Setup and mount volume
        """
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume(mounts=[self.mounts[0]]):
            raise ExecutionError("Failed to Setup and Mount Volume")

    def _set_root_dir_permission(self, permission):
        """ Sets the root dir permission to the given value"""
        m_point = self.mounts[0].mountpoint
        ret = set_file_permissions(self.mounts[0].client_system,
                                   m_point, permission)
        self.assertTrue(ret, "Failed to set root dir permissions")

    def _get_dir_permissions(self, host, directory):
        """ Returns dir permissions"""
        cmd = 'stat -c "%a" {}'.format(directory)
        ret, out, _ = g.run(host, cmd)
        self.assertEqual(ret, 0, "Failed to get permission on {}".format(host))
        return out.strip()

    def _get_root_dir_permission(self, expected=None):
        """ Returns the root dir permission """
        permission = self._get_dir_permissions(self.mounts[0].client_system,
                                               self.mounts[0].mountpoint)
        if not expected:
            return permission.strip()
        self.assertEqual(permission, expected, "The permissions doesn't match")
        return True

    def _bring_a_brick_offline(self):
        """ Brings down a brick from the volume"""
        brick_to_kill = get_all_bricks(self.mnode, self.volname)[-1]
        ret = bring_bricks_offline(self.volname, brick_to_kill)
        self.assertTrue(ret, "Failed to bring brick offline")
        return brick_to_kill

    def _bring_back_brick_online(self, brick):
        """ Brings back down brick from the volume"""
        ret = bring_bricks_online(self.mnode, self.volname, [brick],
                                  "glusterd_restart")
        self.assertTrue(ret, "Failed to bring brick online")

    def _verify_mount_dir_and_brick_dir_permissions(self, expected,
                                                    down_brick=None):
        """ Verifies the mount directory and brick dir permissions are same"""
        # Get root dir permission and verify
        self._get_root_dir_permission(expected)

        # Verify brick dir permission
        brick_list = get_all_bricks(self.mnode, self.volname)
        for brick in brick_list:
            brick_node, brick_path = brick.split(":")
            if down_brick and down_brick.split(":")[-1] != brick_path:
                actual_perm = self._get_dir_permissions(brick_node,
                                                        brick_path)
                self.assertEqual(actual_perm, expected,
                                 "The permissions are not same")

    def test_verify_root_dir_permission_changes(self):
        """
        1. create pure dist volume
        2. mount on client
        3. Checked default permission (should be 755)
        4. Change the permission to 444 and verify
        5. Kill a brick
        6. Change root permission to 755
        7. Verify permission changes on all bricks, except down brick
        8. Bring back the brick and verify the changes are reflected
        """

        # Verify the default permission on root dir is 755
        self._verify_mount_dir_and_brick_dir_permissions("755")

        # Change root permission to 444
        self._set_root_dir_permission("444")

        # Verify the changes were successful
        self._verify_mount_dir_and_brick_dir_permissions("444")

        # Kill a brick
        offline_brick = self._bring_a_brick_offline()

        # Change root permission to 755
        self._set_root_dir_permission("755")

        # Verify the permission changed to 755 on mount and brick dirs
        self._verify_mount_dir_and_brick_dir_permissions("755", offline_brick)

        # Bring brick online
        self._bring_back_brick_online(offline_brick)

        # Verify the permission changed to 755 on mount and brick dirs
        self._verify_mount_dir_and_brick_dir_permissions("755")

    def tearDown(self):
        # Change root permission back to 755
        self._set_root_dir_permission("755")

        # Unmount and cleanup original volume
        if not self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]]):
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
