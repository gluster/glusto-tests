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

"""
    Test Description:
    Tests to validate auth.allow and auth.reject on a volume
"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           volume_reset)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.mount_ops import (mount_volume, umount_volume,
                                          is_mounted)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'arbiter',
           'distributed-arbiter'], ['glusterfs']])
class TestValidateAuthAllowAndAuthReject(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s"
                                 % self.volname)
        g.log.info("Volume created successfully : %s", self.volname)

    def tearDown(self):
        # Cleanup the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup the volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully: %s", self.volname)

        self.get_super_method(self, 'tearDown')()

    def _set_option_and_mount_and_unmount_volumes(self, option="",
                                                  is_allowed=True):
        """
        Setting volume option and then mounting and unmounting the volume
        """
        # Check if an option is passed
        if option:
            # Setting the option passed as an argument
            ret = set_volume_options(self.mnode, self.volname,
                                     {option: self.mounts[0].client_system})
            self.assertTrue(ret, "Failed to set %s option in volume: %s"
                            % (option, self.volname))
            g.log.info("Successfully set %s option in volume: %s", option,
                       self.volname)

        # Mounting a volume
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)

        # Checking if volume was successfully mounted or not
        ret = is_mounted(self.volname, mtype=self.mount_type,
                         mpoint=self.mounts[0].mountpoint,
                         mserver=self.mnode,
                         mclient=self.mounts[0].client_system)
        if is_allowed:
            self.assertTrue(ret, "Failed to mount the volume: %s"
                            % self.volname)
        else:
            self.assertFalse(ret, "Unexpected: Mounting"
                             " the volume %s was successful" % self.volname)

        # Unmount only if the volume is supposed to be mounted
        if is_allowed:
            ret, _, _ = umount_volume(self.mounts[0].client_system,
                                      self.mounts[0].mountpoint,
                                      mtype=self.mount_type)
            self.assertEqual(ret, 0, "Failed to unmount the volume: %s"
                             % self.volname)

    def _reset_the_volume(self):
        """
        Resetting the volume
        """
        ret = volume_reset(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to reset volume: %s" % self.volname)
        g.log.info("Reseting volume %s was successful", self.volname)

    def _check_validate_test(self):
        """
        Checking volume mounting and unmounting with auth.allow
        and auth.reject option set for it
        """
        # Setting auth.allow option and then mounting and unmounting volume
        self._set_option_and_mount_and_unmount_volumes("auth.allow")
        g.log.info("Successfully performed the set, mounting and unmounting"
                   " operation as expected on volume: %s", self.volname)

        # Reseting the volume options
        self._reset_the_volume()

        # Setting auth.reject option and then checking mounting of volume
        self._set_option_and_mount_and_unmount_volumes("auth.reject", False)
        g.log.info("Successfully performed the set and mounting operation"
                   "as expected on volume: %s", self.volname)

        # Reseting the volume options
        self._reset_the_volume()

        # Check mounting and unmounting of volume without setting any options
        self._set_option_and_mount_and_unmount_volumes()
        g.log.info("Successfully mounted and unmounted the volume: %s",
                   self.volname)

    def test_validate_auth_allow_and_auth_reject(self):
        """
        Test Case:
        1. Create and start a volume
        2. Disable brick mutliplex
        2. Set auth.allow option on volume for the client address on which
           volume is to be mounted
        3. Mount the volume on client and then unmmount it.
        4. Reset the volume
        5. Set auth.reject option on volume for the client address on which
           volume is to be mounted
        6. Mounting the volume should fail
        7. Reset the volume and mount it on client.
        8. Repeat the steps 2-7 with brick multiplex enabled
        """
        # Setting cluster.brick-multiplex to disable
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.brick-multiplex': 'disable'})
        self.assertTrue(ret, "Failed to set brick-multiplex to enable.")
        g.log.info("Successfully set brick-multiplex to disable.")

        # Checking auth options with brick multiplex disabled
        self._check_validate_test()

        # Setting cluster.brick-multiplex to enable
        ret = set_volume_options(self.mnode, 'all',
                                 {'cluster.brick-multiplex': 'enable'})
        self.assertTrue(ret, "Failed to set brick-multiplex to enable.")
        g.log.info("Successfully set brick-multiplex to enable.")

        # Checking auth options with brick multiplex enabled
        self._check_validate_test()
