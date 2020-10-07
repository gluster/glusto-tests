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
    Verify Eager lock reduces the number of locks
    being taken when writing to the file continuosly
"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.profile_ops import (profile_start, profile_stop)
from glustolibs.gluster.dht_test_utils import find_hashed_subvol
from glustolibs.gluster.lib_utils import get_extended_attributes_info


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs']])
class EagerlockFunctionalValidationTest(GlusterBaseClass):
    # Method to setup the environment for test case

    def setUp(self):
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=True)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume")

    def _check_dirty_xattr(self, filename):
        """Get trusted.ec.dirty xattr value to validate eagerlock behavior"""
        # Find the hashed subvol of the file created
        # for distributed disperse case
        subvols_info = get_subvols(self.mnode, self.volname)
        subvols_info = subvols_info['volume_subvols']
        if len(subvols_info) > 1:
            _, hashed_subvol = find_hashed_subvol(subvols_info,
                                                  '', filename)
            if hashed_subvol is None:
                g.log.error("Error in finding hash value of %s", filename)
                return None
        else:
            hashed_subvol = 0

        # Collect ec.dirty xattr value from each brick
        result = []
        for subvol in subvols_info[hashed_subvol]:
            host, brickpath = subvol.split(':')
            brickpath = brickpath + '/' + filename
            ret = get_extended_attributes_info(host, [brickpath],
                                               encoding='hex',
                                               attr_name='trusted.ec.dirty')
            ret = ret[brickpath]['trusted.ec.dirty']
            result.append(ret)

        # Check if xattr values are same across all bricks
        if result.count(result[0]) == len(result):
            return ret
        g.log.error("trusted.ec.dirty value is not consistent across the "
                    "disperse set %s", result)
        return None

    def _file_create_and_profile_info(self, status):
        """Create a file and check the volume profile for inode lock count."""
        # Creating file
        mountpoint = self.mounts[0].mountpoint
        client = self.mounts[0].client_system

        filename = 'f1_EagerLock_' + status
        cmd = ("dd if=/dev/urandom of=%s/%s bs=100M count=10"
               % (mountpoint, filename))

        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Failed to create file on mountpoint")
        g.log.info("Successfully created files on mountpoint")

        # Getting and checking output of profile info.
        cmd = "gluster volume profile %s info | grep -i INODELK" % self.volname
        ret, rout, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to grep INODELK count from profile "
                         "info")
        g.log.info("The lock counts on all bricks with eager-lock %s: %s",
                   status, rout)

        return filename

    def test_validate_profile_for_inodelk(self):
        """
        Test Steps:
        1) Create an ecvolume and mount it
        2) Set the eagerlock option
        3) Create a 1GB file
        4) View the profile of the volume for INODELK count must be about
           2-10 locks for each brick.
        5) check backend bricks for trusted.ec.dirty xattr must be non-zero
        6) Disable the eagerlock option
        7) Repeat steps 3-5 and now dirty xattr must be zero and
           INODELK count in range of 100-5k.
        """

        # Enable EagerLock
        ret = set_volume_options(self.mnode, self.volname,
                                 {'disperse.eager-lock': 'on',
                                  'disperse.eager-lock-timeout': '10'})
        self.assertTrue(ret, "Failed to turn on eagerlock"
                             "on %s" % self.volname)

        # Start profile on volume.
        ret, _, _ = profile_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start profile on volume: %s"
                         % self.volname)

        # Test behavior with EagerLock on
        filename = self._file_create_and_profile_info("on")
        self.assertIsNotNone(filename, "Failed to get filename")

        # Test dirty bit with EagerLock on
        ret = self._check_dirty_xattr(filename)
        self.assertEqual(ret, '0x00000000000000010000000000000001',
                         "Unexpected dirty xattr value is %s on %s"
                         % (ret, filename))

        # Disable EagerLock
        ret = set_volume_options(self.mnode, self.volname,
                                 {'disperse.eager-lock': 'off'})
        self.assertTrue(ret, "Failed to turn off eagerlock "
                             "on %s" % self.volname)

        # Test behavior with EagerLock off
        filename = self._file_create_and_profile_info("off")
        self.assertIsNotNone(filename, "Failed to get filename")

        # Test dirty bit with EagerLock off
        ret = self._check_dirty_xattr(filename)
        self.assertEqual(ret, '0x00000000000000000000000000000000',
                         "Unexpected dirty xattr value is %s on %s"
                         % (ret, filename))

        # Stop profile on volume.
        ret, _, _ = profile_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to stop profile on volume: %s"
                         % self.volname)

    def tearDown(self):
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
