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
# GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Description:
    Test set and reset of storage reserve limit in glusterd
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (
    set_volume_options,
    reset_volume_option,
    get_volume_options)


@runs_on([['distributed', 'distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'replicated', 'arbiter', 'dispersed'],
          ['glusterfs']])
class TestGlusterDSetResetReserveLimit(GlusterBaseClass):
    """ Testing set and reset of Reserve limit in GlusterD """

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup and mount the volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume and mount it")

    def validate_vol_option(self, option_name, value_expected):
        """ Function for validating volume options """
        # Get the volume option.
        ret = get_volume_options(self.mnode, self.volname, option_name)
        self.assertIsNotNone(ret, "The %s option is not present" % option_name)
        self.assertEqual(ret[option_name], value_expected,
                         ("Volume option for %s is not equal to %s"
                          % (option_name, value_expected)))
        g.log.info("Volume option %s is equal to the expected value %s",
                   option_name, value_expected)

    def test_glusterd_set_reset_reserve_limit(self):
        """
        Test set and reset of reserve limit on glusterd
        1. Create a volume and start it.
        2. Set storage.reserve limit on the created volume and verify it.
        3. Reset storage.reserve limit on the created volume and verify it.
        """
        # Setting storage.reserve to 50
        ret = set_volume_options(self.mnode, self.volname,
                                 {'storage.reserve': '50'})
        self.assertTrue(ret, "Failed to set storage reserve on %s"
                        % self.mnode)

        # Validate storage.reserve option set to 50
        self.validate_vol_option('storage.reserve', '50')

        # Reseting the storage.reserve limit
        ret, _, _ = reset_volume_option(self.mnode, self.volname,
                                        'storage.reserve')
        self.assertEqual(ret, 0, "Failed to reset the storage.reserve limit")

        # Validate that the storage.reserve option is reset
        self.validate_vol_option('storage.reserve', '1 (DEFAULT)')

    def tearDown(self):
        """tear Down Callback"""
        # Unmount volume and cleanup.
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to unmount and cleanup volume")
        g.log.info("Successful in unmount and cleanup of volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
