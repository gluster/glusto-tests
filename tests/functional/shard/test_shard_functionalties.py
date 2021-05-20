#  Copyright (C) 2017-2020 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.glusterfile import (calculate_hash, get_fattr)
from glustolibs.gluster.brick_libs import (
    bring_bricks_offline, get_all_bricks)
from glustolibs.gluster.brickdir import BrickDir
from glustolibs.gluster.volume_libs import (
    get_subvols, set_volume_options, get_volume_options)


@runs_on([['distributed', 'distributed-replicated', 'distributed-arbiter'],
          ['glusterfs']])
class TestShardFunctionalities(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUp
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        ret = cls.setup_volume_and_mount_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Enable sharding
        options = {'features.shard': 'on'}
        ret = set_volume_options(cls.mnode, cls.volname, options)

    @classmethod
    def tearDownClass(cls):

        # Unmount and cleanup original volume
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        cls.get_super_method(cls, 'tearDownClass')()

    def test_shard_volume_options(self):
        '''
        test case: (verify shard volume options)
        - Verify shard default shard volume options
        - Set shard volume options to other values and verify
        - Reset the shard volume options to default values and verify
        '''
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        vol_option_dict = get_volume_options(self.mnode, self.volname)

        # verify sharding xlator is enabled
        self.assertEqual(vol_option_dict['features.shard'], 'on', "Failed"
                         " to validate "
                         "volume option - features.shard: on")

        # verify default shard block size set to 64MB
        self.assertEqual(
            vol_option_dict['features.shard-block-size'],
            '64MB',
            "Failed"
            " to validate "
            "volume option - features.shard-block-size: 64Mb")

        # verify default shard lru limit set to 16384
        self.assertEqual(
            vol_option_dict['features.shard-lru-limit'],
            '16384',
            "Failed"
            " to validate "
            "volume option - features.shard-lru-limit: 16384")

        # verify default shard deletion rate set to 100
        self.assertEqual(
            vol_option_dict['features.shard-deletion-rate'],
            '100',
            "Failed"
            " to validate "
            "volume option - features.shard-deletion-rate: 100")

        # set shard-block-size to 4MB
        options = {'features.shard-block-size': '4MB',
                   'features.shard-lru-limit': '25',
                   'features.shard-deletion-rate': '200'}
        ret = set_volume_options(self.mnode, self.volname, options)

        vol_option_dict = get_volume_options(self.mnode, self.volname)

        # verify shard block size set to 4MB
        self.assertEqual(
            vol_option_dict['features.shard-block-size'],
            '4MB',
            "Failed"
            " to validate "
            "volume option - features.shard-block-size: 4MB")
        # verify shard lru limit set to 25
        self.assertEqual(
            vol_option_dict['features.shard-lru-limit'],
            '25',
            "Failed"
            " to validate "
            "volume option - features.shard-lru-limit: 25")

        # verify shard deletion rate set to 200
        self.assertEqual(
            vol_option_dict['features.shard-deletion-rate'],
            '200',
            "Failed"
            " to validate "
            "volume option - features.shard-deletion-rate: 200")

        # Reset the shard volume options to default
        options = {'features.shard-block-size': '64MB',
                   'features.shard-lru-limit': '16384',
                   'features.shard-deletion-rate': '100'}
        ret = set_volume_options(self.mnode, self.volname, options)

        vol_option_dict = get_volume_options(self.mnode, self.volname)

        # verify shard block size set to 4MB
        self.assertEqual(
            vol_option_dict['features.shard-block-size'],
            '64MB',
            "Failed"
            " to validate "
            "volume option - features.shard-block-size: 64MB")
        # verify shard lru limit set to 25
        self.assertEqual(
            vol_option_dict['features.shard-lru-limit'],
            '16384',
            "Failed"
            " to validate "
            "volume option - features.shard-lru-limit: 16384")

        # verify shard deletion rate set to 200
        self.assertEqual(
            vol_option_dict['features.shard-deletion-rate'],
            '100',
            "Failed"
            " to validate "
            "volume option - features.shard-deletion-rate: 100")

        g.log.info("Successfully validated volume options"
                   "for volume %s", self.volname)
