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

from random import sample

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.lib_utils import is_core_file_created
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           are_bricks_offline,
                                           are_bricks_online)
from glustolibs.gluster.heal_libs import monitor_heal_completion


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class TestEcTruncateFileWithBrickDown(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup volume and mount it on three clients.
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    def tearDown(self):
        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

        # Unmount and cleanup the volume
        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Unable to unmount and cleanup volume")

    def test_ec_truncate_file_with_brick_down(self):
        """
        Test steps:
        1. Create a volume, start and mount it on a client
        2. Bring down redundant bricks in the subvol
        3. Create a file on the volume using "touch"
        4. Truncate the file using "O_TRUNC"
        5. Bring the brick online
        6. Write data on the file and wait for heal completion
        7. Check for crashes and coredumps
        """
        # Time stamp from mnode for checking cores at the end of test
        ret, test_timestamp, _ = g.run(self.mnode, "date +%s")
        self.assertEqual(ret, 0, "date command failed")
        test_timestamp = test_timestamp.strip()

        # Create a file using touch
        file_name = self.mounts[0].mountpoint + "/test_1"
        ret, _, err = g.run(self.mounts[0].client_system, "touch {}".
                            format(file_name))
        self.assertEqual(ret, 0, "File creation failed")
        g.log.info("File Created successfully")

        # List two bricks in each subvol
        all_subvols_dict = get_subvols(self.mnode, self.volname)
        subvols = all_subvols_dict['volume_subvols']
        bricks_to_bring_offline = []
        for subvol in subvols:
            self.assertTrue(subvol, "List is empty")
            bricks_to_bring_offline.extend(sample(subvol, 2))

        # Bring two bricks of each subvol offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, "Bricks are still online")

        # Validating the bricks are offline or not
        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, "Few of the bricks are still online in"
                             " {} in".format(bricks_to_bring_offline))

        # Truncate the file
        cmd = ('''python -c "import os, sys; fd = os.open('{}', os.O_TRUNC )'''
               '''; os.close( fd )"'''.format(file_name))
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, err)
        g.log.info("File truncated successfully")

        # Bring back the bricks online
        ret, _, err = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, err)
        g.log.info("All bricks are online")

        # Check whether bricks are online or not
        ret = are_bricks_online(self.mnode, self.volname,
                                bricks_to_bring_offline)
        self.assertTrue(ret, "Bricks {} are still offline".
                        format(bricks_to_bring_offline))

        # write data to the file
        cmd = ('''python -c "import os, sys;fd = os.open('{}', os.O_RDWR) ;'''
               '''os.write(fd, 'This is test after truncate'); os.close(fd)"'''
               .format(file_name))
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, err)
        g.log.info("Data written successfully on to the file")

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "Heal pending for file {}".format(file_name))

        # check for any crashes
        ret = is_core_file_created(self.servers, test_timestamp)
        self.assertTrue(ret, "Cores found on the servers")
