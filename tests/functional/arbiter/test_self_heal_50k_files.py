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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.brick_libs import (select_bricks_to_bring_offline,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_volume_in_split_brain)
from glustolibs.io.utils import validate_io_procs


@runs_on([['arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestSelfHeal50kFiles(GlusterBaseClass):
    """
    Description:
        Arbiter self heal of 50k files
    """
    def setUp(self):
        # Calling GlusterBaseClass
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_self_heal_50k_files(self):
        """
        Description:
        - Select bricks to bring offline
        - Bring brick offline
        - Create 50k files
        - Validate IO
        - Bring bricks online
        - Monitor heal
        - Check for split-brain
        - Validate IO
        """
        # pylint: disable=too-many-statements,too-many-locals
        # Select bricks to bring offline
        bricks_to_bring_offline_dict = select_bricks_to_bring_offline(
            self.mnode, self.volname)
        bricks_to_bring_offline = bricks_to_bring_offline_dict['volume_bricks']

        # Bring brick offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_to_bring_offline)
        self.assertIsNotNone(bricks_to_bring_offline, "List is empty")

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Create 50k files
        command = ("cd %s ; "
                   "for i in `seq 1 50000` ; "
                   "do dd if=/dev/urandom of=test.$i "
                   "bs=100k count=1 ;  "
                   "done ;"
                   % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)

        # Validate IO
        self.assertTrue(
            validate_io_procs([proc], self.mounts[0]),
            "IO failed on some of the clients"
        )

        # Bring brick online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online"
                              % self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=3000)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')
