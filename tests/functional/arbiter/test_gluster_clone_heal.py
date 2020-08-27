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
from glustolibs.gluster.brick_libs import (
    select_volume_bricks_to_bring_offline,
    bring_bricks_offline,
    bring_bricks_online,
    are_bricks_offline)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_volume_in_split_brain)
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs)
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['arbiter', 'distributed-arbiter',
           'replicated', 'distributed-replicated'], ['glusterfs']])
class TestGlusterCloneHeal(GlusterBaseClass):
    """
    Description:
        Arbiter Test cases related to self heal
        of data and hardlink
    """
    def setUp(self):
        # Calling GlusterBaseClass
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_gluster_clone_heal(self):
        """
        Test gluster compilation on mount point(Heal command)
        - Creating directory test_compilation
        - Compile gluster on mountpoint
        - Select bricks to bring offline
        - Bring brick offline
        - Validate IO
        - Bring bricks online
        - Wait for volume processes to be online
        - Verify volume's all process are online
        - Monitor heal completion
        - Check for split-brain
        - Get arequal after getting bricks online
        - Compile gluster on mountpoint again
        - Select bricks to bring offline
        - Bring brick offline
        - Validate IO
        - Bring bricks online
        - Wait for volume processes to be online
        - Verify volume's all process are online
        - Monitor heal completion
        - Check for split-brain
        - Get arequal after getting bricks online
        """
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        # Creating directory test_compilation
        ret = mkdir(self.mounts[0].client_system, "{}/test_compilation"
                    .format(self.mounts[0].mountpoint))
        self.assertTrue(ret, "Failed to create directory")
        g.log.info("Directory 'test_compilation' on %s created "
                   "successfully", self.mounts[0])

        # Compile gluster on mountpoint
        cmd = ("cd %s/test_compilation ; rm -rf glusterfs; git clone"
               " git://github.com/gluster/glusterfs.git ; cd glusterfs ;"
               " ./autogen.sh ;./configure CFLAGS='-g3 -O0 -DDEBUG'; make ;"
               " cd ../..;" % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, cmd)

        # Select bricks to bring offline
        bricks_to_bring_offline = select_volume_bricks_to_bring_offline(
            self.mnode, self.volname)
        self.assertIsNotNone(bricks_to_bring_offline, "List is empty")

        # Bring brick offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks {} offline'.
                        format(bricks_to_bring_offline))

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks {} are not offline'.
                        format(bricks_to_bring_offline))
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Validate IO
        self.assertTrue(
            validate_io_procs([proc], self.mounts[0]),
            "IO failed on some of the clients"
        )

        # Bring bricks online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks {} online'.format
                        (bricks_to_bring_offline))

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume {} processes to "
                              "be online".format(self.volname)))

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume {} : All process are not online".format
                              (self.volname)))

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')

        # Get arequal after getting bricks online
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info("Arequal of mountpoint %s", result_after_online)

        # Compile gluster on mountpoint again
        proc1 = g.run_async(self.mounts[0].client_system, cmd)

        # Select bricks to bring offline
        bricks_to_bring_offline = select_volume_bricks_to_bring_offline(
            self.mnode, self.volname)
        self.assertIsNotNone(bricks_to_bring_offline, "List is empty")

        # Bring brick offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks {} offline'.
                        format(bricks_to_bring_offline))

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks {} are not offline'.
                        format(bricks_to_bring_offline))

        # Validate IO
        self.assertTrue(
            validate_io_procs([proc1], self.mounts[0]),
            "IO failed on some of the clients"
        )

        # Bring bricks online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks {} online'.format
                        (bricks_to_bring_offline))

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume {} processes to "
                              "be online".format(self.volname)))

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume {} : All process are not online".format
                              (self.volname)))

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')

        # Get arequal after getting bricks online
        ret, result_after_online = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info("Arequal of mountpoint %s", result_after_online)
