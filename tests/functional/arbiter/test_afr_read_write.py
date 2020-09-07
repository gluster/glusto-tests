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
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.brick_libs import (
    get_all_bricks,
    bring_bricks_offline,
    bring_bricks_online,
    are_bricks_offline)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_volume_in_split_brain)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.io.utils import validate_io_procs


@runs_on([['arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestAfrReadWrite(GlusterBaseClass):

    """
    Description:
        Arbiter test writes and reads from a file
    """
    def setUp(self):
        # Calling GlusterBaseClass
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def _bring_bricks_online_heal(self, mnode, volname, bricks_list):
        """
        Bring bricks online and monitor heal completion
        """
        # Bring bricks online
        ret = bring_bricks_online(
            mnode, volname, bricks_list,
            bring_bricks_online_methods=['volume_start_force'])
        self.assertTrue(ret, 'Failed to bring bricks online')

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(mnode, volname)
        self.assertTrue(ret, ("Failed to wait for volume {} processes to "
                              "be online".format(volname)))

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(mnode, volname)
        self.assertTrue(ret, ("Volume {} : All process are not online".format
                              (volname)))
        g.log.info("Volume %s : All process are online", volname)

        # Monitor heal completion
        ret = monitor_heal_completion(mnode, volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check for split-brain
        ret = is_volume_in_split_brain(mnode, volname)
        self.assertFalse(ret, 'Volume is in split-brain state')

    def test_afr_read_write(self):
        """
        Test read and write of file
        Description:
        - Get the bricks from the volume
        - Creating directory test_write_and_read_file
        - Write from 1st client
        - Read from 2nd client
        - Select brick to bring offline
        - Bring brick offline
        - Validating IO's on client1
        - Validating IO's on client2
        - Bring bricks online
        - Wait for volume processes to be online
        - Verify volume's all process are online
        - Monitor heal completion
        - Check for split-brain
        - Bring 2nd brick offline
        - Check if brick is offline
        - Write from 1st client
        - Read from 2nd client
        - Bring bricks online
        - Wait for volume processes to be online
        - Verify volume's all process are online
        - Monitor heal completion
        - Check for split-brain

        - Get arequal after getting bricks online
        """
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        # Get the bricks from the volume
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # Creating directory test_write_and_read_file
        ret = mkdir(self.mounts[0].client_system,
                    "{}/test_write_and_read_file"
                    .format(self.mounts[0].mountpoint))
        self.assertTrue(ret, "Failed to create directory")
        g.log.info("Directory 'test_write_and_read_file' on %s created "
                   "successfully", self.mounts[0])

        # Write from 1st client
        cmd_to_write = (
            'cd %s/test_write_and_read_file ; for i in `seq 1 5000` ;'
            'do echo -e "Date:`date`\n" >> test_file ;echo -e "'
            '`cal`\n" >> test_file ; done ; cd ..'
            % self.mounts[0].mountpoint)
        proc1 = g.run_async(self.mounts[0].client_system,
                            cmd_to_write)

        # Read from 2nd client
        cmd = ('cd %s/ ;for i in {1..30};'
               'do cat test_write_and_read_file/test_file;done'
               % self.mounts[1].mountpoint)
        proc2 = g.run_async(self.mounts[1].client_system, cmd)

        # Bring brick offline
        bricks_to_bring_offline = sample(bricks_list, 2)
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline[0])
        self.assertTrue(ret, 'Failed to bring bricks {} offline'.
                        format(bricks_to_bring_offline))

        # Check brick is offline
        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_to_bring_offline[0]])
        self.assertTrue(ret, 'Bricks {} are not offline'.
                        format(bricks_to_bring_offline[0]))

        # Validating IO's
        for proc, mount in zip([proc1, proc2], self.mounts):
            ret = validate_io_procs([proc], mount)
            self.assertTrue(ret, "IO failed on client")
        g.log.info("Successfully validated all IO's")

        self._bring_bricks_online_heal(self.mnode, self.volname, bricks_list)

        # Bring down second brick
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline[1])
        self.assertTrue(ret, 'Failed to bring bricks {} offline'.
                        format(bricks_to_bring_offline[1]))

        # Check if brick is offline
        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_to_bring_offline[1]])
        self.assertTrue(ret, 'Bricks {} are not offline'.
                        format(bricks_to_bring_offline[1]))

        # Write from 1st client
        ret, _, _ = g.run(self.mounts[0].client_system, cmd_to_write)
        self.assertEqual(ret, 0, "Failed to write to file")
        g.log.info("Successfully written to file")

        # Read from 2nd client
        cmd = ('cd %s/ ;cat test_write_and_read_file/test_file'
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to read file on mountpoint")
        g.log.info("Successfully read file on mountpoint")

        self._bring_bricks_online_heal(self.mnode, self.volname, bricks_list)
