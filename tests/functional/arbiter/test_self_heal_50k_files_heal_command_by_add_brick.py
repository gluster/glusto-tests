#  Copyright (C) 2015-2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online,
    wait_for_volume_process_to_be_online)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.gluster.brick_libs import (select_bricks_to_bring_offline,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain,
                                          is_shd_daemonized)
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.io.utils import (collect_mounts_arequal, validate_io_procs)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs']])
class TestSelfHeal(GlusterBaseClass):
    """
    Description:
        Arbiter Test cases related to
        healing in default configuration of the volume
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Overriding the volume type to specifically test the volume type
        # Change from distributed-replicated to arbiter
        if cls.volume_type == "distributed-replicated":
            cls.volume['voltype'] = {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 3,
                'arbiter_count': 1,
                'transport': 'tcp'}

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """

        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_self_heal_50k_files_heal_command_by_add_brick(self):
        """
        Test self-heal of 50k files (heal command)
        Description:
        - Set the volume option
          "metadata-self-heal": "off"
          "entry-self-heal": "off"
          "data-self-heal": "off"
          "self-heal-daemon": "off"
        - Bring down all bricks processes from selected set
        - Create IO (50k files)
        - Get arequal before getting bricks online
        - Bring bricks online
        - Set the volume option
          "self-heal-daemon": "on"
        - Check for daemons
        - Start healing
        - Check if heal is completed
        - Check for split-brain
        - Get arequal after getting bricks online and compare with
          arequal before getting bricks online
        - Add bricks
        - Do rebalance
        - Get arequal after adding bricks and compare with
          arequal after getting bricks online
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Setting options
        options = {"metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "data-self-heal": "off",
                   "self-heal-daemon": "off"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options')
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = list(filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks'])))

        # Bring brick offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # Creating files on client side
        all_mounts_procs = []

        # Create 50k files
        g.log.info('Creating files...')
        command = ("cd %s ; "
                   "for i in `seq 1 50000` ; "
                   "do dd if=/dev/urandom of=test.$i "
                   "bs=100k count=1 ;  "
                   "done ;"
                   % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts[0]),
            "IO failed on some of the clients"
        )

        # Get arequal before getting bricks online
        ret, result_before_online = collect_mounts_arequal(self.mounts[0])
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks online '
                   'is successful')

        # Bring brick online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bringing bricks %s online is successful',
                   bricks_to_bring_offline)

        # Setting options
        ret = set_volume_options(self.mnode, self.volname,
                                 {"self-heal-daemon": "on"})
        self.assertTrue(ret, 'Failed to set option self-heal-daemon to ON.')
        g.log.info("Option 'self-heal-daemon' is set to 'on' successfully")

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

        # Wait for self-heal-daemons to be online
        ret = is_shd_daemonized(self.all_servers)
        self.assertTrue(ret, "Either No self heal daemon process found")
        g.log.info("All self-heal-daemons are online")

        # Start healing
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not started')
        g.log.info('Healing is started')

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=3600)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')

        # Get arequal after getting bricks online
        ret, result_after_online = collect_mounts_arequal(self.mounts[0])
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Checking arequals before bringing bricks online
        # and after bringing bricks online
        self.assertItemsEqual(result_before_online, result_after_online,
                              'Checksums before and '
                              'after bringing bricks online are not equal')
        g.log.info('Checksums before and after bringing bricks online '
                   'are equal')

        # Add bricks
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume when IO in "
                              "progress on volume %s", self.volname))
        g.log.info("Expanding volume is successful on volume %s", self.volname)

        # Do rebalance and wait for it to complete
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Failed to start rebalance')
        g.log.info('Rebalance is started')
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=3600)
        self.assertTrue(ret, 'Rebalance is not completed')
        g.log.info('Rebalance is completed successfully')

        # Get arequal after adding bricks
        ret, result_after_adding_bricks = collect_mounts_arequal(
            self.mounts[0])
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks '
                   'is successful')

        # Checking arequals after bringing bricks online
        # and after adding bricks
        self.assertItemsEqual(result_after_online, result_after_adding_bricks,
                              'Checksums after bringing bricks online and '
                              'after adding bricks are not equal')
        g.log.info('Checksums after bringing bricks online and '
                   'after adding bricks are equal')
