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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from random import choice

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           are_bricks_online, get_all_bricks)
from glustolibs.gluster.glusterfile import occurences_of_pattern_in_file
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete)
from glustolibs.gluster.lib_utils import collect_bricks_arequal
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.volume_ops import get_volume_options
from glustolibs.io.utils import collect_mounts_arequal


@runs_on([['distributed-replicated', 'replicated',
           'arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestDefaultGranularEntryHeal(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        self.first_client = self.mounts[0].client_system
        self.mountpoint = self.mounts[0].mountpoint

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to setup and mount volume")

    def tearDown(self):

        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _bring_bricks_offline(self):
        """Brings bricks offline and confirms if they are offline"""
        # Select bricks to bring offline from a replica set
        subvols_dict = get_subvols(self.mnode, self.volname)
        subvols = subvols_dict['volume_subvols']
        self.bricks_to_bring_offline = []
        for subvol in subvols:
            self.bricks_to_bring_offline.append(choice(subvol))

        # Bring bricks offline
        ret = bring_bricks_offline(self.volname, self.bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        self.bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 self.bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % self.bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   self.bricks_to_bring_offline)

    def _restart_volume_and_bring_all_offline_bricks_online(self):
        """Restart volume and bring all offline bricks online"""

        ret = is_heal_complete(self.mnode, self.volname)
        self.assertFalse(ret, 'Heal is completed')
        g.log.info('Heal is pending')

        ret = bring_bricks_online(self.mnode, self.volname,
                                  self.bricks_to_bring_offline,
                                  bring_bricks_online_methods=[
                                      'volume_start_force'])
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        self.bricks_to_bring_offline)

        # Check if bricks are back online or not
        ret = are_bricks_online(self.mnode, self.volname,
                                self.bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks not online %s even after restart' %
                        self.bricks_to_bring_offline)

        g.log.info('Bringing bricks %s online is successful',
                   self.bricks_to_bring_offline)

    def _wait_for_heal_to_completed(self):
        """Check if heal is completed"""
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=3600)
        self.assertTrue(ret, 'Heal has not yet completed')

    def _check_arequal_on_bricks_with_a_specific_arequal(self, arequal,
                                                         brick_list):
        """
        Compare an inital arequal checksum with bricks from a given brick list
        """
        init_val = arequal[0].splitlines()[-1].split(':')[-1]
        ret, arequals = collect_bricks_arequal(brick_list)
        self.assertTrue(ret, 'Failed to get arequal on bricks')
        for brick_arequal in arequals:
            brick_total = brick_arequal.splitlines()[-1].split(':')[-1]
            self.assertEqual(init_val, brick_total, 'Arequals not matching')

    @staticmethod
    def _add_dir_path_to_brick_list(brick_list):
        """Add test_self_heal at the end of brick path"""
        dir_brick_list = []
        for brick in brick_list:
            dir_brick_list.append('{}/{}'.format(brick, 'mydir'))
        return dir_brick_list

    def _check_arequal_checksum_for_the_volume(self):
        """
        Check if arequals of mount point and bricks are
        are the same.
        """
        if self.volume_type == "replicated":
            # Check arequals for "replicated"
            brick_list = get_all_bricks(self.mnode, self.volname)
            dir_brick_list = self._add_dir_path_to_brick_list(brick_list)

            # Get arequal before getting bricks offline
            work_dir = '{}/mydir'.format(self.mountpoint)
            ret, arequals = collect_mounts_arequal([self.mounts[0]],
                                                   path=work_dir)
            self.assertTrue(ret, 'Failed to get arequal')
            g.log.info('Getting arequal before getting bricks offline '
                       'is successful')

            # Get arequal on bricks and compare with mount_point_total
            self._check_arequal_on_bricks_with_a_specific_arequal(
                arequals, dir_brick_list)

        # Check arequals for "distributed-replicated"
        if self.volume_type == "distributed-replicated":
            # Get the subvolumes
            subvols_dict = get_subvols(self.mnode, self.volname)
            num_subvols = len(subvols_dict['volume_subvols'])

            # Get arequals and compare
            for i in range(0, num_subvols):
                # Get arequal for first brick
                brick_list = subvols_dict['volume_subvols'][i]
                dir_brick_list = self._add_dir_path_to_brick_list(brick_list)
                ret, arequals = collect_bricks_arequal([dir_brick_list[0]])
                self.assertTrue(ret, 'Failed to get arequal on first brick')

                # Get arequal for every brick and compare with first brick
                self._check_arequal_on_bricks_with_a_specific_arequal(
                    arequals, dir_brick_list)

    def test_default_granular_entry_heal(self):
        """
        Test case:
        1. Create a cluster.
        2. Create volume start it and mount it.
        3. Check if cluster.granular-entry-heal is ON by default or not.
        4. Check /var/lib/glusterd/<volname>/info for
           cluster.granular-entry-heal=on.
        5. Check if option granular-entry-heal is present in the
           volume graph or not.
        6. Kill one or two bricks of the volume depending on volume type.
        7. Create all types of files on the volume like text files, hidden
           files, link files, dirs, char device, block device and so on.
        8. Bring back the killed brick by restarting the volume.
        9. Wait for heal to complete.
        10. Check arequal-checksum of all the bricks and see if it's proper or
            not.
        """
        # Check if cluster.granular-entry-heal is ON by default or not
        ret = get_volume_options(self.mnode, self.volname,
                                 'granular-entry-heal')
        self.assertEqual(ret['cluster.granular-entry-heal'], 'on',
                         "Value of cluster.granular-entry-heal not on "
                         "by default")

        # Check var/lib/glusterd/<volname>/info for
        # cluster.granular-entry-heal=on
        ret = occurences_of_pattern_in_file(self.mnode,
                                            'cluster.granular-entry-heal=on',
                                            '/var/lib/glusterd/vols/{}/info'
                                            .format(self.volname))
        self.assertEqual(ret, 1, "Failed get cluster.granular-entry-heal=on in"
                         " info file")

        # Check if option granular-entry-heal is present in the
        # volume graph or not
        ret = occurences_of_pattern_in_file(self.first_client,
                                            'option granular-entry-heal on',
                                            "/var/log/glusterfs/mnt-{}_{}.log"
                                            .format(self.volname,
                                                    self.mount_type))
        self.assertTrue(ret > 0,
                        "Failed to find granular-entry-heal in volume graph")
        g.log.info("granular-entry-heal properly set to ON by default")

        # Kill one or two bricks of the volume depending on volume type
        self._bring_bricks_offline()

        # Create all types of files on the volume like text files, hidden
        # files, link files, dirs, char device, block device and so on
        cmd = ("cd {};mkdir mydir;cd mydir;mkdir dir;mkdir .hiddendir;"
               "touch file;touch .hiddenfile;mknod blockfile b 1 5;"
               "mknod charfile b 1 5; mkfifo pipefile;touch fileforhardlink;"
               "touch fileforsoftlink;ln fileforhardlink hardlinkfile;"
               "ln -s fileforsoftlink softlinkfile".format(self.mountpoint))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create files of all types")

        # Bring back the killed brick by restarting the volume	Bricks should
        # be online again
        self._restart_volume_and_bring_all_offline_bricks_online()

        # Wait for heal to complete
        self._wait_for_heal_to_completed()

        # Check arequal-checksum of all the bricks and see if it's proper or
        # not
        self._check_arequal_checksum_for_the_volume()
