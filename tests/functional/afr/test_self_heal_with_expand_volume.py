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
from glustolibs.gluster.glusterfile import (set_file_permissions,
                                            occurences_of_pattern_in_file)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete)
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_rebalance_to_complete)
from glustolibs.gluster.lib_utils import (add_user, del_user)
from glustolibs.gluster.volume_libs import (get_subvols, expand_volume)


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestHealWithExpandVolume(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        self.first_client = self.mounts[0].client_system
        self.mountpoint = self.mounts[0].mountpoint

        # Create non-root users
        self.users = ('qa_user', 'qa_admin')
        for user in self.users:
            if not add_user(self.first_client, user):
                raise ExecutionError("Failed to create non-root user {}"
                                     .format(user))
        g.log.info("Successfully created non-root users")

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to setup and mount volume")

    def tearDown(self):

        # Delete non-root users
        for user in self.users:
            del_user(self.first_client, user)
            ret, _, _ = g.run(self.first_client,
                              "rm -rf /home/{}".format(user))
            if ret:
                raise ExecutionError("Failed to remove home dir of "
                                     "non-root user")
        g.log.info("Successfully deleted all users")

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
        self.bricks_to_bring_offline.append(choice(subvols[0]))

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

    def _check_if_there_are_files_to_be_healed(self):
        """Check if there are files and dirs to be healed"""
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertFalse(ret, 'Heal is completed')
        g.log.info('Heal is pending')

    def _expand_volume_and_wait_for_rebalance_to_complete(self):
        """Expand volume and wait for rebalance to complete"""
        # Add brick to volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)

        # Trigger rebalance and wait for it to complete
        ret, _, _ = rebalance_start(self.mnode, self.volname,
                                    force=True)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=6000)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

    def test_self_heal_and_add_brick_with_data_from_diff_users(self):
        """
        Test case:
        1. Created a 2X3 volume.
        2. Mount the volume using FUSE and give 777 permissions to the mount.
        3. Added a new user.
        4. Login as new user and created 100 files from the new user:
           for i in {1..100};do dd if=/dev/urandom of=$i bs=1024 count=1;done
        5. Kill a brick which is part of the volume.
        6. On the mount, login as root user and create 1000 files:
           for i in {1..1000};do dd if=/dev/urandom of=f$i bs=10M count=1;done
        7. On the mount, login as new user, and copy existing data to
           the mount.
        8. Start volume using force.
        9. While heal is in progress, add-brick and start rebalance.
        10. Wait for rebalance and heal to complete,
        11. Check for MSGID: 108008 errors in rebalance logs.
        """
        # Change permissions of mount point to 777
        ret = set_file_permissions(self.first_client, self.mountpoint,
                                   '-R 777')
        self.assertTrue(ret, "Unable to change mount point permissions")
        g.log.info("Mount point permissions set to 777")

        # Create 100 files from non-root user
        cmd = ("su -l %s -c 'cd %s; for i in {1..100};do dd if=/dev/urandom "
               "of=nonrootfile$i bs=1024 count=1; done'" % (self.users[0],
                                                            self.mountpoint))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create files from non-root user")

        # Kill one brick which is part of the volume
        self._bring_bricks_offline()

        # Create 1000 files from root user
        cmd = ("cd %s; for i in {1..1000};do dd if=/dev/urandom of=rootfile$i"
               " bs=10M count=1;done" % self.mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to creare files from root user")

        # On the mount, login as new user, and copy existing data to
        # the mount
        cmd = ("su -l %s -c 'wget https://cdn.kernel.org/pub/linux/kernel/"
               "v5.x/linux-5.4.54.tar.xz; tar -xvf linux-5.4.54.tar.xz;"
               "cd %s; cp -r ~/ .;'" % (self.users[1], self.mountpoint))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to copy files from non-root user")

        # Check if there are files to be healed
        self._check_if_there_are_files_to_be_healed()

        # Start the vol using force
        self._restart_volume_and_bring_all_offline_bricks_online()

        # Add bricks to volume and wait for heal to complete
        self._expand_volume_and_wait_for_rebalance_to_complete()

        # Wait for heal to complete
        self._wait_for_heal_to_completed()

        # Check for MSGID: 108008 errors in rebalance logs
        particiapting_nodes = []
        for brick in get_all_bricks(self.mnode, self.volname):
            node, _ = brick.split(':')
            particiapting_nodes.append(node)

        for server in particiapting_nodes:
            ret = occurences_of_pattern_in_file(
                server, "MSGID: 108008",
                "/var/log/glusterfs/{}-rebalance.log".format(self.volname))
            self.assertEqual(ret, 0,
                             "[Input/output error] present in rebalance log"
                             " file")
        g.log.info("Expanding volume successful and no MSGID: 108008 "
                   "errors see in rebalance logs")
