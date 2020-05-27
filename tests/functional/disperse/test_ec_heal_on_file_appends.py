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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from random import sample
from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import (
    bring_bricks_offline,
    bring_bricks_online,
    are_bricks_offline,
    validate_xattr_on_all_bricks,
    get_online_bricks_list)
from glustolibs.gluster.glusterfile import get_file_stat
from glustolibs.misc.misc_libs import kill_process


@runs_on([['dispersed'], ['glusterfs']])
class TestHealOnFileAppends(GlusterBaseClass):
    """
    Test to verify heal on dispersed volume on file appends
    """

    def setUp(self):

        self.get_super_method(self, 'setUp')()
        self.mount_obj = self.mounts[0]
        self.client = self.mount_obj.client_system

        # Setup and mount the volume
        ret = self.setup_volume_and_mount_volume(mounts=[self.mount_obj])
        if not ret:
            raise ExecutionError("Failed to create and mount volume")
        g.log.info("Created and Mounted volume successfully")

        self.offline_bricks = []
        self.is_io_started = False
        self.file_name = 'test_file'

    def tearDown(self):

        # Kill the IO on client
        if self.is_io_started:
            ret = kill_process(self.client, process_names=[self.file_name])
            if not ret:
                raise ExecutionError("Not able to kill/stop IO in client")
        g.log.info('Successfully stopped IO in client')

        if self.offline_bricks:
            ret = bring_bricks_online(self.mnode, self.volname,
                                      self.offline_bricks)
            if not ret:
                raise ExecutionError(ret, 'Not able to bring bricks {} '
                                     'online'.format(self.offline_bricks))

        # Cleanup and unmount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mount_obj])
        if not ret:
            raise ExecutionError("Failed to unmount and cleanup volume")
        g.log.info("Unmount and Cleanup of volume is successful")

        self.get_super_method(self, 'tearDown')()

    def test_heal_on_file_appends(self):
        """
        Test steps:
        - create and mount EC volume 4+2
        - start append to a file from client
        - bring down one of the bricks (say b1)
        - wait for ~minute and bring down another brick (say b2)
        - after ~minute bring up first brick (b1)
        - check the xattrs 'ec.size', 'ec.version'
        - xattrs of online bricks should be same as an indication to heal
        """

        # Get bricks list
        bricks_list = get_online_bricks_list(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, 'Not able to get bricks list')

        # Creating a file, generate and append data to the file
        self.file_name = 'test_file'
        cmd = ("cd %s ;"
               "while true; do "
               "cat /dev/urandom | tr -dc  [:space:][:print:] "
               "| head -c 4K >> %s; sleep 2; "
               "done;"
               % (self.mount_obj.mountpoint, self.file_name))
        ret = g.run_async(self.client, cmd,
                          user=self.mount_obj.user)
        self.assertIsNotNone(ret, "Not able to start IO on client")
        g.log.info('Started generating and appending data to the file')
        self.is_io_started = True

        # Select 3 bricks, 2 need to be offline and 1 will be healthy
        brick_1, brick_2, brick_3 = sample(bricks_list, 3)

        # Wait for IO to fill the bricks
        sleep(30)

        # Bring first brick offline and validate
        ret = bring_bricks_offline(self.volname, [brick_1])
        self.assertTrue(
            ret, 'Failed to bring brick {} offline'.format(brick_1))
        ret = are_bricks_offline(self.mnode, self.volname, [brick_1])
        self.assertTrue(ret, 'Not able to validate brick {} being '
                        'offline'.format(brick_1))
        g.log.info("Brick %s is brought offline successfully", brick_1)
        self.offline_bricks.append(brick_1)

        # Wait for IO to fill the bricks
        sleep(30)

        # Bring second brick offline and validate
        ret = bring_bricks_offline(self.volname, [brick_2])
        self.assertTrue(
            ret, 'Failed to bring brick {} offline'.format(brick_2))
        ret = are_bricks_offline(self.mnode, self.volname, [brick_2])
        self.assertTrue(ret, 'Not able to validate brick {} being '
                        'offline'.format(brick_2))
        g.log.info("Brick %s is brought offline successfully", brick_2)
        self.offline_bricks.append(brick_2)

        # Wait for IO to fill the bricks
        sleep(30)

        # Bring first brick online and validate peer status
        ret = bring_bricks_online(
            self.mnode,
            self.volname,
            [brick_1],
            bring_bricks_online_methods=['glusterd_restart'])
        self.assertTrue(ret, 'Not able to bring brick {} '
                        'online'.format(brick_1))
        g.log.info("Offlined brick %s is brought online successfully", brick_1)
        ret = self.validate_peers_are_connected()
        self.assertTrue(ret, "Peers are not in connected state after bringing "
                        "an offline brick to online via `glusterd restart`")
        g.log.info("Successfully validated peers are in connected state")

        # To catchup onlined brick with healthy bricks
        sleep(30)

        # Validate the xattr to be same on onlined and healthy bric
        online_bricks = get_online_bricks_list(self.mnode, self.volname)
        self.assertIsNotNone(online_bricks, 'Unable to fetch online bricks')
        g.log.info('All online bricks are fetched successfully')
        for xattr in ('trusted.ec.size', 'trusted.ec.version'):
            ret = validate_xattr_on_all_bricks(
                [brick_1, brick_3], self.file_name, xattr)
            self.assertTrue(ret, "{} is not same on all online "
                            "bricks".format(xattr))

        # Get epoch time on the client
        ret, prev_ctime, _ = g.run(self.client, 'date +%s')
        self.assertEqual(ret, 0, 'Not able to get epoch time from client')

        # Headroom for file ctime to get updated
        sleep(5)

        # Validate file was being apended while checking for xattrs
        ret = get_file_stat(
            self.client,
            '{}/{}'.format(self.mount_obj.mountpoint, self.file_name))
        self.assertIsNotNone(ret, "Not able to get stats of the file")
        curr_ctime = ret['epoch_ctime']
        self.assertGreater(int(curr_ctime), int(prev_ctime), "Not able "
                           "to validate data is appended to the file "
                           "while checking for xaatrs")

        g.log.info("Data on all online bricks is healed and consistent")
