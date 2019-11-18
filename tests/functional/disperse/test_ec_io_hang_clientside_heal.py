#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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

"""
ec_io_hang_during_clientside_heal:
    Disable server side heal.
    Perform IO on mount point and kill some bricks and bring them up.
    Check that the heal should complete via client side heal and it
    should not hang any IO.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.brick_libs import (
    bring_bricks_offline,
    bring_bricks_online,
    are_bricks_online,
    get_all_bricks)
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status)
from glustolibs.gluster.heal_ops import (
    disable_heal)
from glustolibs.gluster.heal_libs import (
    monitor_heal_completion)
from glustolibs.gluster.heal_ops import get_heal_info
from glustolibs.gluster.glusterfile import set_file_permissions


def ec_check_heal_comp(self):
    g.log.info("Get the pending heal info for the volume %s",
               self.volname)
    heal_info = get_heal_info(self.mnode, self.volname)
    g.log.info("Successfully got heal info for the volume %s",
               self.volname)
    g.log.info("Heal Entries %s : %s", self.volname, heal_info)

    # Monitor heal completion
    ret = monitor_heal_completion(self.mnode, self.volname)
    self.assertTrue(ret, 'Heal has not yet completed')


@runs_on([['dispersed'], ['glusterfs']])
class EcClientHealHangTest(GlusterBaseClass):
    # Method to setup the environment for test case
    def setUp(self):
        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=True)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    # Test Case
    def test_heal_client_io_hang(self):
        mountpoint = self.mounts[0].mountpoint

        # disable server side heal
        ret = disable_heal(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to disable server side heal"))
        g.log.info("Successfully disabled server side heal")

        # Log Volume Info and Status after disabling client side heal
        g.log.info("Logging volume info and status")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed "
                              "on volume %s", self.volname))

        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the bricks list")

        # Create files
        cmd = ("cd %s; mkdir test; cd test; for i in `seq 1 100` ;"
               "do touch file$i; done" % mountpoint)

        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Finished creating files while all the bricks are UP')

        # Bring bricks offline
        ret = bring_bricks_offline(self.volname, bricks_list[0:1])
        self.assertTrue(ret, "Failed to bring down the bricks")
        g.log.info("Successfully brought the bricks down")

        # Start pumping IO from client
        cmd = ("cd %s; mkdir test; cd test; for i in `seq 1 100` ;"
               "do dd if=/dev/urandom of=file$i bs=1M "
               "count=5;done" % mountpoint)

        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Finished writing on files while a brick is DOWN')

        # Bring bricks online
        ret = bring_bricks_online(self.mnode, self.volname, bricks_list[0:1])
        self.assertTrue(ret, "Failed to bring up the bricks")
        g.log.info("Successfully brought the bricks up")

        # Verifying all bricks online
        ret = are_bricks_online(self.mnode, self.volname, bricks_list)
        self.assertTrue(ret, "All bricks are not online")

        # Start client side heal by reading/writing files and directories
        appendcmd = ("cd %s; mkdir test; cd test; for i in `seq 1 100` ;"
                     "do dd if=/dev/urandom of=file$i bs=1M "
                     "count=1 oflag=append conv=notrunc;done" % mountpoint)

        readcmd = ("cd %s; mkdir test; cd test; for i in `seq 1 100` ;"
                   "do dd if=file$i of=/dev/null bs=1M "
                   "count=5;done" % mountpoint)
        ret = set_file_permissions(self.mounts[0].client_system,
                                   "%s/test" % mountpoint, 777)
        self.assertTrue(ret, "Failed to set permission for directory")
        g.log.info("Successfully set permissions for directory")

        ret, _, err = g.run(self.mounts[0].client_system, appendcmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Finished append on files after bringing bricks online')

        ret, _, err = g.run(self.mounts[0].client_system, readcmd)
        self.assertEqual(ret, 0, err)
        g.log.info('Finished read on files after bringing bricks online')

        # check the heal info and completion
        ec_check_heal_comp(self)

        # Log Volume Info and Status after bringing the brick up
        g.log.info("Logging volume info and status")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed "
                              "on volume %s", self.volname))
        g.log.info("Successful in logging volume info and status "
                   "of volume %s", self.volname)

    # Method to cleanup test setup
    def tearDown(self):
        # Stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")
