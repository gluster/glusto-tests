#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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

""" Description
Test Cases in this module is to validate the auth.reject volume option
with "*" as value. All clients will be rejected with this as value for
auth.reject,then will reset the volume mount again.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.volume_ops import (get_volume_info, set_volume_options,
                                           volume_reset)
from glustolibs.gluster.mount_ops import mount_volume, is_mounted
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.brick_libs import get_all_bricks, are_bricks_online
from glustolibs.gluster.volume_libs import cleanup_volume


@runs_on([['replicated'],
          ['glusterfs']])
class AuthRejectVol(GlusterBaseClass):
    """
    Create a replicated volume and start the volume and check
    if volume is started
    """
    def setUp(self):
        # Setup Volume to create a replicated volume
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

        # Check if volume is started
        volinfo = get_volume_info(self.mnode, self.volname)
        if volinfo[self.volname]['statusStr'] != "Started":
            raise ExecutionError("Volume has not Started")
        g.log.info("Volume is started.")
        # Calling GlusterBaseClass Setup
        self.get_super_method(self, 'setUp')()

    def tearDown(self):
        # tearDown for every test
        # Cleanup all successfully created volumes.
        ret = cleanup_volume(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Failed to Cleanup the "
                                 "Volume %s" % self.volname)
        g.log.info("Volume deleted successfully "
                   ": %s", self.volname)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_validate_authreject_vol(self):
        """
        -Set Authentication
        -For all the clients
        -Fetch the bricks
        -Check if bricks are online
        -Create directory
        -Mount the volume
        -Check if it is mounted
        -Check authentication logs
        -Reset the Volume
        -Check if bricks are online
        -Mounting the vol on client1
        """
        # pylint: disable=too-many-statements
        # Set Authentication
        option = {"auth.reject": "\"*\""}
        ret = set_volume_options(self.mnode, self.volname,
                                 option)
        self.assertTrue(ret, "Failed to set authentication")
        g.log.info("Authentication set Successfully")

        for client in self.clients:
            # Fetching all the bricks
            self.mountpoint = '/mnt/testvol'
            g.log.info("Fetching bricks for the volume : %s", self.volname)
            bricks_list = get_all_bricks(self.mnode, self.volname)
            self.assertIsNotNone(bricks_list, "Brick list is empty")
            g.log.info("Brick List : %s", bricks_list)

            # Check are bricks online
            ret = are_bricks_online(self.mnode, self.volname, bricks_list)
            self.assertTrue(ret, "All bricks are not online")
            g.log.info("All bricks are online")

            # Creating directory to mount
            cmd = ("mkdir -p /mnt/testvol")
            ret, _, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, "Failed to create directory")

            # Using this way to check because of bug 1586036
            # Mounting volume
            ret, _, _ = mount_volume(self.volname, self.mount_type,
                                     self.mountpoint,
                                     self.mnode,
                                     client)

            # Checking if volume is mounted
            out = is_mounted(self.volname, self.mountpoint, self.mnode,
                             client, self.mount_type, user='root')
            if (ret == 0) & (not out):
                g.log.error("Mount executed successfully due to bug 1586036")
            elif (ret == 1) & (not out):
                g.log.info("Expected:Mounting has failed successfully")
            else:
                raise ExecutionError("Unexpected Mounting of Volume %s"
                                     "successful" % self.volname)

            # Checking client logs for authentication error
            cmd = ("grep AUTH_FAILED /var/log/glusterfs/mnt-"
                   "testvol.log")
            ret, _, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, "Mounting has not failed due to"
                             "authentication error")
            g.log.info("Mounting has failed due to authentication error")

        # Reset Volume
        ret, _, _ = volume_reset(mnode=self.mnode, volname=self.volname)
        self.assertEqual(ret, 0, "Failed to reset volume")
        g.log.info("Volume %s reset operation is successful", self.volname)

        # Check if bricks are online and  Mounting the vol on client1
        # Fetching bricks
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Brick list is empty")
        g.log.info("Brick List : %s", bricks_list)

        # Checking if bricks are online
        ret = are_bricks_online(self.mnode, self.volname, bricks_list)
        self.assertTrue(ret, "All bricks are not online")
        g.log.info("All bricks are online")

        # Creating directory to mount
        cmd = ("mkdir -p /mnt/testvol")
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to create directory")

        # Mounting Volume
        ret, _, _ = mount_volume(self.volname, self.mount_type,
                                 self.mountpoint, self.servers[0],
                                 self.clients[0])
        self.assertEqual(ret, 0, "Failed to mount volume")
        g.log.info("Mounted Successfully")

        # Checking if Volume is mounted
        out = is_mounted(self.volname, self.mountpoint, self.servers[0],
                         self.clients[0], self.mount_type,
                         user='root')
        self.assertTrue(out, "Volume %s has failed to mount"
                        % self.volname)
        g.log.info("Volume is mounted successfully %s", self.volname)
