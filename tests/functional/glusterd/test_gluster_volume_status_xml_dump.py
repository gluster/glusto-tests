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
# GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Description:
    Test Default volume behavior and quorum options
"""
from time import sleep

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.volume_ops import (
    volume_stop, get_volume_status,
    volume_create, volume_start
)


@runs_on([['distributed-arbiter'],
          ['glusterfs']])
class GetVolumeStatusXmlDump(GlusterBaseClass):

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Fetching all the parameters for volume_create
        list_of_three_servers = []
        server_info_for_three_nodes = {}

        for server in self.servers[0:3]:
            list_of_three_servers.append(server)
            server_info_for_three_nodes[server] = self.all_servers_info[
                server]

        bricks_list = form_bricks_list(
            self.mnode, self.volname, 3, list_of_three_servers,
            server_info_for_three_nodes)
        # Creating 2nd volume
        self.volname_2 = "test_volume"
        ret, _, _ = volume_create(self.mnode, self.volname_2,
                                  bricks_list)
        self.assertFalse(ret, "Volume creation failed")
        g.log.info("Volume %s created successfully", self.volname_2)
        ret, _, _ = volume_start(self.mnode, self.volname_2)
        if ret:
            raise ExecutionError(
                "Failed to start volume {}".format(self.volname_2))
        # Setup and mount the volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume and mount it")

    def test_gluster_volume_status_xml_dump(self):
        """
        Setps:
        1. stop one of the volume
            (i.e) gluster volume stop <vol-name>
        2. Get the status of the volumes with --xml dump
            XML dump should be consistent
        """
        ret, _, _ = volume_stop(self.mnode, volname=self.volname_2,
                                force=True)
        self.assertFalse(ret,
                         "Failed to stop volume '{}'".format(
                             self.volname_2))
        out = get_volume_status(self.mnode)
        self.assertIsNotNone(
            out, "Failed to get volume status on {}".format(self.mnode))
        for _ in range(4):
            sleep(2)
            out1 = get_volume_status(self.mnode)
            self.assertIsNotNone(
                out1, "Failed to get volume status on {}".format(
                    self.mnode))
            self.assertEqual(out1, out)

    def tearDown(self):
        """tear Down Callback"""
        ret = cleanup_volume(self.mnode, self.volname_2)
        if not ret:
            raise ExecutionError(
                "Failed to remove volume '{}'".format(self.volname_2))
        # Unmount volume and cleanup.
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup volume")
        g.log.info("Successful in unmount and cleanup operations")
        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
