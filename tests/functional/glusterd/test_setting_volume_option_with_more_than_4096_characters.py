#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.gluster_base_class import runs_on, GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.peer_ops import wait_for_peers_to_connect
from glustolibs.gluster.volume_libs import setup_volume
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.gluster_init import (restart_glusterd,
                                             wait_for_glusterd_to_start)


@runs_on([['distributed'], ['glusterfs']])
class TestVolumeOptionSetWithMaxcharacters(GlusterBaseClass):

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

    def tearDown(self):

        ret = wait_for_peers_to_connect(self.mnode, self.servers)
        self.assertTrue(ret, "glusterd is not connected %s with peer %s"
                        % (self.servers, self.servers))

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup the Volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_setting_vol_option_with_max_characters(self):

        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, ("Failed to create "
                              "and start volume %s" % self.volname))
        auth_list = []
        for ip_addr in range(256):
            auth_list.append('192.168.122.%d' % ip_addr)
        for ip_addr in range(7):
            auth_list.append('192.168.123.%d' % ip_addr)
        ip_list = ','.join(auth_list)

        # set auth.allow with <4096 characters and restart the glusterd
        g.log.info("Setting auth.allow with string of length %d for %s",
                   len(ip_list), self.volname)
        self.options = {"auth.allow": ip_list}
        ret = set_volume_options(self.mnode, self.volname, self.options)
        self.assertTrue(ret, ("Failed to set auth.allow with string of length"
                              " %d for %s" % (len(ip_list), self.volname)))
        ret = restart_glusterd(self.mnode)
        self.assertTrue(ret, "Failed to restart the glusterd on %s"
                        % self.mnode)

        # set auth.allow with >4096 characters and restart the glusterd
        ip_list = ip_list + ",192.168.123.7"
        self.options = {"auth.allow": ip_list}
        g.log.info("Setting auth.allow with string of length %d for %s",
                   len(ip_list), self.volname)
        ret = set_volume_options(self.mnode, self.volname, self.options)
        self.assertTrue(ret, ("Failed to set auth.allow with string of length"
                              " %d for %s" % (len(ip_list), self.volname)))
        ret = restart_glusterd(self.mnode)
        self.assertTrue(ret, "Failed to restart the glusterd on %s"
                        % self.mnode)

        ret = wait_for_glusterd_to_start(self.servers)
        self.assertTrue(ret, "glusterd is not running on %s"
                        % self.servers)
        g.log.info("Glusterd start on the nodes : %s "
                   "succeeded", self.servers)
