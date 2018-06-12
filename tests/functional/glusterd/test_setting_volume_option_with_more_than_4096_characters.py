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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import runs_on, GlusterBaseClass
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import setup_volume
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.gluster_init import (restart_glusterd,
                                             is_glusterd_running)


@runs_on([['distributed'], ['glusterfs']])
class TestVolumeOptionSetWithMaxcharacters(GlusterBaseClass):

    def setUp(self):

        GlusterBaseClass.setUp.im_func(self)

        # check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

    def tearDown(self):

        count = 0
        while count < 60:
            ret = self.validate_peers_are_connected()
            if ret:
                break
            sleep(2)
            count += 1

        if not ret:
            raise ExecutionError("Peers are not in connected state")

        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup the Volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        GlusterBaseClass.tearDown.im_func(self)

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
        count = 0
        while count < 60:
            ret = is_glusterd_running(self.mnode)
            if not ret:
                break
            sleep(2)
            count += 1
        self.assertEqual(ret, 0, "glusterd is not running on %s" % self.mnode)
