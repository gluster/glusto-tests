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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.peer_ops import get_peer_status


class TestGlusterdInfo(GlusterBaseClass):

    def test_validate_glusterd_info(self):
        """
        Steps:
            1. Check for the presence of /var/lib/glusterd/glusterd.info file
            2. Get the UUID of the current NODE
            3. check the value of the uuid returned by executing the command -
                "gluster system:: uuid get "
            4. Check the uuid value shown by other node in the cluster
                for the same node "gluster peer status"
                on one node will give the UUID of the other node
        """
        uuid_list = []
        for server in self.servers:

            # Getting UUID from glusterd.info
            g.log.info("Getting the UUID from glusterd.info")
            ret, glusterd_volinfo, _ = g.run(
                server, "grep -i uuid /var/lib/glusterd/glusterd.info")
            uuid_list.append(glusterd_volinfo)
            glusterd_volinfo = (glusterd_volinfo.split("="))[1]
            self.assertFalse(
                ret, "Failed to run '{}' on '{}' ".format(server, server))
            self.assertIsNotNone(
                glusterd_volinfo, "UUID not found in 'glusterd.info' file ")

            # Getting UUID from cmd 'gluster system uuid get'
            ret, get_uuid, _ = g.run(
                server, "gluster system uuid get | awk {'print $2'}")
            self.assertFalse(ret, "Unable to get the UUID ")
            self.assertIsNotNone(get_uuid, "UUID not found")

            # Checking if both the uuid are same
            self.assertEquals(
                glusterd_volinfo, get_uuid,
                "UUID does not match in host {}".format(server))

            # Geting the UUID from cmd "gluster peer status"
            for node in self.servers:
                for i in get_peer_status(node):
                    uuid_list.append(i["uuid"])
                if server != node:
                    self.assertTrue(
                        get_uuid.replace("\n", "") in uuid_list,
                        "uuid not matched in {}".format(node))

    def test_glusterd_config_file_check(self):
        """
        Steps:
            1. Check the location of glusterd socket file ( glusterd.socket )
                ls  /var/run/ | grep -i glusterd.socket
            2. systemctl is-enabled glusterd -> enabled

        """

        cmd = "ls  /var/run/ | grep -i glusterd.socket"
        ret, out, _ = g.run(self.mnode, cmd)

        # Checking glusterd.socket file
        self.assertFalse(
            ret, "Failed to get glusterd.socket file on '{}'".format(
                self.mnode))
        self.assertEqual(
            out.replace("\n", ""), "glusterd.socket",
            "Failed to get expected output")

        # Checking for glusterd.service is enabled by default
        ret, out, _ = g.run(
            self.mnode, "systemctl is-enabled glusterd.service")
        self.assertFalse(
            ret, "Failed to execute the cmd on {}".format(self.mnode))
        self.assertEqual(
            out.replace("\n", ""), "enabled",
            "Output of systemctl is-enabled glusterd.service is not enabled")
