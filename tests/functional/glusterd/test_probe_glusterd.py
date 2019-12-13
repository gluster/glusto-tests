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

""" Description:
        Test Cases in this module related to peer probe invalid ip,
        non existing ip, non existing host.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.peer_ops import peer_probe
from glustolibs.gluster.lib_utils import is_core_file_created
from glustolibs.gluster.gluster_init import is_glusterd_running


class PeerProbeInvalidIpNonExistingHost(GlusterBaseClass):

    def test_peer_probe_invalid_ip_nonexist_host_nonexist_ip(self):
        '''
        Test script to verify peer probe non existing ip,
        non_exsting_host and invalid-ip, peer probe has to
        be fail for invalid-ip, non-existing-ip and
        non existing host, verify Glusterd services up and
        running or not after invalid peer probe,
        and core file should not get created
        under "/", /var/log/core and /tmp  directory
        '''
        ret, test_timestamp, _ = g.run_local('date +%s')
        test_timestamp = test_timestamp.strip()
        g.log.info("Running Test : %s", self.id())

        # Assigning non existing ip to variable
        self.non_exist_ip = '256.256.256.256'

        # Assigning invalid ip to variable
        self.invalid_ip = '10.11.a'

        # Assigning non existing host to variable
        self.non_exist_host = 'abc.lab.eng.blr.redhat.com'

        # Peer probe checks for non existing host
        g.log.info("peer probe checking for non existing host")
        ret, _, _ = peer_probe(self.mnode, self.non_exist_host)
        self.assertNotEqual(ret, 0, "peer probe should fail for "
                                    "non existhost: %s" % self.non_exist_host)
        g.log.info("peer probe failed for non existing host")

        # Peer probe checks for invalid ip
        g.log.info("peer probe checking for invalid ip")
        ret, _, _ = peer_probe(self.mnode, self.invalid_ip)
        self.assertNotEqual(ret, 0, "peer probe shouldfail for "
                                    "invalid ip: %s" % self.invalid_ip)
        g.log.info("peer probe failed for invalid_ip")

        # peer probe checks for non existing ip
        g.log.info("peer probe checking for non existing ip")
        ret, _, _ = peer_probe(self.mnode, self.non_exist_ip)
        self.assertNotEqual(ret, 0, "peer probe should fail for non exist "
                                    "ip :%s" % self.non_exist_ip)
        g.log.info("peer probe failed for non existing ip")

        # Checks Glusterd services running or not after peer probe
        # to invalid host and non existing host

        self.mnode_list = []
        self.mnode_list.append(self.mnode)
        ret = is_glusterd_running(self.mnode_list)
        self.assertEqual(ret, 0, "Glusterd service should be running")

        # Chekcing core file created or not in "/", "/tmp" and
        # "/var/log/core" directory
        ret = is_core_file_created(self.servers, test_timestamp)
        self.assertTrue(ret, "core file found")
