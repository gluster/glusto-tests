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

import socket
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         peer_probe_servers,
                                         nodes_from_pool_list)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list, volume_stop,
                                           get_volume_info, get_volume_status)
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.exceptions import ExecutionError


class TestPeerProbe(GlusterBaseClass):

    def setUp(self):
        # Performing peer detach
        for server in self.servers[1:]:
            # Peer detach
            ret, _, _ = peer_detach(self.mnode, server)
            if ret:
                raise ExecutionError("Peer detach failed")
            g.log.info("Peer detach SUCCESSFUL.")

        GlusterBaseClass.setUp.im_func(self)

    def tearDown(self):

        # Peer probe detached servers
        pool = nodes_from_pool_list(self.mnode)
        for node in pool:
            peer_detach(self.mnode, node)
        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to probe detached "
                                 "servers %s" % self.servers)
        g.log.info("Peer probe success for detached "
                   "servers %s", self.servers)

        # clean up all volumes and detaches peers from cluster

        vol_list = get_volume_list(self.mnode)
        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Failed to Cleanup the "
                                     "Volume %s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_peer_probe_validation(self):
        '''
        -> Create trusted storage pool, by probing with networkshort names
        -> Create volume using IP of host
        -> perform basic operations like
            -> gluster volume start <vol>
            -> gluster volume info <vol>
            -> gluster volume status <vol>
            -> gluster volume stop <vol>
        -> Create a volume using the FQDN of the host
        -> perform basic operations like
            -> gluster volume start <vol>
            -> gluster volume info <vol>
            -> gluster volume status <vol>
            -> gluster volume stop <vol>
        '''
        # Peer probing using short name
        for server in self.servers[1:]:
            ret, hostname, _ = g.run(server, "hostname -s")
            self.assertEqual(ret, 0, ("Unable to get short name "
                                      "for server % s" % server))
            ret, _, _ = peer_probe(self.mnode, hostname)
            self.assertEqual(ret, 0, "Unable to peer"
                             "probe to the server % s" % hostname)
            g.log.info("Peer probe succeeded for server %s", hostname)

        # Create a volume
        self.volname = "test-vol"
        self.brick_list = form_bricks_list(self.mnode, self.volname, 3,
                                           self.servers,
                                           self.all_servers_info)
        g.log.info("Creating a volume")
        ret, _, _ = volume_create(self.mnode, self.volname,
                                  self.brick_list, force=False)
        self.assertEqual(ret, 0, "Unable"
                         "to create volume % s" % self.volname)
        g.log.info("Volume created successfully % s", self.volname)

        # Start a volume
        g.log.info("Start a volume")
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Unable"
                         "to start volume % s" % self.volname)
        g.log.info("Volume started successfully % s", self.volname)

        # Get volume info
        g.log.info("get volume info")
        volinfo = get_volume_info(self.mnode, self.volname)
        self.assertIsNotNone(volinfo, "Failed to get the volume "
                                      "info for %s" % self.volname)

        # Get volume status
        vol_status = get_volume_status(self.mnode, self.volname)
        self.assertIsNotNone(vol_status, "Failed to get volume "
                                         "status for %s" % self.volname)

        # stop volume
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Unable"
                         "to stop volume % s" % self.volname)
        g.log.info("Volume stopped successfully % s", self.volname)

        # Create a volume
        self.volname = "test-vol-fqdn"

        self.brick_list = form_bricks_list(self.mnode, self.volname, 3,
                                           self.servers,
                                           self.all_servers_info)

        # Getting FQDN (Full qualified domain name) of each host and
        # replacing ip with FQDN name for each brick for example
        # 10.70.37.219:/bricks/brick0/vol1 is a brick, here ip is replaced
        # with FQDN name now brick looks like
        # dhcp35-219.lab.eng.blr.redhat.com:/bricks/brick0/vol1

        my_brick_list = []
        for brick in self.brick_list:
            fqdn_list = brick.split(":")
            fqdn = socket.getfqdn(fqdn_list[0])
            fqdn = fqdn + ":" + fqdn_list[1]
            my_brick_list.append(fqdn)

        g.log.info("Creating a volume")
        ret, _, _ = volume_create(self.mnode, self.volname,
                                  my_brick_list, force=False)
        self.assertEqual(ret, 0, "Unable"
                         "to create volume % s" % self.volname)
        g.log.info("Volume created successfully % s", self.volname)

        # Start a volume
        g.log.info("Start a volume")
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Unable"
                         "to start volume % s" % self.volname)
        g.log.info("Volume started successfully % s", self.volname)

        # Get volume info
        g.log.info("get volume info")
        volinfo = get_volume_info(self.mnode, self.volname)
        self.assertIsNotNone(volinfo, "Failed to get the volume "
                                      "info for %s" % self.volname)

        # Get volume status
        vol_status = get_volume_status(self.mnode, self.volname)
        self.assertIsNotNone(vol_status, "Failed to get volume "
                                         "status for %s" % self.volname)

        # stop volume
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Unable"
                         "to stop volume % s" % self.volname)
        g.log.info("Volume stopped successfully % s", self.volname)
