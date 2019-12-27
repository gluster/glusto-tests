#  Copyright (C) 2016-2020  Red Hat, Inc. <http://www.redhat.com>
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

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import rmdir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (setup_volume, cleanup_volume)
from glustolibs.gluster.volume_ops import (get_volume_list)
from glustolibs.gluster.peer_ops import (peer_probe_servers,
                                         peer_detach_servers, peer_probe)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              get_rebalance_status)
from glustolibs.gluster.mount_ops import is_mounted


@runs_on([['distributed'], ['glusterfs']])
class TestRebalanceStatus(GlusterBaseClass):

    def setUp(self):
        self.get_super_method(self, 'setUp')()

        # check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

        # detach all the nodes
        ret = peer_detach_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Peer detach failed to all the servers from "
                                 "the node.")
        g.log.info("Peer detach SUCCESSFUL.")

        # Uploading file_dir script in all client direcotries
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", self.clients)
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                   "file_dir_ops.py")
        ret = upload_scripts(self.clients, self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 self.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   self.clients)

    def tearDown(self):

        # unmount the volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Volume unmount failed for %s" % self.volname)
        for mount_obj in self.mounts:
            ret = rmdir(mount_obj.client_system, mount_obj.mountpoint)
            if not ret:
                raise ExecutionError("Failed to remove directory "
                                     "mount directory.")
            g.log.info("Mount directory is removed successfully")

        # get volumes list and clean up all the volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Error while getting vol list")
        else:
            for volume in vol_list:
                ret = cleanup_volume(self.mnode, volume)
                if ret is True:
                    g.log.info("Volume deleted successfully : %s", volume)
                else:
                    raise ExecutionError("Failed Cleanup the"
                                         " Volume %s" % volume)

        # peer probe all the servers
        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Peer probe failed to all the servers from "
                                 "the node.")

        self.get_super_method(self, 'tearDown')()

    def test_rebalance_status_from_newly_probed_node(self):

        # Peer probe first 3 servers
        servers_info_from_three_nodes = {}
        for server in self.servers[0:3]:
            servers_info_from_three_nodes[
                server] = self.all_servers_info[server]
            # Peer probe the first 3 servers
            ret, _, _ = peer_probe(self.mnode, server)
            self.assertEqual(ret, 0, "Peer probe failed to %s" % server)

        self.volume['servers'] = self.servers[0:3]
        # create a volume using the first 3 nodes
        ret = setup_volume(self.mnode, servers_info_from_three_nodes,
                           self.volume, force=True)
        self.assertTrue(ret, "Failed to create"
                        "and start volume %s" % self.volname)

        # Mounting a volume
        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, "Volume mount failed for %s" % self.volname)

        # Checking volume mounted or not
        ret = is_mounted(self.volname, self.mounts[0].mountpoint, self.mnode,
                         self.mounts[0].client_system, self.mount_type)
        self.assertTrue(ret, "Volume not mounted on mount point: %s"
                        % self.mounts[0].mountpoint)
        g.log.info("Volume %s mounted on %s", self.volname,
                   self.mounts[0].mountpoint)

        # run IOs
        g.log.info("Starting IO on all mounts...")
        self.counter = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 10 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 100 %s" % (sys.version_info.major,
                                              self.script_upload_path,
                                              self.counter,
                                              mount_obj.mountpoint))
            ret = g.run(mount_obj.client_system, cmd)
            self.assertEqual(ret, 0, "IO failed on %s"
                             % mount_obj.client_system)
            self.counter = self.counter + 10

        # add a brick to the volume and start rebalance
        brick_to_add = form_bricks_list(self.mnode, self.volname, 1,
                                        self.servers[0:3],
                                        servers_info_from_three_nodes)
        ret, _, _ = add_brick(self.mnode, self.volname, brick_to_add)
        self.assertEqual(ret, 0, "Failed to add a brick to %s" % self.volname)

        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start rebalance")

        # peer probe a new node from existing cluster
        ret, _, _ = peer_probe(self.mnode, self.servers[3])
        self.assertEqual(ret, 0, "Peer probe failed")

        ret = get_rebalance_status(self.servers[3], self.volname)
        self.assertIsNone(ret, "Failed to get rebalance status")
