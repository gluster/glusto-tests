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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list, get_volume_status)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.volume_libs import (cleanup_volume)
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         peer_probe_servers,
                                         peer_detach_servers,
                                         nodes_from_pool_list)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.gluster_init import (start_glusterd, stop_glusterd,
                                             is_glusterd_running)


@runs_on([['distributed'], ['glusterfs']])
class TestRebalanceHang(GlusterBaseClass):

    def setUp(self):

        # Performing peer detach
        ret = peer_detach_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to detach servers %s"
                                 % self.servers)
        g.log.info("Peer detach SUCCESSFUL.")
        self.get_super_method(self, 'setUp')()

    def tearDown(self):

        # UnMount Volume
        g.log.info("Starting to Unmount Volume %s", self.volname)
        ret = umount_volume(self.mounts[0].client_system,
                            self.mounts[0].mountpoint, mtype=self.mount_type)
        self.assertTrue(ret, ("Failed to Unmount Volume %s" % self.volname))
        g.log.info("Successfully Unmounted Volume %s", self.volname)

        # Clean up all volumes and peer probe to form cluster
        vol_list = get_volume_list(self.mnode)
        if vol_list is not None:
            for volume in vol_list:
                ret = cleanup_volume(self.mnode, volume)
                if not ret:
                    raise ExecutionError("Failed to cleanup volume")
                g.log.info("Volume deleted successfully : %s", volume)

        # Peer probe detached servers
        pool = nodes_from_pool_list(self.mnode)
        for node in pool:
            peer_detach(self.mnode, node)
        ret = peer_probe_servers(self.mnode, self.servers)
        if not ret:
            raise ExecutionError("Failed to probe peer "
                                 "servers %s" % self.servers)
        g.log.info("Peer probe success for detached "
                   "servers %s", self.servers)
        self.get_super_method(self, 'tearDown')()

    def test_rebalance_hang(self):
        """
        In this test case:
        1. Trusted storage Pool of 2 nodes
        2. Create a distributed volumes with 2 bricks
        3. Start the volume
        4. Mount the volume
        5. Add some data file on mount
        6. Start rebalance with force
        7. kill glusterd on 2nd node
        8. Issue volume related command
        """

        # pylint: disable=too-many-statements
        my_server_info = {
            self.servers[0]: self.all_servers_info[self.servers[0]]
        }
        my_servers = self.servers[0:2]
        index = 1
        ret, _, _ = peer_probe(self.servers[0], self.servers[index])
        self.assertEqual(ret, 0, ("peer probe from %s to %s is failed",
                                  self.servers[0], self.servers[index]))
        g.log.info("peer probe is success from %s to "
                   "%s", self.servers[0], self.servers[index])
        key = self.servers[index]
        my_server_info[key] = self.all_servers_info[key]

        self.volname = "testvol"
        bricks_list = form_bricks_list(self.mnode, self.volname, 2,
                                       my_servers,
                                       my_server_info)
        g.log.info("Creating a volume %s ", self.volname)
        ret, _, _ = volume_create(self.mnode, self.volname,
                                  bricks_list, force=False)
        self.assertEqual(ret, 0, ("Unable"
                                  "to create volume %s" % self.volname))
        g.log.info("Volume created successfully %s", self.volname)

        ret, _, _ = volume_start(self.mnode, self.volname, False)
        self.assertEqual(ret, 0, ("Failed to start the "
                                  "volume %s", self.volname))
        g.log.info("Get all the bricks of the volume")
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        # Mounting a volume
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume %s is not mounted") % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)

        self.all_mounts_procs = []
        # Creating files
        command = ("cd %s/ ; "
                   "for i in `seq 1 10` ; "
                   "do mkdir l1_dir.$i ; "
                   "for j in `seq 1 5` ; "
                   "do mkdir l1_dir.$i/l2_dir.$j ; "
                   "for k in `seq 1 10` ; "
                   "do dd if=/dev/urandom of=l1_dir.$i/l2_dir.$j/test.$k "
                   "bs=128k count=$k ; "
                   "done ; "
                   "done ; "
                   "done ; "
                   % (self.mounts[0].mountpoint))

        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)
        self.io_validation_complete = False
        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        g.log.info("Starting rebalance with force on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname, False, True)
        self.assertEqual(ret, 0, ("Failed to start rebalance for volume %s",
                                  self.volname))
        g.log.info("Successfully rebalance on the volume %s",
                   self.volname)

        ret = stop_glusterd(self.servers[1])
        self.assertTrue(ret, "Failed to stop glusterd on one of the node")
        ret = is_glusterd_running(self.servers[1])
        self.assertNotEqual(ret, 0, ("Glusterd is not stopped on servers %s",
                                     self.servers[1]))
        g.log.info("Glusterd stop on the nodes : %s succeeded",
                   self.servers[1])

        # Wait for fix-layout to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        vol_status = get_volume_status(self.mnode, self.volname)
        self.assertIsNotNone(vol_status, "Failed to get volume "
                             "status for %s" % self.volname)

        # Start glusterd on the node where it is stopped
        ret = start_glusterd(self.servers[1])
        self.assertTrue(ret, "glusterd start on the node failed")
        count = 0
        while count < 60:
            ret = is_glusterd_running(self.servers[1])
            if not ret:
                break
            sleep(2)
            count += 1
        self.assertEqual(ret, 0, "glusterd is not running on %s"
                         % self.servers[1])
        g.log.info("Glusterd start on the nodes : %s "
                   "succeeded", self.servers[1])
