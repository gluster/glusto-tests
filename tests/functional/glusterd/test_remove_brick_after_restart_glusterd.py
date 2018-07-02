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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.volume_libs import (cleanup_volume)
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach,
                                         peer_probe_servers,
                                         nodes_from_pool_list)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.gluster_init import restart_glusterd


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestRemoveBrickAfterRestartGlusterd(GlusterBaseClass):

    def setUp(self):

        # Performing peer detach
        for server in self.servers[1:]:
            ret, _, _ = peer_detach(self.mnode, server)
            if ret != 0:
                raise ExecutionError("Peer detach failed")
            g.log.info("Peer detach SUCCESSFUL.")
        GlusterBaseClass.setUp.im_func(self)

    def tearDown(self):

        # Cleanup and umount volume
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
        g.log.info("Successful in umounting the volume and Cleanup")

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
        GlusterBaseClass.tearDown.im_func(self)

    def test_remove_brick(self):
        """
        In this test case:
        1. Trusted storage Pool of 4 nodes
        2. Create a distributed-replicated volumes with 4 bricks
        3. Start the volume
        4. Fuse mount the gluster volume on out of trusted nodes
        5. Create some data file
        6. Start remove-brick operation for one replica pair
        7. Restart glusterd on all nodes
        8. Try to commit the remove-brick operation while rebalance
           is in progress, it should fail
        """

        # pylint: disable=too-many-statements
        my_servers = self.servers[0:4]
        my_server_info = {}
        for server in self.servers[0:4]:
            my_server_info[server] = self.all_servers_info[server]
        for index in range(1, 4):
            ret, _, _ = peer_probe(self.servers[0], self.servers[index])
            self.assertEqual(ret, 0, ("peer probe from %s to %s is failed",
                                      self.servers[0], self.servers[index]))
            g.log.info("peer probe is success from %s to "
                       "%s", self.servers[0], self.servers[index])

        self.volname = "testvol"
        bricks_list = form_bricks_list(self.mnode, self.volname, 4,
                                       my_servers,
                                       my_server_info)
        g.log.info("Creating a volume %s ", self.volname)
        kwargs = {}
        kwargs['replica_count'] = 2
        ret = volume_create(self.mnode, self.volname,
                            bricks_list, force=False, **kwargs)
        self.assertEqual(ret[0], 0, ("Unable"
                                     "to create volume %s" % self.volname))
        g.log.info("Volume created successfuly %s", self.volname)

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
        g.log.info("Volume mounted sucessfully : %s", self.volname)

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

        remove_brick_list = bricks_list[2:4]
        ret, _, _ = remove_brick(self.mnode, self.volname, remove_brick_list,
                                 'start')
        self.assertEqual(ret, 0, "Failed to start remove brick operation")
        g.log.info("Remove bricks operation started successfully")
        g.log.info("Restart glusterd on servers %s", self.servers)
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, ("Failed to restart glusterd on servers %s",
                              self.servers))
        g.log.info("Successfully restarted glusterd on servers %s",
                   self.servers)

        ret, _, _ = remove_brick(self.mnode, self.volname, remove_brick_list,
                                 'commit')
        self.assertNotEqual(ret, 0, "Remove brick commit ops should be fail")
        g.log.info("Remove bricks commit operation failure is expected")
