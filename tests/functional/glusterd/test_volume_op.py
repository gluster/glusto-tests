#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           volume_stop, volume_delete,
                                           get_volume_list, get_volume_info)
from glustolibs.gluster.volume_libs import (setup_volume, cleanup_volume)
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach)
from glustolibs.gluster.lib_utils import form_bricks_list


@runs_on([['distributed', 'replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'], ['glusterfs']])
class TestVolumeOperations(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # check whether peers are in connected state
        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

    def tearDown(self):

        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume % s" % volume)
            g.log.info("Volume deleted successfully : %s" % volume)

        GlusterBaseClass.tearDown.im_func(self)

    def test_volume_op(self):

        # Starting a non existing volume should fail
        ret, _, _ = volume_start(self.mnode, "no_vol", force=True)
        self.assertNotEqual(ret, 0, "Expected: It should fail to Start a non"
                            " existing volume. Actual: Successfully started "
                            "a non existing volume")
        g.log.info("Starting a non existing volume is failed")

        # Stopping a non existing volume should fail
        ret, _, _ = volume_stop(self.mnode, "no_vol", force=True)
        self.assertNotEqual(ret, 0, "Expected: It should fail to stop "
                            "non-existing volume. Actual: Successfully "
                            "stopped a non existing volume")
        g.log.info("Stopping a non existing volume is failed")

        # Deleting a non existing volume should fail
        ret = volume_delete(self.mnode, "no_vol")
        self.assertTrue(ret, "Expected: It should fail to delete a "
                        "non existing volume. Actual:Successfully deleted "
                        "a non existing volume")
        g.log.info("Deleting a non existing volume is failed")

        # Detach a server and try to create volume with node
        # which is not in cluster
        ret, _, _ = peer_detach(self.mnode, self.servers[1])
        self.assertEqual(ret, 0, ("Peer detach is failed"))
        g.log.info("Peer detach is successful")

        num_of_bricks = len(self.servers)
        bricks_list = form_bricks_list(self.mnode, self.volname, num_of_bricks,
                                       self.servers, self.all_servers_info)

        ret, _, _ = volume_create(self.mnode, self.volname, bricks_list)
        self.assertNotEqual(ret, 0, "Successfully created volume with brick "
                            "from which is not a part of node")
        g.log.info("Creating a volume with brick from node which is not part "
                   "of cluster is failed")

        # Peer probe the detached server
        ret, _, _ = peer_probe(self.mnode, self.servers[1])
        self.assertEqual(ret, 0, ("Peer probe is failed"))
        g.log.info("Peer probe is successful")

        # Create and start a volume
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume,
                           force=True)
        self.assertTrue(ret, "Failed to create the volume")
        g.log.info("Successfully created and started the volume")

        # Starting already started volume should fail
        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertNotEqual(ret, 0, "Expected: It should fail to start a "
                            "already started volume. Actual:Successfully"
                            " started a already started volume ")
        g.log.info("Starting a already started volume is Failed.")

        # Deleting a volume without stopping should fail
        ret = volume_delete(self.mnode, self.volname)
        self.assertFalse(ret, ("Expected: It should fail to delete a volume"
                               " without stopping. Actual: Successfully "
                               "deleted a volume without stopping it"))
        g.log.error("Failed to delete a volume without stopping it")

        # Stopping a volume should succeed
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("volume stop is failed"))
        g.log.info("Volume stop is success")

        # Stopping a already stopped volume should fail
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertNotEqual(ret, 0, "Expected: It should fail to stop a "
                            "already stopped volume . Actual: Successfully"
                            "stopped a already stopped volume")
        g.log.info("Volume stop is failed on already stopped volume")

        # Deleting a volume should succeed
        ret = volume_delete(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume delete is failed"))
        g.log.info("Volume delete is success")

        # Deleting a non existing volume should fail
        ret = volume_delete(self.mnode, self.volname)
        self.assertTrue(ret, "Expected: It should fail to delete a non "
                        "existing volume. Actual:Successfully deleted a "
                        "non existing volume")
        g.log.info("Volume delete is failed for non existing volume")

        # Volume info command should succeed
        ret = get_volume_info(self.mnode)
        self.assertIsNotNone(ret, "volume info command failed")
        g.log.info("Volume info command is success")
