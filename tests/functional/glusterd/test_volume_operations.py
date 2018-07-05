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

import random
import re
import os

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           get_volume_list, volume_stop,
                                           volume_delete, get_volume_info)

from glustolibs.gluster.brick_libs import (are_bricks_online)
from glustolibs.gluster.volume_libs import cleanup_volume, setup_volume
from glustolibs.gluster.peer_ops import (peer_probe, peer_detach)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed'], ['glusterfs']])
class TestVolumeCreate(GlusterBaseClass):

    def setUp(self):
        GlusterBaseClass.setUp.im_func(self)
        # check whether peers are in connected state
        ret = self.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Peers are not in connected state")

    def tearDown(self):

        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume %s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        GlusterBaseClass.tearDown.im_func(self)

    def test_volume_start_force(self):

        # get the brick list and create a volume
        num_of_bricks = len(self.servers)
        bricks_list = form_bricks_list(self.mnode, self.volname, num_of_bricks,
                                       self.servers, self.all_servers_info)

        ret, _, _ = volume_create(self.mnode, self.volname, bricks_list)
        self.assertEqual(ret, 0, "Failed to create volume")

        # remove brick path in one node and try to start the volume with force
        # and without force
        index_of_node = random.randint(0, len(bricks_list) - 1)
        brick_node = bricks_list[index_of_node]
        node = brick_node.split(":")[0]
        brick_path = brick_node.split(":")[1]
        cmd = "rm -rf %s" % brick_path
        ret, _, _ = g.run(node, cmd)
        self.assertEqual(ret, 0, "Failed to delete the brick")
        g.log.info("Deleted the brick successfully")

        ret, _, _ = volume_start(self.mnode, self.volname)
        self.assertNotEqual(ret, 0, "Volume start succeeded")

        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Volume start with force failed")

        # volume start force should not bring the brick online
        ret = are_bricks_online(self.mnode, self.volname,
                                [bricks_list[index_of_node]])
        self.assertFalse(ret, "Volume start force brought the bricks online")
        g.log.info("Volume start force didn't bring the brick online")

    def test_volume_create_on_brick_root(self):
        # pylint: disable=too-many-locals

        # try to create a volume on brick root path without using force and
        # with using force
        self.volname = "second_volume"
        num_of_bricks = len(self.servers)
        bricks_list = form_bricks_list(self.mnode, self.volname, num_of_bricks,
                                       self.servers, self.all_servers_info)

        # save for using it later
        same_bricks_list = bricks_list[:]

        complete_brick = bricks_list[0].split(":")
        brick_root = os.path.dirname(complete_brick[1])
        root_brickpath = complete_brick[0] + ":" + brick_root
        bricks_list[0] = root_brickpath

        # creation of volume on root brick path should fail
        ret, _, _ = volume_create(self.mnode, self.volname, bricks_list)
        self.assertNotEqual(ret, 0, "Volume create on root brick path is "
                            "success")

        # volume create force should succeed
        ret, _, _ = volume_create(self.mnode, self.volname, bricks_list, True)
        self.assertEqual(ret, 0, "Volume create on root brick path with"
                         " force is failed")
        g.log.info("Volume create on root brick path with force is success")

        # create a sub directory under root partition and create a volume
        self.volname = "third_volume"

        sub_dir_path = "%s/sub_dir" % brick_root
        cmd = "mkdir %s" % sub_dir_path
        ret, _, _ = g.run(self.servers[0], cmd)
        sub_dir_brickpath_node = bricks_list[0].split(":")[0]
        sub_dir_brickpath = sub_dir_brickpath_node + ":" + sub_dir_path
        bricks_list[0] = sub_dir_brickpath

        # volume create with previously used bricks should fail
        ret, _, _ = volume_create(self.mnode, self.volname, bricks_list, True)
        self.assertNotEqual(ret, 0, "Volume create with previously used bricks"
                            " is success")

        # delete the volume created on root partition and clear all attributes
        # now, creation of volume should succeed.
        self.volname = "second_volume"
        ret, _, _ = g.run(self.mnode, "gluster vol delete %s  --mode=script"
                          % self.volname)
        for brick in bricks_list:
            server = brick.split(":")[0]
            brick_root = os.path.dirname(brick.split(":")[1])
            cmd1 = "rm -rf %s/*" % brick_root
            cmd2 = "getfattr -d -m . %s/" % brick_root
            cmd3 = "setfattr -x trusted.glusterfs.volume-id %s/" % brick_root
            cmd4 = "setfattr -x trusted.gfid %s/" % brick_root
            ret, _, _ = g.run(server, cmd1)
            self.assertEqual(ret, 0, "Failed to delete the files")
            g.log.info("Successfully deleted the files")
            ret, out, _ = g.run(server, cmd2)
            if re.search("trusted.glusterfs.volume-id", out):
                ret, _, _ = g.run(server, cmd3)
                self.assertEqual(ret, 0, "Failed to delete the xattrs")
                g.log.info("Deleted trusted.glusterfs.volume-id the xattrs")
            if re.search("trusted.gfid", out):
                ret, _, _ = g.run(server, cmd4)
                self.assertEqual(ret, 0, "Failed to delete gfid xattrs")
                g.log.info("Deleted trusterd.gfid xattrs")

        # creation of volume should succeed
        ret, _, _ = volume_create(self.mnode, self.volname, same_bricks_list)
        self.assertEqual(ret, 0, "Failed to create volume")

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
        self.assertTrue(
            volume_delete(self.mnode, "no_vol", xfail=True),
            "Expected: It should fail to delete a "
            "non existing volume. Actual:Successfully deleted "
            "a non existing volume"
        )

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
        self.assertTrue(
            volume_delete(self.mnode, self.volname, xfail=True),
            "Expected: It should fail to delete a volume"
            " without stopping. Actual: Successfully "
            "deleted a volume without stopping it"
        )
        g.log.info("Expected: volume delete should fail without "
                   "stopping volume: %s", self.volname)

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
        self.assertTrue(
            volume_delete(self.mnode, self.volname),
            "Volume delete is failed"
        )

        # Deleting an already deleted volume should fail
        self.assertTrue(
            volume_delete(self.mnode, self.volname, xfail=True),
            "Expected: It should fail to delete an "
            "already deleted volume. Actual:Successfully "
            "deleted an already deleted volume"
        )

        # Volume info command should succeed
        ret = get_volume_info(self.mnode)
        self.assertIsNotNone(ret, "volume info command failed")
        g.log.info("Volume info command is success")
