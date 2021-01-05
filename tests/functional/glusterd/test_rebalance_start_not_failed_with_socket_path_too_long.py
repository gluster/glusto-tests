#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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
"""
Description:
    Test Rebalance should start successfully if name of volume more than 108
    chars
"""

from glusto.core import Glusto as g
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.mount_ops import umount_volume, mount_volume
from glustolibs.gluster.rebalance_ops import (
    rebalance_start,
    wait_for_rebalance_to_complete
)
from glustolibs.gluster.volume_libs import (
    volume_start,
    cleanup_volume
)
from glustolibs.gluster.volume_ops import volume_create, get_volume_list
from glustolibs.io.utils import run_linux_untar


class TestLookupDir(GlusterBaseClass):
    def tearDown(self):
        cmd = ("sed -i '/transport.socket.bind-address/d'"
               " /etc/glusterfs/glusterd.vol")
        ret, _, _ = g.run(self.mnode, cmd)
        if ret:
            raise ExecutionError("Failed to remove entry from 'glusterd.vol'")
        for mount_dir in self.mount:
            ret = umount_volume(self.clients[0], mount_dir)
            if not ret:
                raise ExecutionError("Failed to cleanup Volume")

        vol_list = get_volume_list(self.mnode)
        if vol_list is not None:
            for volume in vol_list:
                ret = cleanup_volume(self.mnode, volume)
                if not ret:
                    raise ExecutionError("Failed to cleanup volume")
                g.log.info("Volume deleted successfully : %s", volume)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_rebalance_start_not_fail(self):
        """
        1. On Node N1, Add "transport.socket.bind-address N1" in the
            /etc/glusterfs/glusterd.vol
        2. Create a replicate (1X3) and disperse (4+2) volumes with
            name more than 108 chars
        3. Mount the both volumes using node 1 where you added the
            "transport.socket.bind-address" and start IO(like untar)
        4. Perform add-brick on replicate volume 3-bricks
        5. Start rebalance on replicated volume
        6. Perform add-brick for disperse volume 6 bricks
        7. Start rebalance of disperse volume
        """
        cmd = ("sed -i 's/end-volume/option "
               "transport.socket.bind-address {}\\n&/g' "
               "/etc/glusterfs/glusterd.vol".format(self.mnode))
        disperse = ("disperse_e4upxjmtre7dl4797wedbp7r3jr8equzvmcae9f55t6z1"
                    "ffhrlk40jtnrzgo4n48fjf6b138cttozw3c6of3ze71n9urnjkshoi")
        replicate = ("replicate_e4upxjmtre7dl4797wedbp7r3jr8equzvmcae9f55t6z1"
                     "ffhrlk40tnrzgo4n48fjf6b138cttozw3c6of3ze71n9urnjskahn")

        volnames = (disperse, replicate)
        for volume, vol_name in (
                ("disperse", disperse), ("replicate", replicate)):

            bricks_list = form_bricks_list(self.mnode, volume,
                                           6 if volume == "disperse" else 3,
                                           self.servers,
                                           self.all_servers_info)
            if volume == "replicate":
                ret, _, _ = volume_create(self.mnode, replicate,
                                          bricks_list,
                                          replica_count=3)

            else:
                ret, _, _ = volume_create(
                    self.mnode, disperse, bricks_list, force=True,
                    disperse_count=6, redundancy_count=2)

            self.assertFalse(
                ret,
                "Unexpected: Volume create '{}' failed ".format(vol_name))
            ret, _, _ = volume_start(self.mnode, vol_name)
            self.assertFalse(ret, "Failed to start volume")

        # Add entry in 'glusterd.vol'
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertFalse(
            ret, "Failed to add entry in 'glusterd.vol' file")

        self.list_of_io_processes = []

        # mount volume
        self.mount = ("/mnt/replicated_mount", "/mnt/disperse_mount")
        for mount_dir, volname in zip(self.mount, volnames):
            ret, _, _ = mount_volume(
                volname, "glusterfs", mount_dir, self.mnode,
                self.clients[0])
            self.assertFalse(
                ret, "Failed to mount the volume '{}'".format(mount_dir))

            # Run IO
            # Create a dir to start untar
            # for mount_point in self.mount:
            self.linux_untar_dir = "{}/{}".format(mount_dir, "linuxuntar")
            ret = mkdir(self.clients[0], self.linux_untar_dir)
            self.assertTrue(ret, "Failed to create dir linuxuntar for untar")

            # Start linux untar on dir linuxuntar
            ret = run_linux_untar(self.clients[:1], mount_dir,
                                  dirs=tuple(['linuxuntar']))
            self.list_of_io_processes += ret
            self.is_io_running = True

        # Add Brick to replicate Volume
        bricks_list = form_bricks_list(
            self.mnode, replicate, 3,
            self.servers, self.all_servers_info, "replicate")
        ret, _, _ = add_brick(
            self.mnode, replicate, bricks_list, force=True)
        self.assertFalse(ret, "Failed to add-brick '{}'".format(replicate))

        # Trigger Rebalance on the volume
        ret, _, _ = rebalance_start(self.mnode, replicate)
        self.assertFalse(
            ret, "Failed to start rebalance on the volume '{}'".format(
                replicate))

        # Add Brick to disperse Volume
        bricks_list = form_bricks_list(
            self.mnode, disperse, 6,
            self.servers, self.all_servers_info, "disperse")

        ret, _, _ = add_brick(
            self.mnode, disperse, bricks_list, force=True)
        self.assertFalse(ret, "Failed to add-brick '{}'".format(disperse))

        # Trigger Rebalance on the volume
        ret, _, _ = rebalance_start(self.mnode, disperse)
        self.assertFalse(
            ret,
            "Failed to start rebalance on the volume {}".format(disperse))

        # Check if Rebalance is completed on both the volume
        for volume in (replicate, disperse):
            ret = wait_for_rebalance_to_complete(
                self.mnode, volume, timeout=600)
            self.assertTrue(
                ret, "Rebalance is not Compleated on Volume '{}'".format(
                    volume))
