#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from time import sleep
from random import randint
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (setup_volume, cleanup_volume,
                                            form_bricks_list_to_add_brick)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              rebalance_stop)
from glustolibs.gluster.peer_ops import (peer_detach,
                                         peer_probe,
                                         is_peer_connected)
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.gluster.glusterfile import (get_fattr, file_exists,
                                            get_fattr_list)


@runs_on([['replicated'], ['glusterfs']])
class TestChangeReservcelimit(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        GlusterBaseClass.setUpClass.im_func(cls)

        # Override Volumes setup
        cls.volume['voltype'] = {
            'type': 'replicated',
            'dist_count': 1,
            'replica_count': 3,
            'transport': 'tcp'}

    def tearDown(self):

        # Start rebalance for volume.
        g.log.info("Stopping rebalance on the volume")
        ret, _, _ = rebalance_stop(self.mnode, self.volname)
        if ret:
            raise ExecutionError("Failed to stop rebalance "
                                 "on the volume .")
        g.log.info("Successfully stopped rebalance on the volume %s",
                   self.volname)

        # Peer probe node which was detached
        ret, _, _ = peer_probe(self.mnode, self.servers[4])
        if ret:
            raise ExecutionError("Failed to probe %s" % self.servers[4])
        g.log.info("Peer probe successful %s", self.servers[4])

        # Wait till peers are in connected state
        count = 0
        while count < 60:
            ret = is_peer_connected(self.mnode, self.servers)
            if ret:
                break
            sleep(3)

        # Unmounting and cleaning volume
        ret, _, _ = umount_volume(mclient=self.mounts[0].client_system,
                                  mpoint=self.mounts[0].mountpoint)
        if ret:
            raise ExecutionError("Unable to unmount volume %s" % self.volname)
        g.log.info("Volume unmounted successfully  %s", self.volname)

        ret = cleanup_volume(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)
        g.log.info("Volume deleted successfully  %s", self.volname)
        GlusterBaseClass.tearDown.im_func(self)

    def test_detach_node_used_to_mount(self):
        # pylint: disable=too-many-statements
        """
        Test case:
        1.Create a 1X3 volume with only 3 nodes from the cluster.
        2.Mount volume on client node using the ip of the fourth node.
        3.Write IOs to the volume.
        4.Detach node N4 from cluster.
        5.Create a new directory on the mount point.
        6.Create a few files using the same command used in step 3.
        7.Add three more bricks to make the volume
          2x3 using add-brick command.
        8.Do a gluster volume rebalance on the volume.
        9.Create more files from the client on the mount point.
        10.Check for files on bricks from both replica sets.
        11.Create a new directory from the client on the mount point.
        12.Check for directory in both replica sets.
        """

        # Create and start a volume
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")
        g.log.info("Volume %s created successfully", self.volname)

        # Mounting the volume.
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.servers[4],
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume %s is not mounted") % self.volname)
        g.log.info("Volume mounted successfully using %s", self.servers[4])

        # Creating 100 files.
        command = ('for number in `seq 1 100`;do touch ' +
                   self.mounts[0].mountpoint + '/file$number; done')
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "File creation failed.")
        g.log.info("Files create on mount point.")

        # Detach N4 from the list.
        ret, _, _ = peer_detach(self.mnode, self.servers[4])
        self.assertEqual(ret, 0, "Failed to detach %s" % self.servers[4])
        g.log.info("Peer detach successful %s", self.servers[4])

        # Creating a dir.
        ret = mkdir(self.mounts[0].client_system,
                    self.mounts[0].mountpoint+"/dir1",
                    parents=True)
        self.assertTrue(ret, ("Failed to create directory dir1."))
        g.log.info("Directory dir1 created successfully.")

        # Creating 100 files.
        command = ('for number in `seq 101 200`;do touch ' +
                   self.mounts[0].mountpoint + '/file$number; done')
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "File creation failed.")
        g.log.info("Files create on mount point.")

        # Forming brick list
        brick_list = form_bricks_list_to_add_brick(self.mnode,
                                                   self.volname,
                                                   self.servers,
                                                   self.all_servers_info)

        # Adding bricks
        ret, _, _ = add_brick(self.mnode, self.volname, brick_list)
        self.assertEqual(ret, 0, "Failed to add brick to the volume %s"
                         % self.volname)
        g.log.info("Brick added successfully to the volume %s", self.volname)

        # Start rebalance for volume.
        g.log.info("Starting rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance "
                                  "on the volume %s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Creating 100 files.
        command = ('for number in `seq 201 300`;do touch ' +
                   self.mounts[0].mountpoint + '/file$number; done')
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "File creation failed.")
        g.log.info("Files create on mount point.")

        # Check for files on bricks.
        attempts = 10
        while attempts:
            number = str(randint(1, 300))
            for brick in brick_list:
                brick_server, brick_dir = brick.split(':')
                file_name = brick_dir+"/file" + number
                if file_exists(brick_server, file_name):
                    g.log.info("Check xattr"
                               " on host %s for file %s",
                               brick_server, file_name)
                    ret = get_fattr_list(brick_server, file_name)
                    self.assertTrue(ret, ("Failed to get xattr for %s"
                                          % file_name))
                    g.log.info("Got xattr for %s successfully",
                               file_name)
            attempts -= 1

        # Creating a dir.
        ret = mkdir(self.mounts[0].client_system,
                    self.mounts[0].mountpoint+"/dir2")
        if not ret:
            attempts = 5
            while attempts:
                ret = mkdir(self.mounts[0].client_system,
                            self.mounts[0].mountpoint+"/dir2")
                if ret:
                    break
                attempts -= 1
        self.assertTrue(ret, ("Failed to create directory dir2."))
        g.log.info("Directory dir2 created successfully.")

        # Check for directory in both replica sets.
        for brick in brick_list:
            brick_server, brick_dir = brick.split(':')
            folder_name = brick_dir+"/dir2"
            if file_exists(brick_server, folder_name):
                g.log.info("Check trusted.glusterfs.dht"
                           " on host %s for directory %s",
                           brick_server, folder_name)
                ret = get_fattr(brick_server, folder_name,
                                'trusted.glusterfs.dht')
                self.assertTrue(ret, ("Failed to get trusted.glusterfs.dht"
                                      " xattr for %s" % folder_name))
                g.log.info("Get trusted.glusterfs.dht xattr"
                           " for %s successfully", folder_name)
