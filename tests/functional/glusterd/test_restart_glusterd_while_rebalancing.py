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

""" Description:
      Test restart glusterd while rebalance is in progress
"""

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import form_bricks_list_to_add_brick
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              get_rebalance_status)
from glustolibs.gluster.gluster_init import (restart_glusterd,
                                             is_glusterd_running)
from glustolibs.io.utils import validate_io_procs
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestRestartGlusterdWhileRebalance(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.counter = 1
        GlusterBaseClass.setUpClass.im_func(cls)

        # Uploading file_dir script in all client direcotries
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        setUp method for every test
        """
        # Creating Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)
        g.log.info("Volme created and mounted successfully : %s",
                   self.volname)

        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

    def tearDown(self):
        """
        tearDown for every test
        """

        # checking for peer status from every node
        count = 0
        while count < 80:
            ret = self.validate_peers_are_connected()
            if ret:
                break
            sleep(2)
            count += 1

        if not ret:
            raise ExecutionError("Servers are not in peer probed state")

        # unmounting the volume and Cleaning up the volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup the Volume %s"
                                 % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

    def test_glusterd_rebalance(self):

        '''
        -> Create Volume
        -> Fuse mount the volume
        -> Perform I/O on fuse mount
        -> Add bricks to the volume
        -> Perform rebalance on the volume
        -> While rebalance is in progress,
        -> restart glusterd on all the nodes in the cluster
        '''

        # run IOs
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 4 "
                   "--dir-length 6 "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 25 %s" % (self.script_upload_path,
                                             self.counter,
                                             mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Forming brick list
        brick_list = form_bricks_list_to_add_brick(
            self.mnode, self.volname, self.servers, self.all_servers_info)

        # Adding Bricks
        ret, _, _ = add_brick(self.mnode, self.volname, brick_list)
        self.assertEqual(ret, 0, "Failed to add brick to the volume %s"
                         % self.volname)
        g.log.info("Brick added successfully to the volume %s", self.volname)

        # Performing rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, 'Failed to start rebalance on volume %s'
                         % self.volname)
        g.log.info("Rebalance started successfully on volume %s",
                   self.volname)

        # Checking Rebalance is in progress or not
        rebalance_status = get_rebalance_status(self.mnode, self.volname)
        if rebalance_status['aggregate']['statusStr'] != 'in progress':
            raise ExecutionError("Rebalance is not in 'in progress' state, "
                                 "either rebalance is in compeleted state or"
                                 " failed to get rebalance status")

        # Restart glusterd
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, "Failed to restart glusterd on servers")
        g.log.info("Glusterd restarted successfully on %s", self.servers)

        # Checking glusterd status
        count = 0
        while count < 60:
            ret = is_glusterd_running(self.servers)
            if not ret:
                break
            sleep(2)
            count += 1
        self.assertEqual(ret, 0, "Glusterd is not running on some of the "
                                 "servers")
        g.log.info("Glusterd is running on all servers %s", self.servers)
