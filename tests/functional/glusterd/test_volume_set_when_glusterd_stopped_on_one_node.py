#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
      Volume set operation when glusterd is stopped on one node
"""

from random import choice
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import (
    set_volume_options, get_volume_info)
from glustolibs.gluster.brick_libs import get_online_bricks_list
from glustolibs.gluster.gluster_init import (
    start_glusterd, stop_glusterd, wait_for_glusterd_to_start)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestVolumeSetWhenGlusterdStoppedOnOneNode(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

        # Uploading file_dir script in all client direcotries
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        self.get_super_method(self, 'setUp')()
        # Creating Volume and mounting volume.
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)
        g.log.info("Volme created and mounted successfully : %s",
                   self.volname)

    def tearDown(self):
        # Check if a node is still down
        if self.glusterd_is_stopped:
            ret = start_glusterd(self.random_server)
            self.assertTrue(ret, "Failed to start glusterd on %s"
                            % self.random_server)
            g.log.info("Successfully started glusterd on node: %s",
                       self.random_server)

            # Waiting for glusterd to start completely
            ret = wait_for_glusterd_to_start(self.random_server)
            self.assertTrue(ret, "glusterd is not running on %s"
                            % self.random_server)
            g.log.info("glusterd is started and running on %s",
                       self.random_server)

        # Unmounting and cleaning volume.
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Unable to delete volume % s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_volume_set_when_glusterd_stopped_on_one_node(self):
        """
        Test Case:
        1) Setup and mount a volume on client.
        2) Stop glusterd on a random server.
        3) Start IO on mount points
        4) Set an option on the volume
        5) Start glusterd on the stopped node.
        6) Verify all the bricks are online after starting glusterd.
        7) Check if the volume info is synced across the cluster.
        """
        # Fetching the bricks list and storing it for later use
        list1 = get_online_bricks_list(self.mnode, self.volname)
        self.assertIsNotNone(list1, "Failed to get the list of online bricks "
                             "for volume: %s" % self.volname)

        # Fetching a random server from list.
        self.random_server = choice(self.servers[1:])

        # Stopping glusterd on one node.
        ret = stop_glusterd(self.random_server)
        self.assertTrue(ret, "Failed to stop glusterd on one node.")
        g.log.info("Successfully stopped glusterd on one node.")

        self.glusterd_is_stopped = True

        # Start IO on mount points.
        self.all_mounts_procs = []
        counter = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dir-depth 4 "
                   "--dir-length 6 "
                   "--dirname-start-num %d "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path,
                       counter, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            counter += 1

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        g.log.info("IO validation complete.")

        # set a option on volume, stat-prefetch on
        self.options = {"stat-prefetch": "on"}
        ret = set_volume_options(self.mnode, self.volname, self.options)
        self.assertTrue(ret, ("Failed to set option stat-prefetch to on"
                              "for the volume %s" % self.volname))
        g.log.info("Succeeded in setting stat-prefetch option to on"
                   "for the volume %s", self.volname)

        # start glusterd on the node where glusterd is stopped
        ret = start_glusterd(self.random_server)
        self.assertTrue(ret, "Failed to start glusterd on %s"
                        % self.random_server)
        g.log.info("Successfully started glusterd on node: %s",
                   self.random_server)

        # Waiting for glusterd to start completely
        ret = wait_for_glusterd_to_start(self.random_server)
        self.assertTrue(ret, "glusterd is not running on %s"
                        % self.random_server)
        g.log.info("glusterd is started and running on %s", self.random_server)

        self.glusterd_is_stopped = False

        # Confirm if all the bricks are online or not
        count = 0
        while count < 10:
            list2 = get_online_bricks_list(self.mnode, self.volname)
            if list1 == list2:
                break
            sleep(2)
            count += 1

        self.assertListEqual(list1, list2, "Unexpected: All the bricks in the"
                             "volume are not online")
        g.log.info("All the bricks in the volume are back online")

        # volume info should be synced across the cluster
        out1 = get_volume_info(self.mnode, self.volname)
        self.assertIsNotNone(out1, "Failed to get the volume info from %s"
                             % self.mnode)
        g.log.info("Getting volume info from %s is success", self.mnode)

        count = 0
        while count < 60:
            out2 = get_volume_info(self.random_server, self.volname)
            self.assertIsNotNone(out2, "Failed to get the volume info from %s"
                                 % self.random_server)
            if out1 == out2:
                break
            sleep(2)
            count += 1

        self.assertDictEqual(out1, out2, "Volume info is not synced in the"
                             "restarted node")
        g.log.info("Volume info is successfully synced across the cluster")
