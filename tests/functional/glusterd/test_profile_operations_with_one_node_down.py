#  Copyright (C) 2019-2020  Red Hat, Inc. <http://www.redhat.com>
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

"""
    Test Description:
    Tests to check basic profile operations with one node down.
"""

from random import randint
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.profile_ops import (profile_start, profile_info,
                                            profile_stop)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.brick_libs import get_online_bricks_list
from glustolibs.gluster.gluster_init import (stop_glusterd, start_glusterd,
                                             wait_for_glusterd_to_start)
from glustolibs.gluster.peer_ops import wait_for_peers_to_connect


@runs_on([['distributed-replicated', 'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestProfileOpeartionsWithOneNodeDown(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

        # Uploading file_dir script in all client direcotries
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
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

        # Starting glusterd on node where stopped.
        ret = start_glusterd(self.servers[self.random_server])
        if ret:
            ExecutionError("Failed to start glusterd.")
        g.log.info("Successfully started glusterd.")

        # Checking if peer is connected
        for server in self.servers:
            ret = wait_for_peers_to_connect(self.mnode, server)
            if not ret:
                ExecutionError("Peers are not in connected state.")
            g.log.info("Peers are in connected state.")

        # Unmounting and cleaning volume.
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Unable to delete volume % s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_profile_operations_with_one_node_down(self):

        # pylint: disable=too-many-statements
        """
        Test Case:
        1) Create a volume and start it.
        2) Mount volume on client and start IO.
        3) Start profile info on the volume.
        4) Stop glusterd on one node.
        5) Run profile info with different parameters
           and see if all bricks are present or not.
        6) Stop profile on the volume.
        """

        # Start IO on mount points.
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        counter = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dir-depth 4 "
                   "--dirname-start-num %d "
                   "--dir-length 6 "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 5 %s" % (
                       sys.version_info.major, self.script_upload_path,
                       counter, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            counter += 1

        # Start profile on volume.
        ret, _, _ = profile_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start profile on volume: %s"
                         % self.volname)
        g.log.info("Successfully started profile on volume: %s",
                   self.volname)

        # Fetching a random server from list.
        self.random_server = randint(1, len(self.servers)-1)

        # Stopping glusterd on one node.
        ret = stop_glusterd(self.servers[self.random_server])
        self.assertTrue(ret, "Failed to stop glusterd on one node.")
        g.log.info("Successfully stopped glusterd on one node.")
        ret = wait_for_glusterd_to_start(self.servers[self.random_server])
        self.assertFalse(ret, "glusterd is still running on %s"
                         % self.servers[self.random_server])
        g.log.info("Glusterd stop on the nodes : %s "
                   "succeeded", self.servers[self.random_server])

        # Getting and checking output of profile info.
        ret, out, _ = profile_info(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to run profile info on volume: %s"
                         % self.volname)
        g.log.info("Successfully executed profile info on volume: %s",
                   self.volname)

        # Checking if all bricks are present in profile info.
        brick_list = get_online_bricks_list(self.mnode, self.volname)
        for brick in brick_list:
            self.assertTrue(brick in out,
                            "Brick %s not a part of profile info output."
                            % brick)
            g.log.info("Brick %s showing in profile info output.",
                       brick)

        # Running profile info with different profile options.
        profile_options = ['peek', 'incremental', 'clear', 'incremental peek',
                           'cumulative']
        for option in profile_options:

            # Getting and checking output of profile info.
            ret, out, _ = profile_info(self.mnode, self.volname,
                                       options=option)
            self.assertEqual(ret, 0,
                             "Failed to run profile info %s on volume: %s"
                             % (option, self.volname))
            g.log.info("Successfully executed profile info %s on volume: %s",
                       option, self.volname)

            # Checking if all bricks are present in profile info peek.
            for brick in brick_list:
                self.assertTrue(brick in out,
                                "Brick %s not a part of profile"
                                " info %s output."
                                % (brick, option))
                g.log.info("Brick %s showing in profile info %s output.",
                           brick, option)

        # Starting glusterd on node where stopped.
        ret = start_glusterd(self.servers[self.random_server])
        self.assertTrue(ret, "Failed to start glusterd.")
        g.log.info("Successfully started glusterd.")

        # Checking if peer is connected
        ret = wait_for_peers_to_connect(self.mnode, self.servers)
        self.assertTrue(ret, "Peers are not in connected state.")
        g.log.info("Peers are in connected state.")

        # Stop profile on volume.
        ret, _, _ = profile_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to stop profile on volume: %s"
                         % self.volname)
        g.log.info("Successfully stopped profile on volume: %s", self.volname)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        g.log.info("IO validation complete.")
