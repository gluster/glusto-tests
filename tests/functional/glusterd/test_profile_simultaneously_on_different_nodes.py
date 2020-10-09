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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
    Test Description:
    Tests to test profile simultaneously on different nodes.
"""

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.profile_ops import profile_start
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.lib_utils import is_core_file_created
from glustolibs.gluster.gluster_init import is_glusterd_running
from glustolibs.gluster.volume_ops import get_volume_list
from glustolibs.gluster.volume_libs import (cleanup_volume, setup_volume)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestProfileSimultaneouslyOnDifferentNodes(GlusterBaseClass):

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
        g.log.info("Volume created and mounted successfully : %s",
                   self.volname)

    def tearDown(self):
        # Unmounting and cleaning volume.
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Unable to delete volume % s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if not vol_list:
            raise ExecutionError("Failed to get the volume list")
        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume % s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        self.get_super_method(self, 'tearDown')()

    def test_profile_simultaneously_on_different_nodes(self):
        """
        Test Case:
        1) Create a volume and start it.
        2) Mount volume on client and start IO.
        3) Start profile on the volume.
        4) Create another volume.
        5) Start profile on the volume.
        6) Run volume status in a loop in one of the node.
        7) Run profile info for the new volume on one of the other node
        8) Run profile info for the new volume in loop for 100 times on
           the other node
        """
        # Timestamp of current test case of start time
        ret, test_timestamp, _ = g.run_local('date +%s')
        test_timestamp = test_timestamp.strip()

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

        # Start profile on volume.
        ret, _, _ = profile_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start profile on volume: %s"
                         % self.volname)
        g.log.info("Successfully started profile on volume: %s",
                   self.volname)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        g.log.info("IO validation complete.")

        # Create and start a volume
        self.volume['name'] = "volume_2"
        self.volname = "volume_2"
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")
        g.log.info("Successfully created and started volume_2")

        # Start profile on volume.
        ret, _, _ = profile_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start profile on volume: %s"
                         % self.volname)
        g.log.info("Successfully started profile on volume: %s",
                   self.volname)

        # Run volume status on one of the node in loop
        cmd = "for i in `seq 1 100`;do gluster v status;done"
        proc1 = g.run_async(self.servers[1], cmd)

        # Check profile on one of the other node
        cmd = "gluster v profile %s info" % self.volname
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to run profile info on volume: %s"
                         " on node %s" % (self.volname, self.mnode))
        g.log.info("Successfully run pofile info on volume: %s on node %s",
                   self.volname, self.mnode)

        # Run volume profile info on one of the other node in loop
        cmd = """for i in `seq 1 100`;do gluster v profile %s info;
              done""" % self.volname
        proc2 = g.run_async(self.servers[3], cmd)

        ret1, _, _ = proc1.async_communicate()
        ret2, _, _ = proc2.async_communicate()

        self.assertEqual(ret1, 0, "Failed to run volume status in a loop"
                         " on node %s" % self.servers[1])
        g.log.info("Successfully running volume status in a loop on node"
                   " %s", self.servers[1])

        self.assertEqual(ret2, 0, "Failed to run profile info in a loop"
                         " on node %s" % self.servers[3])
        g.log.info("Successfully running volume status in a loop on node"
                   " %s", self.servers[3])

        # Chekcing for core files.
        ret = is_core_file_created(self.servers, test_timestamp)
        self.assertTrue(ret, "glusterd service should not crash")
        g.log.info("No core file found, glusterd service running "
                   "successfully")

        # Checking whether glusterd is running or not
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, "Glusterd has crashed on nodes.")
        g.log.info("No glusterd crashes observed.")
