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


from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.gluster_init import (
    stop_glusterd, start_glusterd,
    is_glusterd_running
)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.peer_ops import wait_for_peers_to_connect
from glustolibs.gluster.rebalance_ops import (
    get_rebalance_status,
    rebalance_start
)
from glustolibs.gluster.volume_libs import (
    cleanup_volume
)
from glustolibs.gluster.volume_ops import (
    volume_stop, volume_create, volume_start, get_volume_status
)
from glustolibs.io.utils import (
    list_all_files_and_dirs_mounts,
    wait_for_io_to_complete
)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed-replicated'], ['glusterfs']])
class XmlDumpGlusterVolumeStatus(GlusterBaseClass):
    """
    xml Dump of gluster volume status during rebalance, when one gluster
    node is down
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        # Start IO on mounts
        cls.all_mounts_procs = []
        for index, mount_obj in enumerate(cls.mounts, start=1):
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 1 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 10 "
                   "--num-of-files 60 %s" % (
                       cls.script_upload_path,
                       index + 10, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            cls.all_mounts_procs.append(proc)
        cls.io_validation_complete = False

        # Wait for IO to complete
        if not cls.io_validation_complete:
            g.log.info("Wait for IO to complete")
            ret = wait_for_io_to_complete(cls.all_mounts_procs, cls.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")

            ret = list_all_files_and_dirs_mounts(cls.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")

    def test_xml_dump_of_gluster_volume_status_during_rebalance(self):
        """
        1. Create a trusted storage pool by peer probing the node
        2.  Create a distributed-replicated volume
        3. Start the volume and fuse mount the volume and start IO
        4. Create another replicated volume and start it and stop it
        5. Start rebalance on the volume
        6. While rebalance in progress, stop glusterd on one of the nodes
            in the Trusted Storage pool.
        7. Get the status of the volumes with --xml dump
        """
        self.volname_2 = "test_volume_2"

        # create volume
        # Fetching all the parameters for volume_create
        list_of_three_servers = []
        server_info_for_three_nodes = {}
        for server in self.servers[:3]:
            list_of_three_servers.append(server)
            server_info_for_three_nodes[server] = self.all_servers_info[
                server]

        bricks_list = form_bricks_list(self.mnode, self.volname,
                                       3, list_of_three_servers,
                                       server_info_for_three_nodes)
        # Creating volumes using 3 servers
        ret, _, _ = volume_create(self.mnode, self.volname_2,
                                  bricks_list, force=True)
        self.assertFalse(ret, "Volume creation failed")
        g.log.info("Volume %s created successfully", self.volname_2)
        ret, _, _ = volume_start(self.mnode, self.volname_2)
        self.assertFalse(
            ret, "Failed to start volume {}".format(self.volname_2))
        ret, _, _ = volume_stop(self.mnode, self.volname_2)
        self.assertFalse(
            ret, "Failed to stop volume {}".format(self.volname_2))

        # Start Rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))

        # Get rebalance status
        status_info = get_rebalance_status(self.mnode, self.volname)
        status = status_info['aggregate']['statusStr']

        self.assertIn('in progress', status,
                      "Rebalance process is not running")
        g.log.info("Rebalance process is running")

        # Stop glusterd
        ret = stop_glusterd(self.servers[2])
        self.assertTrue(ret, "Failed to stop glusterd")

        ret, out, _ = g.run(
            self.mnode,
            "gluster v status  | grep -A 4 'Rebalance' | awk 'NR==3{print "
            "$3,$4}'")

        ret = get_volume_status(self.mnode, self.volname, options="tasks")
        rebalance_status = ret[self.volname]['task_status'][0]['statusStr']
        self.assertIn(rebalance_status, out.replace("\n", ""))

    def tearDown(self):
        ret = is_glusterd_running(self.servers)
        if ret:
            ret = start_glusterd(self.servers)
            if not ret:
                raise ExecutionError("Failed to start glusterd on %s"
                                     % self.servers)
        g.log.info("Glusterd started successfully on %s", self.servers)

        # Checking for peer status from every node
        for server in self.servers:
            ret = wait_for_peers_to_connect(server, self.servers)
            if not ret:
                raise ExecutionError("Servers are not in peer probed state")

        ret = cleanup_volume(self.mnode, self.volname_2)
        if not ret:
            raise ExecutionError(
                "Unable to delete volume % s" % self.volname_2)
        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
