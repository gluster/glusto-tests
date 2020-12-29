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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.dht_test_utils import is_layout_complete
from glustolibs.gluster.glusterfile import (file_exists,
                                            occurences_of_pattern_in_file)
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import expand_volume, shrink_volume
from glustolibs.io.utils import (validate_io_procs, wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated'], ['glusterfs']])
class TestAddBrickFollowedByRemoveBrick(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

        cls.first_client = cls.mounts[0].client_system
        cls.mountpoint = cls.mounts[0].mountpoint
        cls.is_io_running = False

        # Upload IO scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        if not file_exists(cls.first_client, cls.script_upload_path):
            if not upload_scripts(cls.first_client, cls.script_upload_path):
                raise ExecutionError(
                    "Failed to upload IO scripts to client %s"
                    % cls.first_client)

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to setup and mount volume")

    def tearDown(self):

        if self.is_io_running:
            if not wait_for_io_to_complete(self.all_mounts_procs,
                                           [self.mounts[0]]):
                raise ExecutionError("IO failed on some of the clients")

        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _check_layout_of_bricks(self):
        """Check the layout of bricks"""
        ret = is_layout_complete(self.mnode, self.volname, "/")
        self.assertTrue(ret, ("Volume %s: Layout is not complete",
                              self.volname))
        g.log.info("Volume %s: Layout is complete", self.volname)

    def _add_brick_and_wait_for_rebalance_to_complete(self):
        """Add brick and wait for rebalance to complete"""

        # Add brick to volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)

        # Trigger rebalance and wait for it to complete
        ret, _, _ = rebalance_start(self.mnode, self.volname,
                                    force=True)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1200)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

        self._check_layout_of_bricks()

    def _remove_brick_from_volume(self):
        """Remove bricks from volume"""
        # Remove bricks from the volume
        ret = shrink_volume(self.mnode, self.volname, rebalance_timeout=2000)
        self.assertTrue(ret, "Failed to remove-brick from volume")
        g.log.info("Remove-brick rebalance successful")

    def test_add_brick_followed_by_remove_brick(self):
        """
        Test case:
        1. Create a volume, start it and mount it to a client.
        2. Start I/O on volume.
        3. Add brick and trigger rebalance, wait for rebalance to complete.
           (The volume which was 1x3 should now be 2x3)
        4. Add brick and trigger rebalance, wait for rebalance to complete.
           (The volume which was 2x3 should now be 3x3)
        5. Remove brick from volume such that it becomes a 2x3.
        6. Remove brick from volume such that it becomes a 1x3.
        7. Wait for I/O to complete and check for any input/output errors in
           both client and rebalance logs.
        """
        # Start I/O on mount point
        self.all_mounts_procs = []
        cmd = ("/usr/bin/env python {} create_deep_dirs_with_files "
               "--dirname-start-num {} --dir-depth 5 --dir-length 5 "
               "--max-num-of-dirs 5 --num-of-files 5 {}"
               .format(self.script_upload_path, 10, self.mountpoint))
        proc = g.run_async(self.first_client, cmd)
        self.all_mounts_procs.append(proc)
        self.is_io_running = True

        # Convert 1x3 to 2x3 and then convert 2x3 to 3x3
        for _ in range(0, 2):
            self._add_brick_and_wait_for_rebalance_to_complete()

        # Convert 3x3 to 2x3 and then convert 2x3 to 1x3
        for _ in range(0, 2):
            self._remove_brick_from_volume()

        # Validate I/O processes running on the nodes
        ret = validate_io_procs(self.all_mounts_procs, [self.mounts[0]])
        self.is_io_running = False
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO on all mounts: Complete")

        # Check for Input/output errors in rebalance logs
        particiapting_nodes = []
        for brick in get_all_bricks(self.mnode, self.volname):
            node, _ = brick.split(':')
            particiapting_nodes.append(node)

        for server in particiapting_nodes:
            ret = occurences_of_pattern_in_file(
                server, "Input/output error",
                "/var/log/glusterfs/{}-rebalance.log".format(self.volname))
            self.assertEqual(ret, 0,
                             "[Input/output error] present in rebalance log"
                             " file")

        # Check for Input/output errors in client logs
        ret = occurences_of_pattern_in_file(
            self.first_client, "Input/output error",
            "/var/log/glusterfs/mnt-{}_{}.log".format(self.volname,
                                                      self.mount_type))
        self.assertEqual(ret, 0,
                         "[Input/output error] present in client log file")
        g.log.info("Expanding and shrinking volume successful and no I/O "
                   "errors see in rebalance and client logs")
