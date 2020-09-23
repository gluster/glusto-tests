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
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterdir import get_dir_contents
from glustolibs.gluster.lib_utils import is_core_file_created


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed'], ['glusterfs']])
class TestInvalidMemoryReadAfterFreed(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

        # Assign a variable for the first_client
        self.first_client = self.mounts[0].client_system

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_invalid_memory_read_after_freed(self):
        """
        Test case:
        1. Create a volume and start it.
        2. Mount the volume using FUSE.
        3. Create multiple level of dirs and files inside every dir.
        4. Rename files such that linkto files are created.
        5. From the mount point do an rm -rf * and check if all files
           are delete or not from mount point as well as backend bricks.
        """
        # Fetch timestamp to check for core files
        ret, test_timestamp, _ = g.run(self.mnode, "date +%s")
        self.assertEqual(ret, 0, "date command failed")
        test_timestamp = test_timestamp.strip()

        # Create multiple level of dirs and files inside every dir
        cmd = ("cd %s; for i in {1..100}; do mkdir dir$i; cd dir$i; "
               "for i in {1..200}; do dd if=/dev/urandom of=file$i bs=1K"
               " count=1; done; done" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to create dirs and files")

        # Rename files such that linkto files are created
        cmd = ("cd %s; for i in {1..100}; do cd dir$i; for i in {1..200}; do "
               "mv file$i ntfile$i; done; done" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "Failed to rename files")
        g.log.info("Files created and renamed successfully")

        # From the mount point do an rm -rf * and check if all files
        # are delete or not from mount point as well as backend bricks.
        ret, _, _ = g.run(self.first_client,
                          "rm -rf {}/*".format(self.mounts[0].mountpoint))
        self.assertFalse(ret, "rn -rf * failed on mount point")

        ret = get_dir_contents(self.first_client,
                               "{}/".format(self.mounts[0].mountpoint))
        self.assertEqual(ret, [], "Unexpected: Files and directories still "
                         "seen from mount point")

        for brick in get_all_bricks(self.mnode, self.volname):
            node, brick_path = brick.split(":")
            ret = get_dir_contents(node, "{}/".format(brick_path))
            self.assertEqual(ret, [], "Unexpected: Files and dirs still seen "
                             "on brick %s on node %s" % (brick_path, node))
        g.log.info("rm -rf * on mount point successful")

        # Check for core file on servers and clients
        servers = self.servers + [self.first_client]
        ret = is_core_file_created(servers, test_timestamp)
        self.assertTrue(ret, "Core files found on servers used for test")
        g.log.info("No cores found on all participating servers")
