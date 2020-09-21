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
from glustolibs.gluster.glusterdir import mkdir, get_dir_contents
from glustolibs.gluster.glusterfile import set_fattr
from glustolibs.gluster.brick_libs import get_all_bricks


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed'], ['glusterfs']])
class TestNukeHappyPath(GlusterBaseClass):

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

    def test_nuke_happy_path(self):
        """
        Test case:
        1. Create a distributed volume, start and mount it
        2. Create 1000 dirs and 1000 files under a directory say 'dir1'
        3. Set xattr glusterfs.dht.nuke to "test" for dir1
        4. Validate dir-1 is not seen from mount point
        5. Validate if the entry is moved to '/brickpath/.glusterfs/landfill'
           and deleted eventually.
        """
        # Create 1000 dirs and 1000 files under a directory say 'dir1'
        self.dir_1_path = "{}/dir1/".format(self.mounts[0].mountpoint)
        ret = mkdir(self.first_client, self.dir_1_path)
        self.assertTrue(ret, "Failed to create dir1 on mount point")
        cmd = ("cd {};for i in `seq 1 1000`;do mkdir dir$i;touch file$i;done"
               .format(self.dir_1_path))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertFalse(ret, "I/O failed at dir1 on mount point")

        # Set xattr glusterfs.dht.nuke to "test" for dir1
        ret = set_fattr(self.first_client, self.dir_1_path,
                        'glusterfs.dht.nuke', 'test')
        self.assertTrue(ret, "Failed to set xattr glusterfs.dht.nuke")

        # Validate dir-1 is not seen from mount point
        ret = get_dir_contents(self.first_client, self.mounts[0].mountpoint)
        self.assertEqual([], ret,
                         "UNEXPECTED: Mount point has files ideally it should "
                         "be empty.")

        # Validate if the entry is moved to '/brickpath/.glusterfs/landfill'
        # and deleted eventually
        for brick_path in get_all_bricks(self.mnode, self.volname):
            node, path = brick_path.split(":")
            path = "{}/.glusterfs/landfill/*/".format(path)
            ret = get_dir_contents(node, path)
            # In case if landfile is already cleaned before checking
            # stop execution of the loop.
            if ret is None:
                g.log.info("Bricks have been already cleaned up.")
                break
            self.assertIsNotNone(ret,
                                 "Files not present in /.glusterfs/landfill"
                                 " dir")
        g.log.info("Successully nuked dir1.")
