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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks


@runs_on([['arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestDataDelete(GlusterBaseClass):
    """
    Description:
        Test data delete/rename on arbiter volume
    """
    def setUp(self):
        # Calling GlusterBaseClass
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """

        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_data_delete(self):
        """
        Test steps:
        - Get brick list
        - Create files and rename
        - Check if brick path contains old files
        - Delete files from mountpoint
        - Check .glusterfs/indices/xattrop is empty
        - Check if brickpath is empty
        """

        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        # Get the bricks from the volume
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # Create files and rename
        cmd = ('cd %s ;for i in `seq 1 100` ;do mkdir -pv directory$i;'
               'cd directory$i;dd if=/dev/urandom of=file$i bs=1M count=5;'
               'mv file$i renamed$i;done;' % (self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Fail: Not able to create files on "
                         "{}".format(self.mounts[0].mountpoint))
        g.log.info("Files created successfully and renamed")

        # Check if brickpath contains old files
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            cmd = ("ls -1 %s |grep file |wc -l " % brick_path)
            ret, out, _ = g.run(brick_node, cmd)
            self.assertEqual(0, int(out.strip()), "Brick path {} contains old "
                             "file in node {}".format(brick_path, brick_node))
        g.log.info("Brick path contains renamed files")

        # Delete files from mountpoint
        cmd = ('rm -rf -v %s/*' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to delete files")
        g.log.info("Files deleted successfully for %s", self.mounts[0])

        # Check .glusterfs/indices/xattrop is empty
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            cmd = ("ls -1 %s/.glusterfs/indices/xattrop/ | "
                   "grep -ve \"xattrop-\" | wc -l" % brick_path)
            ret, out, _ = g.run(brick_node, cmd)
            self.assertEqual(0, int(out.strip()), ".glusterfs/indices/"
                             "xattrop is not empty")
        g.log.info("No pending heals on bricks")

        # Check if brickpath is empty
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            cmd = ("ls -1 %s |wc -l " % brick_path)
            ret, out, _ = g.run(brick_node, cmd)
            self.assertEqual(0, int(out.strip()), "Brick path {} is not empty "
                             "in node {}".format(brick_path, brick_node))
        g.log.info("Brick path is empty on all nodes")
