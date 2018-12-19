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
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    get_rebalance_status, rebalance_start)
from glustolibs.gluster.volume_libs import (get_subvols,
                                            form_bricks_list_to_add_brick,
                                            log_volume_info_and_status)
from glustolibs.gluster.dht_test_utils import find_new_hashed
from glustolibs.gluster.glusterfile import move_file, is_linkto_file
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.brick_libs import get_all_bricks


@runs_on([['distributed', 'distributed-replicated',
           'distributed-dispersed', 'distributed-arbiter'],
          ['glusterfs']])
class DeleteFileInMigration(GlusterBaseClass):
    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup and Mount Volume")

        # Form brick list for add-brick operation
        self.add_brick_list = form_bricks_list_to_add_brick(
            self.mnode, self.volname, self.servers, self.all_servers_info,
            distribute_count=1, add_to_hot_tier=False)
        if not self.add_brick_list:
            raise ExecutionError("Volume %s: Failed to form bricks list for"
                                 " add-brick" % self.volname)
        g.log.info("Volume %s: Formed bricks list for add-brick operation",
                   (self.add_brick_list, self.volname))

    def tearDown(self):

        # Unmount Volume and Cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_delete_file_in_migration(self):
        """
        Verify that if a file is picked for migration and then deleted, the
        file should be removed successfully.
        * First create a big data file of 10GB.
        * Rename that file, such that after rename a linkto file is created
          (we are doing this to make sure that file is picked for migration.)
        * Add bricks to the volume and trigger rebalance using force option.
        * When the file has been picked for migration, delete that file from
          the mount point.
        * Check whether the file has been deleted or not on the mount-point
          as well as the back-end bricks.
        """

        # pylint: disable=too-many-statements
        # pylint: disable=too-many-locals
        # pylint: disable=protected-access

        mountpoint = self.mounts[0].mountpoint

        # Location of source file
        src_file = mountpoint + '/file1'

        # Finding a file name such that renaming source file to it will form a
        # linkto file
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        newhash = find_new_hashed(subvols, "/", "file1")
        new_name = str(newhash.newname)
        new_host = str(newhash.hashedbrickobject._host)
        new_name_path = str(newhash.hashedbrickobject._fqpath)[:-2]

        # Location of destination file to which source file will be renamed
        dst_file = '{}/{}'.format(mountpoint, new_name)
        # Create a 10GB file source file
        cmd = ("dd if=/dev/urandom of={} bs=1024K count=10000".format(
            src_file))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, ("File {} creation failed".format(src_file)))

        # Move file such that it hashes to some other subvol and forms linkto
        # file
        ret = move_file(self.clients[0], src_file, dst_file)
        self.assertTrue(ret, "Rename failed")
        g.log.info('Renamed file %s to %s', src_file, dst_file)

        # Check if "file_two" is linkto file
        ret = is_linkto_file(new_host,
                             '{}/{}'.format(new_name_path, new_name))
        self.assertTrue(ret, "File is not a linkto file")
        g.log.info("File is linkto file")

        # Expanding volume by adding bricks to the volume
        ret, _, _ = add_brick(self.mnode, self.volname,
                              self.add_brick_list, force=True)
        self.assertEqual(ret, 0, ("Volume {}: Add-brick failed".format
                                  (self.volname)))
        g.log.info("Volume %s: add-brick successful", self.volname)

        # Log Volume Info and Status after expanding the volume
        log_volume_info_and_status(self.mnode, self.volname)

        # Start Rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, ("Volume {}: Failed to start rebalance".format
                                  (self.volname)))
        g.log.info("Volume %s : Rebalance started ", self.volname)

        # Check if rebalance is running and delete the file
        status_info = get_rebalance_status(self.mnode, self.volname)
        status = status_info['aggregate']['statusStr']
        self.assertEqual(status, 'in progress', "Rebalance is not running")
        ret, _, _ = g.run(self.clients[0], (" rm -rf {}".format(dst_file)))
        self.assertEqual(ret, 0, ("Cannot delete file {}".format
                                  (dst_file)))
        g.log.info("File is deleted")

        # Check if the file is present on the mount point
        ret, _, _ = g.run(self.clients[0], ("ls -l {}".format(dst_file)))
        self.assertEqual(ret, 2, ("Failed to delete file {}".format
                                  (dst_file)))

        # Check if the file is present on the backend bricks
        bricks = get_all_bricks(self.mnode, self.volname)
        for brick in bricks:
            node, brick_path = brick.split(':')
            ret, _, _ = g.run(node, "ls -l {}/{}".format
                              (brick_path, new_name))
            self.assertEqual(ret, 2, "File is still present on"
                             " back-end brick: {}".format(
                                 brick_path))
            g.log.info("File is deleted from back-end brick: %s", brick_path)

        # Check if rebalance process is still running
        for server in self.servers:
            ret, _, _ = g.run(server, "pgrep rebalance")
            self.assertEqual(ret, 1, ("Rebalance process is still"
                                      " running on server {}".format
                                      (server)))
            g.log.info("Rebalance process is not running")
