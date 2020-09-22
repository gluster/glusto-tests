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
from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.glusterdir import mkdir, get_dir_contents
from glustolibs.gluster.glusterfile import set_fattr, get_dht_linkto_xattr
from glustolibs.gluster.rebalance_ops import wait_for_remove_brick_to_complete
from glustolibs.gluster.volume_libs import form_bricks_list_to_remove_brick


@runs_on([['distributed'], ['glusterfs']])
class TestDeletDirWithSelfPointingLinktofiles(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Changing dist_count to 2
        self.volume['voltype']['dist_count'] = 2

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

    def test_delete_dir_with_self_pointing_linkto_files(self):
        """
        Test case:
        1. Create a pure distribute volume with 2 bricks, start and mount it.
        2. Create dir dir0/dir1/dir2 inside which create 1000 files and rename
           all the files.
        3. Start remove-brick operation on the volume.
        4. Check remove-brick status till status is completed.
        5. When remove-brick status is completed stop it.
        6. Go to brick used for remove brick and perform lookup on the files.
        8. Change the linkto xattr value for every file in brick used for
           remove brick to point to itself.
        9. Perfrom rm -rf * from mount point.
        """
        # Create dir /dir0/dir1/dir2
        self.dir_path = "{}/dir0/dir1/dir2/".format(self.mounts[0].mountpoint)
        ret = mkdir(self.first_client, self.dir_path, parents=True)
        self.assertTrue(ret, "Failed to create /dir0/dir1/dir2/ dir")

        # Create 1000 files inside /dir0/dir1/dir2
        ret, _, _ = g.run(self.first_client,
                          'cd %s;for i in {1..1000}; do echo "Test file" '
                          '> tfile-$i; done' % self.dir_path)
        self.assertFalse(ret,
                         "Failed to create 1000 files inside /dir0/dir1/dir2")

        # Rename 1000 files present inside /dir0/dir1/dir2
        ret, _, _ = g.run(self.first_client,
                          "cd %s;for i in {1..1000};do mv tfile-$i "
                          "ntfile-$i;done" % self.dir_path)
        self.assertFalse(ret,
                         "Failed to rename 1000 files inside /dir0/dir1/dir2")
        g.log.info("I/O successful on mount point.")

        # Start remove-brick operation on the volume
        brick = form_bricks_list_to_remove_brick(self.mnode, self.volname,
                                                 subvol_num=1)
        self.assertIsNotNone(brick, "Brick_list is empty")
        ret, _, _ = remove_brick(self.mnode, self.volname, brick, 'start')
        self.assertFalse(ret, "Failed to start remov-brick on volume")

        # Check remove-brick status till status is completed
        ret = wait_for_remove_brick_to_complete(self.mnode, self.volname,
                                                brick)
        self.assertTrue(ret, "Remove-brick didn't complete on volume")

        # When remove-brick status is completed stop it
        ret, _, _ = remove_brick(self.mnode, self.volname, brick, 'stop')
        self.assertFalse(ret, "Failed to start remov-brick on volume")
        g.log.info("Successfully started and stopped remove-brick")

        # Go to brick used for remove brick and perform lookup on the files
        node, path = brick[0].split(":")
        path = "{}/dir0/dir1/dir2/".format(path)
        ret, _, _ = g.run(node, 'ls {}*'.format(path))
        self.assertFalse(ret, "Failed to do lookup on %s" % brick[0])

        # Change the linkto xattr value for every file in brick used for
        # remove brick to point to itself
        ret = get_dir_contents(node, path)
        self.assertIsNotNone(ret,
                             "Unable to get files present in dir0/dir1/dir2")

        ret = get_dht_linkto_xattr(node, "{}{}".format(path, ret[0]))
        self.assertIsNotNone(ret, "Unable to fetch dht linkto xattr")

        # Change trusted.glusterfs.dht.linkto from dist-client-0 to
        # dist-client-1 or visa versa according to initial value
        dht_linkto_xattr = ret.split("-")
        if int(dht_linkto_xattr[2]):
            dht_linkto_xattr[2] = "0"
        else:
            dht_linkto_xattr[2] = "1"
        linkto_value = "-".join(dht_linkto_xattr)

        # Set xattr trusted.glusterfs.dht.linkto on all the linkto files
        ret = set_fattr(node, '{}*'.format(path),
                        'trusted.glusterfs.dht.linkto', linkto_value)
        self.assertTrue(ret,
                        "Failed to change linkto file to point to itself")

        # Perfrom rm -rf * from mount point
        ret, _, _ = g.run(self.first_client,
                          "rm -rf {}/*".format(self.mounts[0].mountpoint))
        self.assertFalse(ret, "Failed to run rm -rf * on mount point")
        g.log.info("rm -rf * successful on mount point")
