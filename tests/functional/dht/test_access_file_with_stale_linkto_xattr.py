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
from glustolibs.gluster.lib_utils import add_user, del_user, set_passwd
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           reset_volume_option)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.dht_test_utils import find_new_hashed
from glustolibs.gluster.glusterfile import move_file, is_linkto_file
from glustolibs.gluster.glusterfile import set_file_permissions


@runs_on([['distributed', 'distributed-arbiter',
           'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs']])
class TestAccessFileWithStaleLinktoXattr(GlusterBaseClass):
    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup and Mount Volume")

        # Add a new user to the clients
        ret = add_user(self.clients[0], "test_user1")
        if ret is not True:
            raise ExecutionError("Failed to add user")

        # Set password for user "test_user1"
        ret = set_passwd(self.clients[0], "test_user1", "red123")
        if ret is not True:
            raise ExecutionError("Failed to set password")

        # Geneate ssh key on local host
        cmd = 'echo -e "n" | ssh-keygen -f ~/.ssh/id_rsa -q -N ""'
        ret, out, _ = g.run_local(cmd)
        if ret and "already exists" not in out:
            raise ExecutionError("Failed to generate ssh-key")
        g.log.info("Successfully generated ssh-key")

        # Perform ssh-copy-id
        cmd = ('sshpass -p "red123" ssh-copy-id -o StrictHostKeyChecking=no'
               ' test_user1@{}'.format(self.clients[0]))
        ret, _, _ = g.run_local(cmd)
        if ret:
            raise ExecutionError("Failed to perform ssh-copy-id")
        g.log.info("Successfully performed ssh-copy-id")

    def tearDown(self):
        # Delete the added user
        ret = del_user(self.clients[0], "test_user1")
        if ret is not True:
            raise ExecutionError("Failed to delete user")

        # Reset the volume options set inside the test
        for opt in ('performance.parallel-readdir',
                    'performance.readdir-ahead'):
            ret, _, _ = reset_volume_option(self.mnode, self.volname, opt)
            if ret:
                raise ExecutionError("Failed to reset the volume option %s"
                                     % opt)
            g.log.info("Successfully reset the volume options")

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_readdirp_with_rebalance(self):
        """
        Description: Checks if the files are accessible as non-root user if
                     the files have stale linkto xattr.
        Steps:
        1) Create a volume and start it.
        2) Mount the volume on client node using FUSE.
        3) Create a file.
        4) Enable performance.parallel-readdir and
           performance.readdir-ahead on the volume.
        5) Rename the file in order to create
           a linkto file.
        6) Force the linkto xattr values to become stale by changing the dht
           subvols in the graph
        7) Login as an non-root user and access the file.
        """
        # pylint: disable=protected-access

        # Set permissions on the mount-point
        m_point = self.mounts[0].mountpoint
        ret = set_file_permissions(self.clients[0], m_point, "-R 777")
        self.assertTrue(ret, "Failed to set file permissions")
        g.log.info("Successfully set file permissions on mount-point")

        # Creating a file on the mount-point
        cmd = 'dd if=/dev/urandom of={}/FILE-1 count=1 bs=16k'.format(
            m_point)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "File to create file")

        # Enable performance.parallel-readdir and
        # performance.readdir-ahead on the volume
        options = {"performance.parallel-readdir": "enable",
                   "performance.readdir-ahead": "enable"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, "Failed to set volume options")
        g.log.info("Successfully set volume options")

        # Finding a file name such that renaming source file to it will form a
        # linkto file
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        newhash = find_new_hashed(subvols, "/", "FILE-1")
        new_name = str(newhash.newname)
        new_host = str(newhash.hashedbrickobject._host)
        new_name_path = str(newhash.hashedbrickobject._fqpath)[:-1]

        # Move file such that it hashes to some other subvol and forms linkto
        # file
        ret = move_file(self.clients[0], "{}/FILE-1".format(m_point),
                        "{}/{}".format(m_point, new_name))
        self.assertTrue(ret, "Rename failed")
        g.log.info('Renamed file %s to %s',
                   "{}/FILE-1".format(m_point),
                   "{}/{}".format(m_point, new_name))

        # Check if "dst_file" is linkto file
        ret = is_linkto_file(new_host,
                             '{}{}'.format(new_name_path, new_name))
        self.assertTrue(ret, "File is not a linkto file")
        g.log.info("File is linkto file")

        # Force the linkto xattr values to become stale by changing the dht
        # subvols in the graph; for that:
        # disable performance.parallel-readdir and
        # performance.readdir-ahead on the volume
        options = {"performance.parallel-readdir": "disable",
                   "performance.readdir-ahead": "disable"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, "Failed to disable volume options")
        g.log.info("Successfully disabled volume options")

        # Access the file as non-root user
        cmd = "ls -lR {}".format(m_point)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd,
                          user="test_user1")
        self.assertEqual(ret, 0, "Lookup failed ")
        g.log.info("Lookup successful")
