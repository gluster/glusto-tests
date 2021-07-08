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
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.


from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.lib_utils import add_user, del_user, set_passwd
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)

@runs_on([['distributed', 'distributed-replicated'],
          ['glusterfs']])
class TestRebalanceWithNonRootUser(GlusterBaseClass):

    def setUp(self):
        """
        Setup and mount volume
        """
        # Setup Volume
        if not self.setup_volume_and_mount_volume(mounts=[self.mounts[0]]):
            raise ExecutionError("Failed to Setup and Mount Volume")

        self.get_super_method(self, 'setUp')()

        self.m_point = self.mounts[0].mountpoint
        self.first_client = self.mounts[0].client_system

        # Add a new user to the clients
        ret = add_user(self.first_client, "test_user1")
        if ret is not True:
            raise ExecutionError("Failed to add user")

        # Set password for user "test_user1"
        ret = set_passwd(self.first_client, "test_user1", "red123")
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
               ' test_user1@{}'.format(self.first_client))
        ret, _, _ = g.run_local(cmd)
        if ret:
            raise ExecutionError("Failed to perform ssh-copy-id")
        g.log.info("Successfully performed ssh-copy-id")

        # Create a file in a non-gluster mount dir
        cmd = "dd if=/dev/urandom of=/tmp/temp_file bs=500M count=1"
        ret, _, _ = g.run(self.first_client, cmd, user="test_user1")
        if ret:
            raise AssertionError("Failed to create a temp file")

    def _get_file_permissions(self, host, file):
        """ Returns dir permissions"""
        cmd = 'stat -c "%U %a" {}'.format(file)
        ret, out, _ = g.run(host, cmd)
        self.assertEqual(ret, 0, "Failed to get permission on {}".format(host))
        return out.split(" ")

    def _get_checksum(self, host, file_path):
        """ Returns the checksum of the given file"""
        ret, out, err = g.run(host, "cksum %s" % file_path)
        self.assertEqual(ret, 0, "Failed to calculate checksum")
        return out

    def _start_file_copy_and_return_file_info(self):
        """ Performs copy of file byte by byte and returns the file stats"""
        cmd = ("cd {};dd if=/tmp/temp_file of=zzFile bs=1"
               .format(self.m_point))
        file_path = "/tmp/temp_file"
        owner, permission = self._get_file_permissions(self.first_client,
                                                      file_path)

        checksum = self._get_checksum(self.first_client, file_path)
        self.proc = g.run_async(self.first_client, cmd, user="test_user1")
        g.log.info("PROC")
        g.log.info(self.proc)
        return [owner, permission, checksum]

    def _expand_volume_and_verify_rebalance(self):
        """ Expands the volume, trigger rebalance and verify file is copied"""

        # Expand the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to expand the volume")

        # Trigger rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1200)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

    def _wait_for_file_copy_and_return_file_info(self):
        """ Wait for file copy to complete and return file info"""
        ret, _, _ = self.proc.async_communicate()
        self.assertEqual(ret, 0, "Failed to copy file")

        file_path = "{}/zzFile".format(self.m_point)
        owner, permission = self._get_file_permissions(self.first_client,
                                                      file_path)
        checksum = self._get_checksum(self.first_client, file_path)
        return [owner, permission, checksum]

    def test_verify_rebalance_open_file_as_non_root_user(self):
        """
        Steps:
        1. Create a distributed or distributed-replicate volume
        2. As a non-root user copy a file byte by byte from non-gluster mount
           to gluster mount, collect the permissions and owners for the files
        3. While above copy is in progress add-brick and initiate rebalance
           (make sure that above file with openfd gets migrated , can be tried
            out with renaming the file)
        4. After migration of this file is over wait till copy is done
        5. Calculate the checksum of original and copied file , also collect
           permissions and owner of the file

        => Checksum should match, permissions and ownerships should match
        """

        # Copy file
        file_info = self._start_file_copy_and_return_file_info()
        g.log.info("BEFORE")
        g.log.info(file_info)

        # Expand volume and initiate rebalance
        self._expand_volume_and_verify_rebalance()

        new_file_info = self._wait_for_file_copy_and_return_file_info()
        g.log.info("AFTER")
        g.log.info(new_file_info)

    def tearDown(self):
        # Unmount and cleanup original volume
        if not self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]]):
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Create a user
        if not del_user(self.first_client, "test_user"):
            g.log.error("Failed to delete newly created user")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
