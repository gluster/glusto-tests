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

"""
       Test Cases in this module tests the nfs ganesha version 3 and 4
       rootsquash functionality cases.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import runs_on, GlusterBaseClass
from glustolibs.gluster.nfs_ganesha_libs import (
                wait_for_nfs_ganesha_volume_to_get_unexported)
from glustolibs.io.utils import get_mounts_stat
from glustolibs.gluster.nfs_ganesha_ops import (
                set_root_squash,
                unexport_nfs_ganesha_volume)
from glustolibs.gluster.lib_utils import (append_string_to_file)
from glustolibs.gluster.glusterfile import set_file_permissions


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaRootSquash(GlusterBaseClass):

    def setUp(self):
        """
        Setup Volume
        """
        self.get_super_method(self, 'setUp')()

        # Setup and mount volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s"
                                 % self.volname)
        g.log.info("Successful in setup and mount volume %s", self.volname)

    def test_root_squash_multiple_client(self):
        """
        Tests to verify Nfs Ganesha rootsquash functionality with multi
        client
        Steps:
        1. Create some directories on mount point.
        2. Create some files inside those directories
        3. Set permission as 777 for mount point
        4. Enable root-squash on volume
        5. Edit file created by root user from client 2
           It should not allow to edit the file
        6. Create some directories on mount point.
        7. Create some files inside the directories
           Files and directories will be created by
           nfsnobody user
        8. Edit the file created in step 7
           It should allow to edit the file
        9. Disable root squash
        10. Edit the file created at step 7
            It should allow to edit the file
        """
        # Create Directories on Mount point
        cmd = ("for i in {1..10}; do mkdir %s/dir$i; done"
               % self.mounts[0].mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd,
                            user=self.mounts[0].user)
        self.assertEqual(ret, 0, err)

        # Create files inside directories on mount point.
        cmd = ("for i in {1..10}; do touch %s/dir$i/file$i; done"
               % self.mounts[0].mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd,
                            user=self.mounts[0].user)
        self.assertEqual(ret, 0, err)

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successful in getting stats of files/dirs "
                   "from mount point")

        # Set mount point permission to 777
        ret = set_file_permissions(self.mounts[0].client_system,
                                   self.mounts[0].mountpoint, 777)
        self.assertTrue(ret, "Failed to set permission for directory")
        g.log.info("Successfully set permissions for directory")

        # Enable root-squash on volume
        ret = set_root_squash(self.servers[0], self.volname)
        self.assertTrue(ret, "Failed to enable root-squash on volume")
        g.log.info("root-squash is enable on the volume")

        # Edit file created by root user from client 2
        ret = append_string_to_file(self.mounts[1].client_system,
                                    "%s/dir5/file5"
                                    % self.mounts[1].mountpoint, 'hello')
        self.assertFalse(ret, "Unexpected:nfsnobody user editing file "
                              "created by root user should FAIL")
        g.log.info("Successful:nfsnobody user failed to edit file "
                   "created by root user")

        # Create Directories on Mount point
        cmd = ("for i in {1..10}; do mkdir %s/SquashDir$i; done"
               % self.mounts[0].mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd,
                            user=self.mounts[0].user)
        self.assertEqual(ret, 0, err)

        # Create files inside directories on mount point
        cmd = ("for i in {1..10}; do touch %s/SquashDir$i/Squashfile$i;"
               "done" % self.mounts[0].mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd,
                            user=self.mounts[0].user)
        self.assertEqual(ret, 0, err)

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successful in getting stats of files/dirs "
                   "from mount point")

        # Edit the file created by nfsnobody user from client 2
        ret = append_string_to_file(self.mounts[1].client_system,
                                    "%s/SquashDir5/Squashfile5"
                                    % self.mounts[1].mountpoint,
                                    'hello')
        self.assertTrue(ret, "Unexpected:nfsnobody user failed to edit "
                             "the file created by nfsnobody user")
        g.log.info("Successful:nfsnobody user successfully edited the "
                   "file created by nfsnobody user")

        # Disable root-squash
        ret = set_root_squash(self.servers[0], self.volname, squash=False,
                              do_refresh_config=True)
        self.assertTrue(ret, "Failed to disable root-squash on volume")
        g.log.info("root-squash is disabled on the volume")

        # Edit the file created by nfsnobody user from root user
        ret = append_string_to_file(self.mounts[1].client_system,
                                    "%s/SquashDir10/Squashfile10"
                                    % self.mounts[1].mountpoint, 'hello')
        self.assertTrue(ret, "Unexpected:root user failed to edit "
                             "the file created by nfsnobody user")
        g.log.info("Successful:root user successfully edited the "
                   "file created by nfsnobody user")

    def tearDown(self):

        # Unexport volume
        unexport_nfs_ganesha_volume(self.mnode, self.volname)
        ret = wait_for_nfs_ganesha_volume_to_get_unexported(self.mnode,
                                                            self.volname)
        if not ret:
            raise ExecutionError("Failed:Volume %s is not unexported."
                                 % self.volname)
        g.log.info("Unexporting of volume is successful")

        # Unmount and cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if ret:
            g.log.info("Successful unmount and cleanup of volume")
        else:
            raise ExecutionError("Failed to unmount and cleanup volume")
