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

"""
       Test Cases in this module tests the nfs ganesha version 3 and 4
       rootsquash functionality with volume unmount and remount.
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
from glustolibs.gluster.mount_ops import unmount_mounts
from glustolibs.gluster.lib_utils import append_string_to_file, is_rhel7
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

    def test_root_squash_mount_unmount(self):
        """
        Tests to verify Nfs Ganesha rootsquash functionality when volume
        is remounted
        Steps:
        1. Set permission as 777 for mount point
        2. Create some files and dirs inside mount point
        3. Enable root-squash on volume
        4. Create some more files and dirs inside mount point
        5. Unmount the volume
        6. Remount the volume
        7. Try to edit file created in step 2
           It should not allow to edit the file
        8. Try to edit the file created in step 4
           It should allow to edit the file
        9. Create some more files and directory inside mount point
            It should be created as nobody user
        10. Disable root-squash on volume
        11. Edit any of the file created in step 2.
            It should allow to edit the file
        """
        # Set mount point permission to 777
        ret = set_file_permissions(self.mounts[0].client_system,
                                   self.mounts[0].mountpoint, 777)
        self.assertTrue(ret, "Failed to set permission for directory")
        g.log.info("Successfully set permissions for directory")

        # Create Directories on Mount point
        cmd = ("for i in {1..20}; do mkdir %s/dir$i; done"
               % self.mounts[0].mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd,
                            user=self.mounts[0].user)
        self.assertEqual(ret, 0, err)

        # Create multiple files inside directories on mount point.
        for i in range(1, 21):
            cmd = ("for j in {1..20}; do touch %s/dir%s/file$j; done"
                   % (self.mounts[0].mountpoint, i))
            ret, _, err = g.run(self.mounts[0].client_system, cmd,
                                user=self.mounts[0].user)
            self.assertEqual(ret, 0, err)

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successful in getting stats of files/dirs "
                   "from mount point")

        # Enable root-squash on volume
        ret = set_root_squash(self.servers[0], self.volname)
        self.assertTrue(ret, "Failed to enable root-squash on volume")
        g.log.info("root-squash is enable on the volume")

        # Create some more Directories after enabling root-squash
        cmd = ("for i in {1..20}; do mkdir %s/squashed_dir$i; done"
               % self.mounts[0].mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd,
                            user=self.mounts[0].user)
        self.assertEqual(ret, 0, err)

        # Create some more files inside directories
        for i in range(1, 21):
            cmd = ("for j in {1..20}; do touch "
                   "%s/squashed_dir%s/squashed_file$j; done"
                   % (self.mounts[0].mountpoint, i))
            ret, _, err = g.run(self.mounts[0].client_system, cmd,
                                user=self.mounts[0].user)
            self.assertEqual(ret, 0, err)

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successful in getting stats of files/dirs "
                   "from mount point")

        # Unmount volume
        ret = unmount_mounts(self.mounts)
        self.assertTrue(ret, "Volume unmount failed for %s" % self.volname)

        # Remount volume
        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, "Volume mount failed for %s" % self.volname)

        # Edit file created by root user
        for mount_obj in self.mounts:
            ret = append_string_to_file(mount_obj.client_system,
                                        "%s/dir10/file10"
                                        % mount_obj.mountpoint,
                                        'hello')
            self.assertFalse(ret, "Unexpected:nobody user editing file "
                                  "created by root user should FAIL")
            g.log.info("Successful:nobody user failed to edit file "
                       "created by root user")

        # Edit the file created by nobody user
        for mount_obj in self.mounts:
            ret = append_string_to_file(mount_obj.client_system,
                                        "%s/squashed_dir10/squashed_file10"
                                        % mount_obj.mountpoint,
                                        'hello')
            self.assertTrue(ret, "Unexpected:nobody user failed to edit "
                            "the file created by nobody user")
            g.log.info("Successful:nobody user successfully edited the "
                       "file created by nobody user")

        # Create some more files on mount point post remount.
        cmd = ("for i in {1..20}; do touch %s/remount_file$i; done"
               % self.mounts[0].mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd,
                            user=self.mounts[0].user)
        self.assertEqual(ret, 0, err)

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successful in getting stats of files/dirs "
                   "from mount point")

        # Check for owner and group of all the files
        for mount_obj in self.mounts:
            for i in range(1, 21):
                cmd = ("ls -l %s/remount_file%i | awk '{ print $3, $4 }' |sort"
                       % (mount_obj.mountpoint, i))
                ret, out, err = g.run(mount_obj.client_system, cmd)
                self.assertFalse(ret, err)
                if is_rhel7:
                    self.assertIn("nobody nobody", out,
                                  "Owner and group is not nobody")
                else:
                    self.assertIn("nfsnobody nfsnobody", out,
                                  "Owner and group is not nobody")
                g.log.info("Owner and group of file is nobody")

        # Disable root-squash
        ret = set_root_squash(self.mnode, self.volname, squash=False,
                              do_refresh_config=True)
        self.assertTrue(ret, "Failed to disable root-squash on volume")
        g.log.info("root-squash is disable on the volume")

        # Edit file created by root user
        for mount_obj in self.mounts:
            ret = append_string_to_file(mount_obj.client_system,
                                        "%s/dir15/file15"
                                        % mount_obj.mountpoint,
                                        'hello')
            self.assertTrue(ret, "Unexpected:root user should be allowed to "
                                 "edit the file created by root user")
            g.log.info("Successful:root user successful in editing file "
                       "created by root user")

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

        if not ret:
            raise ExecutionError("Failed to unmount and cleanup volume")
        g.log.info("Successful unmount and cleanup of volume")
