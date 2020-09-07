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
    wait_for_nfs_ganesha_volume_to_get_unexported,
    wait_for_nfs_ganesha_volume_to_get_exported)
from glustolibs.io.utils import get_mounts_stat
from glustolibs.gluster.nfs_ganesha_ops import (
    set_root_squash,
    unexport_nfs_ganesha_volume)
from glustolibs.gluster.volume_ops import (volume_stop, volume_start)
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

    def test_root_squash_enable(self):
        """
        Tests to verify Nfs Ganesha rootsquash functionality when volume
        is restarted
        Steps:
        1. Create some files and dirs inside mount point
        2. Set permission as 777 for mount point
        3. Enable root-squash on volume
        4. Create some more files and dirs
        5. Restart volume
        6. Try to edit file created in step 1
           It should not allow to edit the file
        7. Try to edit the file created in step 5
           It should allow to edit the file
        """
        # Start IO on mount point.
        cmd = ("for i in {1..10}; do touch %s/file$i; done"
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

        # Start IO on mount point.
        cmd = ("for i in {1..10}; do touch %s/Squashfile$i; done"
               % self.mounts[0].mountpoint)
        ret, _, err = g.run(self.mounts[0].client_system, cmd,
                            user=self.mounts[0].user)
        self.assertEqual(ret, 0, err)

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successful in getting stats of files/dirs "
                   "from mount point")

        # Stopping volume
        ret = volume_stop(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to stop volume %s" % self.volname))
        g.log.info("Successful in stopping volume %s" % self.volname)

        # Waiting for few seconds for volume unexport. Max wait time is
        # 120 seconds.
        ret = wait_for_nfs_ganesha_volume_to_get_unexported(self.mnode,
                                                            self.volname)
        self.assertTrue(ret, ("Failed to unexport volume %s after "
                              "stopping volume" % self.volname))
        g.log.info("Volume is unexported successfully")

        # Starting volume
        ret = volume_start(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to start volume %s" % self.volname))
        g.log.info("Successful in starting volume %s" % self.volname)

        # Waiting for few seconds for volume export. Max wait time is
        # 120 seconds.
        ret = wait_for_nfs_ganesha_volume_to_get_exported(self.mnode,
                                                          self.volname)
        self.assertTrue(ret, ("Failed to export volume %s after "
                              "starting volume" % self.volname))
        g.log.info("Volume is exported successfully")

        # Edit file created by root user
        for mount_obj in self.mounts:
            ret = append_string_to_file(mount_obj.client_system,
                                        "%s/file10" % mount_obj.mountpoint,
                                        'hello')
            self.assertFalse(ret, "Unexpected:nfsnobody user editing file "
                                  "created by root user should FAIL")
            g.log.info("Successful:nfsnobody user failed to edit file "
                       "created by root user")

        # Edit the file created by nfsnobody user
        for mount_obj in self.mounts:
            ret = append_string_to_file(mount_obj.client_system,
                                        "%s/Squashfile5"
                                        % mount_obj.mountpoint,
                                        'hello')
            self.assertTrue(ret, "Unexpected:nfsnobody user failed to edit "
                            "the file created by nfsnobody user")
            g.log.info("Successful:nfsnobody user successfully edited the "
                       "file created by nfsnobody user")

    def tearDown(self):

        # Disable root-squash
        ret = set_root_squash(self.mnode, self.volname, squash=False,
                              do_refresh_config=True)
        if not ret:
            raise ExecutionError("Failed to disable root-squash on nfs "
                                 "ganesha cluster")
        g.log.info("root-squash is disabled on volume")

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
