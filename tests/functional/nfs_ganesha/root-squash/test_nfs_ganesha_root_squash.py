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
from glustolibs.io.utils import validate_io_procs, get_mounts_stat
from glustolibs.gluster.nfs_ganesha_ops import (
    set_root_squash,
    unexport_nfs_ganesha_volume)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaRootSquash(GlusterBaseClass):
    """
    Tests to verify Nfs Ganesha v3/v4 rootsquash stability
    Steps:
    1. Create some files and dirs inside mount point
    2. Check for owner and group
    3. Set permission as 777 for mount point
    4. Enable root-squash on volume
    5. Create some more files and dirs
    6. Check for owner and group for any file
    7. Edit file created by root user
    """
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

    def test_rootsquash_enable(self):
        # Start IO on mount point.
        self.all_mounts_procs = []
        cmd = ("for i in {1..10}; do touch %s/file$i; done"
               % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfull in getting stats of files/dirs "
                   "from mount point")

        # Check for owner and group of random file
        for mount_obj in self.mounts:
            cmd = ("ls -l %s/file5 | awk '{ print $3, $4 }' |sort"
                   % mount_obj.mountpoint)
            ret, out, err = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, err)
            self.assertIn("root root", out, "Owner and group is not ROOT")
            g.log.info("Owner and group of file is ROOT")

        # Set mount point permission to 777
        for mount_obj in self.mounts:
            cmd = ("chmod 777 %s" % mount_obj.mountpoint)
            ret, _, err = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, err)
            g.log.info("Mount point permission changed to 777")

        # Enable root-squash on volume
        ret = set_root_squash(self.servers[0], self.volname)
        self.assertTrue(ret, "Failed to enable root-squash on volume")
        g.log.info("root-squash is enable on the volume")

        # Start IO on mount point.
        self.all_mounts_procs = []
        cmd = ("for i in {1..10}; do touch %s/Squashfile$i; done"
               % self.mounts[0].mountpoint)
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Get stat of all the files/dirs created.
        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Stat failed on some of the clients")
        g.log.info("Successfull in getting stats of files/dirs "
                   "from mount point")

        # Check for owner and group of random file
        for mount_obj in self.mounts:
            cmd = ("ls -l %s/Squashfile5 | awk '{print $3, $4}' | sort"
                   % mount_obj.mountpoint)
            ret, out, err = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, err)
            self.assertIn("nfsnobody nfsnobody", out,
                          "Owner and group of file is NOT NFSNOBODY")
            g.log.info("Owner and group of file is NFSNOBODY")

        # Edit file created by root user
        for mount_obj in self.mounts:
            cmd = ("echo hello > %s/file10" % mount_obj.mountpoint)
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertEqual(ret, 1, "nfsnobody user editing file created by "
                             "root user should FAIL")
            g.log.info("nfsnobody user failed to edit file "
                       "created by root user")

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
            raise ExecutionError("Volume %s is not unexported." % self.volname)
        g.log.info("Unexporting of volume is successful")

        # Unmount and cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if ret:
            g.log.info("Successfull unmount and cleanup of volume")
        else:
            raise ExecutionError("Failed to unmount and cleanup volume")
