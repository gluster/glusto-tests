#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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

""" Description:
        Test Cases in this module test NFS-Ganesha Sanity.
"""

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.nfs_ganesha_libs import (
     NfsGaneshaClusterSetupClass)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import (
     upload_scripts,
     git_clone_and_compile)
from glustolibs.gluster.nfs_ganesha_ops import (
     is_nfs_ganesha_cluster_in_healthy_state,
     set_acl)
from glustolibs.io.utils import validate_io_procs


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaSanity(NfsGaneshaClusterSetupClass):
    """
        Tests to verify NFS Ganesha Sanity.
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup nfs-ganesha if not exists.
        Upload IO scripts to clients
        """
        cls.get_super_method(cls, 'setUpClass')()

        # Setup nfs-ganesha if not exists.
        ret = cls.setup_nfs_ganesha()
        if not ret:
            raise ExecutionError("Failed to setup nfs-ganesha cluster")
        g.log.info("nfs-ganesha cluster is healthy")

        # Upload IO scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        # Cloning posix test suite
        cls.dir_name = "repo_dir"
        link = "https://github.com/ffilz/ntfs-3g-pjd-fstest.git"
        ret = git_clone_and_compile(cls.clients, link, cls.dir_name,
                                    compile_option=False)
        if not ret:
            raise ExecutionError("Failed to clone test repo")
        g.log.info("Successfully cloned test repo on client")
        cmd = "cd /root/repo_dir; sed 's/ext3/glusterfs/g' tests/conf; make"
        for client in cls.clients:
            ret, _, _ = g.run(client, cmd)
            if ret == 0:
                g.log.info("Test repo successfully compiled on"
                           "client %s" % client)
            else:
                raise ExecutionError("Failed to compile test repo")

    def setUp(self):
        """
        Setup and mount volume
        """
        g.log.info("Starting to setup and mount volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s"
                                 % self.volname)
        g.log.info("Successful in setup and mount volume %s", self.volname)

    def test_nfs_ganesha_HA_Basic_IO(self):
        """
        Tests to create an HA cluster and run basic IO
        """

        # Starting IO on the mounts
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       sys.version_info.major, self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Validate IO
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all IO")

        # Check nfs-ganesha status
        g.log.info("Checking for Cluster Status after IO run")
        ret = is_nfs_ganesha_cluster_in_healthy_state(self.mnode)
        self.assertTrue(ret, "Nfs Ganesha cluster is not healthy after "
                             "running IO")

        # Running kernel untar now,single loop for the sanity test
        g.log.info("Running kernel untars now")
        for mount_obj in self.mounts:
            cmd = ("cd %s ;mkdir $(hostname);cd $(hostname);"
                   "wget https://www.kernel.org/pub/linux/kernel/v2.6"
                   "/linux-2.6.1.tar.gz;"
                   "tar xvf linux-2.6.1.tar.gz" % mount_obj.mountpoint)
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertEqual(ret, 0, "Kernel untar failed!")
            g.log.info("Kernel untar successful on %s"
                       % mount_obj.client_system)

        # Check nfs-ganesha status
        g.log.info("Checking for Cluster Status after kernel untar")
        ret = is_nfs_ganesha_cluster_in_healthy_state(self.mnode)
        self.assertTrue(ret, "Nfs Ganesha cluster is not healthy after "
                             "kernel untar")

    def test_nfs_ganesha_posix_compliance(self):
        """
        Run Posix Compliance Suite with ACL enabled/disabled.
        """
        # Run test with ACL enabled

        # Enable ACL.
        ret = set_acl(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to enable ACL")
        g.log.info("ACL successfully enabled")
        # Run test
        for mount_object in self.mounts:
            g.log.info("Running test now")
            cmd = ("cd %s ; prove -r /root/%s"
                   % (mount_object.mountpoint, self.dir_name))
            ret, _, _ = g.run(mount_object.client_system, cmd)
            # Not asserting here,so as to continue with ACL disabled.
            if ret != 0:
                g.log.error("Posix Compliance Suite failed")
        g.log.info("Continuing with ACL disabled")

        # Check ganesha cluster status
        g.log.info("Checking ganesha cluster status")
        self.assertTrue(is_nfs_ganesha_cluster_in_healthy_state(self.mnode),
                        "Cluster is not healthy after test")
        g.log.info("Ganesha cluster is healthy after the test with ACL "
                   "enabled")

        # Now run test with ACL disabled

        # Disable ACL
        ret = set_acl(self.mnode, self.volname, acl=False,
                      do_refresh_config=True)
        self.assertEqual(ret, 0, "Failed to disable ACL")
        g.log.info("ACL successfully disabled")

        # Run test
        for mount_object in self.mounts:
            cmd = ("cd %s ; prove -r /root/%s"
                   % (mount_object.mountpoint, self.dir_name))
            # No assert , known failures with Posix Compliance and glusterfs
            ret, _, _ = g.run(mount_object.client_system, cmd)
            if ret != 0:
                g.log.error("Posix Compliance Suite failed. "
                            "Full Test Summary in Glusto Logs")

        # Check ganesha cluster status
        g.log.info("Checking ganesha cluster status")
        self.assertTrue(is_nfs_ganesha_cluster_in_healthy_state(self.mnode),
                        "Cluster is not healthy after test")
        g.log.info("Ganesha cluster is healthy after the test with ACL"
                   " disabled")

    def tearDown(self):
        """
        Unmount and cleanup volume
        """
        # Unmount volume
        ret = self.unmount_volume(self.mounts)
        if ret:
            g.log.info("Successfully unmounted the volume")
        else:
            g.log.error("Failed to unmount volume")

        # Cleanup volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Cleanup volume %s completed successfully", self.volname)

        # Cleanup test repo
        flag = 0
        for client in self.clients:
            ret, _, _ = g.run(client, "rm -rf /root/%s" % self.dir_name)
            if ret:
                g.log.error("Failed to cleanup test repo on "
                            "client %s" % client)
                flag = 1
            else:
                g.log.info("Test repo successfully cleaned on "
                           "client %s" % client)
        if flag:
            raise ExecutionError("Test repo deletion failed. "
                                 "Check log errors for more info")
        else:
            g.log.info("Test repo cleanup successfull on all clients")

    @classmethod
    def tearDownClass(cls):
        cls.get_super_method(cls, 'tearDownClass')(
            delete_nfs_ganesha_cluster=False)
