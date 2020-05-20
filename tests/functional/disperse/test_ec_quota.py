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
Test Description:
    Test quota on an EC volume
"""


from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_disable,
                                          quota_set_soft_timeout,
                                          quota_set_hard_timeout,
                                          quota_limit_usage)
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestEcQuota(GlusterBaseClass):

    def setUp(self):
        # Calling GlusterBaseClass setUp
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
        # Disable Quota
        ret, _, _ = quota_disable(self.mnode, self.volname)
        if ret:
            raise ExecutionError("Failed to disable quota on the volume %s")
        g.log.info("Successfully disabled quota on the volume %")

        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def set_quota_limit(self, limit):
        """
        Set Quota limit on the volume
        """
        # Path to set quota limit
        path = "/"

        # Set Quota limit
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path, limit=limit)
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

    def read_write_files(self, files, mount_dir, client):
        """
        Read and write files on the volume
        """
        # Write files
        for i in range(1, 5):
            writecmd = ("cd %s/dir%s; for i in `seq 1 %s` ;"
                        "do dd if=/dev/urandom of=file$i bs=1M "
                        "count=5;done" % (mount_dir, i, files))
            ret, _, _ = g.run(client, writecmd)
            self.assertEqual(ret, 0, "Unexpected: File creation failed ")
            g.log.info("Expected: File creation succeeded")

        # Reading files
        for i in range(1, 5):
            readcmd = ("cd %s/dir%s; for i in `seq 1 %s` ;"
                       "do dd if=file$i of=/dev/null bs=1M "
                       "count=5;done" % (mount_dir, i, files))
            ret, _, _ = g.run(client, readcmd)
            self.assertEqual(ret, 0, "Unexpected: Reading of file failed ")
            g.log.info("Expected: Able to read file successfully")

    def test_ec_quota(self):
        """
        - Enable quota on the volume
        - Set a limit of 4 GB on the root of the volume
        - Set Quota soft-timeout to 0 seconds
        - Set Quota hard-timeout to 0 second
        - Create 10 directories from the mount point
        - Create files of around 2.5 GB
        - Reading files
        - Decrease quota limit to  3 GB on the root of the volume
        - Writing files of around 500 MB
        - Reading files
        """
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        # Enable quota on the volume
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Set a limit of 4 GB on the root of the volume
        self.set_quota_limit(limit="4GB")

        # Set Quota soft-timeout to 0 seconds
        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, '0sec')
        self.assertEqual(ret, 0, "Failed to set soft timeout")
        g.log.info("Quota soft timeout set successful")

        # Set Quota hard-timeout to 0 second
        ret, _, _ = quota_set_hard_timeout(self.mnode, self.volname, '0sec')
        self.assertEqual(ret, 0, "Failed to set hard timeout")
        g.log.info("Quota hard timeout set successful")

        # Create 10 directories from the mount point
        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        for i in range(1, 11):
            ret = mkdir(client, "%s/dir%s" % (mount_dir, i))
            self.assertTrue(ret, ("Failed to create dir under %s-%s",
                                  client, mount_dir))
            g.log.info("Directory 'dir%s' created successfully", i)
        g.log.info("Successfully created directories on %s:%s",
                   client, mount_dir)

        # Create files of around 2.5 GB and reading
        self.read_write_files(files=100, mount_dir=mount_dir,
                              client=client)

        # Decrease quota limit to  3 GB on the root of the volume
        self.set_quota_limit(limit="3GB")

        # Writing files of around 500 MB and reading
        self.read_write_files(files=10, mount_dir=mount_dir,
                              client=client)
