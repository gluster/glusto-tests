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

from glusto.core import Glusto as g
from glustolibs.io.utils import list_all_files_and_dirs_mounts
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_set_soft_timeout,
                                          quota_set_hard_timeout,
                                          quota_limit_usage)
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.glusterfile import file_exists
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['distributed-replicated', 'replicated', 'distributed'],
          ['glusterfs', 'nfs']])
class QuotaFileLargerThanLimit(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        """
        Setup volume, mount volume and initialize necessary variables
        which is used in tests
        """
        # calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup and Mount Volume %s",
                   cls.volname)
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume %s"
                                 % cls.volname)
        g.log.info("Successful in Setup and Mount Volume %s", cls.volname)

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume and umount volume from client
        """
        # stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # calling GlusterBaseClass tearDownClass
        GlusterBaseClass.tearDownClass.im_func(cls)

    def test_quota_file_larger_than_limit(self):
        # pylint: disable=too-many-statements
        """
        Verifying directory Quota functionality with respect to the
        limit-usage option.

        If a limit is set and a file of size larger than limit is created
        then the file creation will stop when it will reach the limit.

        Quota list will show limit-set and size as same.

        * Enable Quota
        * Create a directory from mount point
        * Set a limit of 10 MB on the directory
        * Set Quota soft-timeout and hard-timeout to 0 seconds
        * Create a file of size larger than the Quota limit
          eg. 20 MB file
        * Perform Quota list operation to check if all the fields are
          appropriate such as hard_limit, available_space, sl_exceeded,
          hl_execeeded, etc.
        """
        # Enable Quota
        g.log.info("Enabling Quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable Quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled Quota on the volume %s", self.volname)

        # Path to set the Quota limit
        path = '/foo'

        # Create a directory 'foo' from the mount point
        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        g.log.info("Creating dir named 'foo' from client %s", client)
        ret = mkdir(client, "%s/foo" % mount_dir)
        self.assertTrue(ret, "Failed to create dir under %s-%s"
                        % (client, mount_dir))
        g.log.info("Directory 'foo' created successfully")

        # Set Quota limit of 10 MB on the directory 'foo' of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="10MB")
        self.assertEqual(ret, 0, ("Failed to set Quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

        # Set Quota soft-timeout to 0 seconds
        g.log.info("Set Quota soft timeout:")
        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, '0sec')
        self.assertEqual(ret, 0, ("Failed to set soft timeout"))
        g.log.info("Quota soft timeout set successful")

        # Set Quota hard-timeout to 0 second
        g.log.info("Set Quota hard timeout:")
        ret, _, _ = quota_set_hard_timeout(self.mnode, self.volname, '0sec')
        self.assertEqual(ret, 0, ("Failed to set hard timeout"))
        g.log.info("Quota hard timeout set successful")

        # Validate if the Quota limit set is appropriate
        g.log.info("Validate if the Quota limit set is correct for the "
                   "directory %s of the volume %s", path, self.volname)
        ret = quota_validate(self.mnode, self.volname, path=path,
                             hard_limit=10485760)
        self.assertTrue(ret, ("Quota Limit of 10 MB was not set properly on "
                              "the directory %s of the volume %s",
                              path, self.volname))
        g.log.info("Successfully Validated Quota Limit of 10 MB is set on the"
                   " directory %s of the volume %s", path, self.volname)

        # Create a single file of size 20 MB
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s/foo ; "
               "dd if=/dev/zero of=20MBfile "
               "bs=1M "
               "count=20"
               % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 1, "Unexpected: File creation succeeded even "
                                 "after exceeding the hard-limit")
        g.log.info("Expected: File creation failed after exceeding "
                   "hard-limit")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

        # Check if the file created above exists
        g.log.info("Checking if the file created exists in the volume %s",
                   self.volname)
        ret = file_exists(client, "%s/foo/20MBfile" % mount_dir)
        self.assertTrue(ret, ("File does not exist in the volume %s",
                              self.volname))
        g.log.info("Successfully validated the presence of file in the "
                   "volume %s", self.volname)

        # Validate if the Quota limit set is appropriate
        g.log.info("Validate if the Quota list fields are appropriate for the "
                   "directory %s of the volume %s", path, self.volname)
        ret = quota_validate(self.mnode, self.volname, path=path,
                             hard_limit=10485760, avail_space=0,
                             sl_exceeded=True, hl_exceeded=True)
        self.assertTrue(ret, ("Failed to validate the Quota limits on "
                              "the volume %s", self.volname))
        g.log.info("Successfully Validated Quota Limit of 100 MB is set on the"
                   " directory %s of the volume %s", path, self.volname)
