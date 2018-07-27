#  Copyright (C) 2015-2018  Red Hat, Inc. <http://www.redhat.com>
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
                                          quota_limit_usage)
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['distributed-replicated', 'replicated', 'dispersed', 'distributed',
           'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class QuotaVolumeAndSubdirLimits(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        """
        setup volume, mount volume and initialize necessary variables
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

    def test_quota_volume_subdir_limits(self):
        """
        Verifying directory quota functionality WRT limit-usage on volume
        as well as sub-directories in volume.

        * Enable quota
        * Set a limit of 1 GB on / of volume
        * Create 10 directories on mount point
        * Set a limit of 100 MB on all the sub-directories created
        * Create data inside the sub-directories on mount point till the limits
          are reached
        * Validate if the hard limit and available space fields inside the
          quota list command are appropriate
        """

        # Enable quota on the volume
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Path to set quota limit
        path = "/"

        # Set a limit of 1 GB on the root of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="1GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

        # Create 10 directories from the mount point
        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        g.log.info("Creating directories on %s:%s", client, mount_dir)
        for i in range(1, 11):
            ret = mkdir(client, "%s/foo%s" % (mount_dir, i))
            self.assertTrue(ret, ("Failed to create dir under %s-%s",
                                  client, mount_dir))
            g.log.info("Directory 'foo%s' created successfully", i)
        g.log.info("Successfully created directories on %s:%s",
                   client, mount_dir)

        # Set a limit of 100 MB on each directory
        g.log.info("Setting a limit of 100 MB on all the directories inside "
                   "the volume %s", self.volname)
        for j in range(1, 11):
            dir_name = "/foo" + str(j)
            ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                          path=dir_name, limit="100MB")
            self.assertEqual(ret, 0, ("Failed to set quota limit on path "
                                      "%s of the volume %s",
                                      dir_name, self.volname))
            g.log.info("Successfully set the Quota limit on /foo%s of "
                       "the volume %s", j, self.volname)
        g.log.info("Successfully set the limit of 100 MB on all directories "
                   "inside the volume %s", self.volname)

        # Validate if quota limit usage is set properly
        g.log.info("Validate quota limit usage on all directories")
        for k in range(1, 11):
            dir_name = "/foo" + str(k)
            ret = quota_validate(self.mnode, self.volname, path=dir_name,
                                 hard_limit=104857600)
            self.assertTrue(ret, ("Failed to validate quota limit usage on the"
                                  "directory %s", dir_name))
            g.log.info("Successfully validated quota limit usage for the "
                       "directory %s of volume %s", dir_name, self.volname)

        # Create data inside each directory from mount point
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        for var1 in range(1, 11):
            cmd = ("cd %s/foo%s ; "
                   "for i in `seq 1 100` ; "
                   "do dd if=/dev/zero of=testfile$i "
                   "bs=1M "
                   "count=1 ; "
                   "done"
                   % (mount_dir, var1))
            ret, _, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, ("Failed to create files in /foo%s",
                                      var1))
            g.log.info("Files created successfully in /foo%s", var1)
        g.log.info("Files creation is successful on all directories of the "
                   "volume %s", self.volname)

        # List the files inside each directory
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

        # Validate the hard limit and available space fields are appropriate
        g.log.info("Validate quota hard limit and available space on all the "
                   "directories are appropriate")
        for var2 in range(1, 11):
            dir_name = "/foo" + str(var2)
            ret = quota_validate(self.mnode, self.volname, path=dir_name,
                                 hard_limit=104857600, avail_space=0,
                                 sl_exceeded=True, hl_exceeded=True,
                                 used_space=104857600)
            self.assertTrue(ret, ("Failed to validate quota hard limit and "
                                  "available space on the directory %s",
                                  dir_name))
            g.log.info("Successfully validated quota hard limit and available"
                       " space fields inside quota list for directory %s "
                       "of volume %s", dir_name, self.volname)
