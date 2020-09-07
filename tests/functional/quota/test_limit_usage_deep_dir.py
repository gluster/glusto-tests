#  Copyright (C) 2015-2020  Red Hat, Inc. <http://www.redhat.com>
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

import random

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_remove,
                                          quota_set_soft_timeout,
                                          quota_set_hard_timeout,
                                          quota_limit_usage)
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.glusterdir import (mkdir,
                                           rmdir)


@runs_on([['distributed-replicated', 'replicated', 'distributed'],
          ['glusterfs', 'nfs']])
class LimitUsageDeepDir(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_limit_usage_deep_dir(self):
        # pylint: disable=too-many-statements
        """
        Verifying directory quota functionality with respect to the
        limit-usage option. Set limits on various directories [breadth]
        and check for the quota list of all the directories.

        * Enable Quota
        * Create 10 directories one inside the other and set limit of 1GB
          on each directory
        * Perform a quota list operation
        * Create some random amount of data inside each directory
        * Perform a quota list operation
        * Remove the quota limit and delete the data
        """
        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Create deep directories in the mount point
        for mount_object in self.mounts:
            g.log.info("Creating directories on %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            ret = mkdir(mount_object.client_system,
                        "%s/dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10"
                        % (mount_object.mountpoint), parents=True)
            self.assertTrue(ret, ("Failed to create dir under %s-%s",
                                  mount_object.client_system,
                                  mount_object.mountpoint))
            g.log.info("Successfully created deep directories on %s:%s",
                       mount_object.client_system, mount_object.mountpoint)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Set soft timeout to 1 second
        g.log.info("Set quota soft timeout:")
        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, '1sec')
        self.assertEqual(ret, 0, ("Failed to set soft timeout"))
        g.log.info("Quota soft timeout set successful")

        # Set hard timeout to 0 second
        g.log.info("Set quota hard timeout:")
        ret, _, _ = quota_set_hard_timeout(self.mnode, self.volname, '0sec')
        self.assertEqual(ret, 0, ("Failed to set hard timeout"))
        g.log.info("Quota hard timeout set successful")

        # Get dir list
        g.log.info('Getting dir list in %s', self.volname)
        cmd = ("ls -R %s | grep ':' | tr -d :" % self.mounts[0].mountpoint)
        ret, out, err = g.run(self.mounts[0].client_system, cmd)
        g.log.info('mountpoint %s', self.mounts[0].mountpoint)
        self.assertFalse(ret, err)
        dir_list = out.split()
        for dir_name in dir_list:
            # Parsed to remove the mount point as quota doesn't work when
            # passed with mountpoint.
            tmp_name = dir_name.replace(self.mounts[0].mountpoint, "")
            dir_list[dir_list.index(dir_name)] = '%s' % tmp_name
        dir_list.pop(0)
        # The first entry of ls -R is the current directory which is not
        # necessary.

        # Set limit of 1 GB on every directory created inside the mountpoint
        g.log.info("Set Quota Limit on each directory of the volume %s",
                   self.volname)
        for dir_name in dir_list:
            ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                          dir_name, '1GB')
            self.assertFalse(ret, "Failed to set Quota for dir %s" %
                             dir_name)
            g.log.info("Set quota for dir %s successfully", dir_name)
        g.log.info("Successfully set the Quota limit on each path of the "
                   "volume %s", self.volname)

        # Validate quota on every Directory of the Volume
        g.log.info("Get Quota list for every directory on the volume %s",
                   self.volname)
        for dir_name in dir_list:
            ret = quota_validate(self.mnode, self.volname, path=dir_name,
                                 hard_limit=1073741824)
            self.assertTrue(ret, "Quota validate Failed for dir %s" %
                            dir_name)

        # Create some data inside each directory and do a quota validate
        self.all_mounts_procs = []
        for mount_object in self.mounts:
            g.log.info("Creating Files on %s:%s", mount_object.client_system,
                       mount_object.mountpoint)
            # Data creation
            # Creates one file of rand[0] size in each dir
            rand = random.sample([1, 10, 512], 1)
            cmd = ("/usr/bin/env python %s create_files "
                   "--fixed-file-size %sk %s/%s" % (
                       self.script_upload_path,
                       rand[0], mount_object.mountpoint, dir_list[0]))

            ret, _, _ = g.run(mount_object.client_system, cmd)
            self.assertFalse(ret, "Failed to create files")

            # quota_validate for each dir
            for dir_num, dir_name in enumerate(dir_list):
                # To calculate the dir usage for quota
                usage = (rand[0] * 1024) + \
                         ((len(dir_list) - (dir_num + 1)) * rand[0] * 1024)
                if usage >= 1073741824:
                    raise ExecutionError("usage crossed hardlimit")
                ret = quota_validate(self.mnode, self.volname, path=dir_name,
                                     hard_limit=1073741824, used_space=usage)
                self.assertTrue(ret, "Quota validate Failed for dir %s" %
                                dir_name)
                g.log.info("Quota list validate  and file created successful "
                           "for %s", dir_name)
            g.log.info("Files created and quota validated successfully")

        # Deleting data and validating quota
        self.all_mounts_procs = []
        # Deleting deep directories in the mount point
        for mount_object in self.mounts:
            ret = rmdir(mount_object.client_system, "%s/dir1/dir2" %
                        (mount_object.mountpoint), force=True)
            self.assertTrue(ret, ("Failed to delete dir under %s/dir1/dir2"
                                  % (mount_object.mountpoint)))
            g.log.info("Successfully deleted deep directories")
            # Quota validate
            # converting into bytes
            usage = (rand[0] * 1024)
            ret = quota_validate(self.mnode, self.volname,
                                 path=dir_list[0],
                                 used_space=usage)
            self.assertTrue(ret, "Quota validate Failed for dir /dir1")
            g.log.info("Quota list validate successful for /dir1")

        # Remove Quota limit
        g.log.info("Get Quota list for every directory on the volume %s",
                   self.volname)
        ret = quota_remove(self.mnode, self.volname, path=dir_list[0])
        self.assertTrue(ret, "Failed to remove Quota for dir %s" % dir_name)
        g.log.info("Quota remove  for dir %s successfully", dir_name)
