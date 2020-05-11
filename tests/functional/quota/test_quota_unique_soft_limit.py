#  Copyright (C) 2017-2020 Red Hat, Inc. <http://www.redhat.com>
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

import time
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_limit_usage,
                                          quota_set_soft_timeout)
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['distributed-replicated', 'replicated', 'dispersed', 'distributed',
           'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class QuotaUniqueSoftLimit(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        """
        Setup volume, mount volume and initialize necessary variables
        which is used in tests
        """
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup and Mount Volume %s",
                   cls.volname)
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume %s"
                                 % cls.volname)
        g.log.info("Successful in Setup and Mount Volume %s", cls.volname)

    def tearDown(self):

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_quota_unique_soft_limit(self):
        """
        Validating directory quota functionality WRT soft-limit

        * Enable quota
        * Create 10 directories
        * Set a hard-limit of 100 MB and a unique soft-limit on each directory
          example : 5%, 15%, 25%, 35%, ...
        * Create some data inside the directories such that the
          soft-limit is exceeded
        * Perform quota list operation to validate if the
          soft-limit is exceeded
        """

        # pylint: disable=too-many-statements
        # Enable Quota on the volume
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Set soft timeout to 0 second
        g.log.info("Set quota soft timeout:")
        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, '0sec')
        self.assertEqual(ret, 0, ("Failed to set soft timeout"))
        g.log.info("Quota soft timeout set successful")

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

        # Set a hard-limit of 100 MB with a unique soft-limit
        # having a difference of 10% on each directory
        g.log.info("Setting a limit of 100 MB and unique soft-limit on all "
                   "the directories inside the volume %s", self.volname)
        var1 = 1
        for var2 in range(5, 100, 10):
            dir_name = "/foo" + str(var1)
            softlim = str(var2) + "%"
            ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                          path=dir_name, limit="100MB",
                                          soft_limit=softlim)
            self.assertEqual(ret, 0, ("Failed to set quota limits on path "
                                      "%s with soft-limit %s for the "
                                      "volume %s",
                                      dir_name, softlim, self.volname))
            g.log.info("Successfully set the Quota limits on %s of "
                       "the volume %s", dir_name, self.volname)
            var1 = var1 + 1

        # Validate hard/soft limits set above , Create data inside the
        # directories such that soft limit is exceeded and validate with
        # quota list command
        var3 = 1
        for var4 in range(5, 100, 10):
            dir_name = "/foo" + str(var3)
            softlim = var4
            data = var4 + 1

            # Validate if the hard limit and soft limit is set properly
            # on all directories
            g.log.info("Validate quota limit usage on the directory %s of the "
                       "volume %s", dir_name, self.volname)
            ret = quota_validate(self.mnode, self.volname, path=dir_name,
                                 hard_limit=104857600,
                                 soft_limit_percent=softlim)
            self.assertTrue(ret, ("Failed to validate quota limit usage on the"
                                  " directory %s of the volume %s",
                                  dir_name, self.volname))
            g.log.info("Successfully validated quota limit usage for the "
                       "directory %s of the volume %s", dir_name, self.volname)

            # Perform IO on each Directory
            g.log.info("Creating Files on %s:%s", client, mount_dir)
            cmd = ("cd %s%s ; "
                   "dd if=/dev/zero of=foo%s "
                   "bs=%sM "
                   "count=1 "
                   % (mount_dir, dir_name, var3, data))
            ret, _, _ = g.run(client, cmd)
            self.assertEqual(ret, 0, ("Failed to create files on %s",
                                      dir_name))
            g.log.info("Files created successfully on %s", dir_name)

            time.sleep(1)

            # Validate quota list and check if soft-limit is exceeded for
            # each directory
            g.log.info("Validate quota limit usage and soft-limit on the "
                       "directory %s", dir_name)
            ret = quota_validate(self.mnode, self.volname, path=dir_name,
                                 hard_limit=104857600,
                                 soft_limit_percent=softlim,
                                 sl_exceeded=True)
            self.assertTrue(ret, ("Failed to validate quota limit usage on the"
                                  " directory %s of the volume %s",
                                  dir_name, self.volname))
            g.log.info("Successfully validated quota limit usage for the "
                       "directory %s of volume %s", dir_name, self.volname)
            var3 = var3 + 1
