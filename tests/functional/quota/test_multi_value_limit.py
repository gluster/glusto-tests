#  Copyright (C) 2015-2020 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_set_soft_timeout,
                                          quota_set_hard_timeout,
                                          quota_limit_usage,
                                          quota_remove)
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed-replicated', 'replicated', 'dispersed', 'distributed',
           'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class QuotaMultiValueLimits(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        """
        setup volume, mount volume and initialize necessary variables
        which is used in tests
        """

        # calling GlusterBaseClass setUpClass
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

    def test_quota_multi_value_limits(self):
        # pylint: disable=too-many-statements
        """
        Verifying directory quota functionality with respect to
        the limit-usage option. Set limits of various values being
        big, small and decimal values. eg. 1GB, 10GB, 2.5GB, etc.
        Set limits on various directories and check for the quota
        list of all the directories.

        * Enable Quota.
        * Create some data on mount point.
        * Set limit of 1 GB on path "/" , perform some I/O from mount
          such that the hard limit is reached and validate with quota list.
        * Set limit of 1.5 GB on path "/" , perform some I/O from mounts
          such that the hard limit is reached and validate with quota list.
        * Set limit of 10 GB on path "/" , perform some I/O from mounts
          such that the hard limit is reached and validate with quota list.
        """

        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Perform some I/O on mounts
        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s ; "
               "for i in `seq 1 10` ; "
               "do dd if=/dev/zero of=foo$i "
               "bs=10M "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Failed to create files on mountpoint")
        g.log.info("Files created successfully on mountpoint")

        # Set soft timeout to 1 second
        g.log.info("Set quota soft timeout:")
        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, '1sec')
        self.assertEqual(ret, 0, ("Failed to set soft timeout"))
        g.log.info("Quota soft timeout set successfully")

        # Set hard timeout to 0 second
        g.log.info("Set quota hard timeout:")
        ret, _, _ = quota_set_hard_timeout(self.mnode, self.volname, '0sec')
        self.assertEqual(ret, 0, ("Failed to set hard timeout"))
        g.log.info("Quota hard timeout set successfully")

        # Path to set quota limit
        path = "/"

        # Set Quota limit of 1 GB on the root of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="1GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

        # Create data such that soft limit is crossed but not the hard limit
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s ; "
               "for i in `seq 11 102` ; "
               "do dd if=/dev/zero of=foo$i "
               "bs=10M "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Failed to create files on mountpoint")
        g.log.info("Files created successfully on mountpoint")

        # Create data such that hard limit is crossed
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s ; "
               "for i in `seq 1 10` ; "
               "do dd if=/dev/zero of=small$i "
               "bs=3M "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 1, "Failed: Files created successfully in spite "
                                 "of crossing hard-limit")
        g.log.info("Files creation stopped on mountpoint once exceeded "
                   "hard limit")

        # Validate if the Quota limit set is correct and hard limit is reached
        # by checking the quota list
        g.log.info("Validate if the Quota limit set is correct for the "
                   "volume %s", self.volname)
        ret = quota_validate(self.mnode, self.volname, path=path,
                             hard_limit=1073741824, avail_space=0,
                             sl_exceeded=True, hl_exceeded=True)
        self.assertTrue(ret, ("Quota Limit of 1 GB was not set properly on the"
                              " volume %s", self.volname))
        g.log.info("Successfully Validated Quota Limit of 1 GB is set on "
                   "volume %s", self.volname)

        # Set Quota limit as decimal value eg. 1.5 GB on the root of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="1.5GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

        # Create data such that soft limit is crossed but not the hard limit
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s ; "
               "for i in `seq 103 152` ; "
               "do dd if=/dev/zero of=foo$i "
               "bs=10M "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Failed to create files on mountpoint")
        g.log.info("Files created successfully on mountpoint")

        # Create data such that hard limit is crossed
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s ; "
               "for i in `seq 1 10` ; "
               "do dd if=/dev/zero of=medium$i "
               "bs=5M "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 1, "Failed: Files created successfully in spite "
                                 "of crossing hard-limit")
        g.log.info("Files creation stopped on mountpoint once exceeded "
                   "hard limit")

        # Validate if the Quota limit set is correct and hard limit is reached
        # by checking the quota list
        g.log.info("Validate if the Quota limit set is correct for the "
                   "volume %s", self.volname)
        ret = quota_validate(self.mnode, self.volname, path=path,
                             hard_limit=1610612736, avail_space=0,
                             sl_exceeded=True, hl_exceeded=True)
        self.assertTrue(ret, ("Quota Limit of 1.5GB was not set properly on "
                              "the volume %s", self.volname))
        g.log.info("Successfully Validated Quota Limit of 1.5 GB is set on "
                   "volume %s", self.volname)

        # Set Quota limit of 10 GB on the root of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="10GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

        # Create data such that soft limit is crossed but not the hard limit
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s ; "
               "for i in `seq 1 17` ; "
               "do dd if=/dev/zero of=large$i "
               "bs=500M "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Failed to create files on mountpoint")
        g.log.info("Files created successfully on mountpoint")

        # Create data such that hard limit is crossed
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s ; "
               "for i in `seq 1 30` ; "
               "do dd if=/dev/zero of=largelimit$i "
               "bs=10M "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 1, "Failed: Files created successfully in spite "
                                 "of crossing hard-limit")
        g.log.info("Files creation stopped on mountpoint once exceeded "
                   "hard limit")

        # Validate if the Quota limit set is correct and hard limit is reached
        # by checking the quota list
        g.log.info("Validate if the Quota limit set is correct for the "
                   "volume %s", self.volname)
        ret = quota_validate(self.mnode, self.volname, path=path,
                             hard_limit=10737418240, avail_space=0,
                             sl_exceeded=True, hl_exceeded=True)
        self.assertTrue(ret, ("Quota Limit of 10GB was not set properly on the"
                              " volume %s", self.volname))
        g.log.info("Successfully Validated Quota Limit of 10 GB is set on "
                   "volume %s", self.volname)

        # Remove Quota from the Volume
        g.log.info("Remove Quota Limit set on path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_remove(self.mnode, self.volname, path=path)
        self.assertEqual(ret, 0, ("Failed to remove quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully removed the Quota limit on path %s of the "
                   "volume %s", path, self.volname)
