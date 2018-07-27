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
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_limit_usage,
                                          quota_fetch_list)
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class QuotaListPathValues(GlusterBaseClass):
    """
    QuotaListPathValues contains tests which verifies the
    quota list functionality with and without path

    """

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
            raise ExecutionError("Failed to Setup_Volume "
                                 "and Mount_Volume %s" % cls.volname)
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

    def test_quota_list_path_values(self):
        """

        Verifying directory quota list functionality where giving
        the quota list command with and without the path should return
        the same output.

        * Enable quota
        * Set limit of 2 GB on /
        * Create data inside the volume
        * Execute a quota list command with and without path
          where both outputs should be same

        """

        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Path to set quota limit
        path = "/"

        # Set Quota limit on the root of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="2GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  " the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume "
                   "%s", path, self.volname)

        # Starting IO on the mounts
        for mount_object in self.mounts:
            g.log.info("Creating Files on %s:%s", mount_object.client_system,
                       mount_object.mountpoint)
            cmd = ("cd %s ; mkdir foo ; cd foo ; for i in `seq 1 100` ;"
                   "do dd if=/dev/urandom of=file$i bs=20M "
                   "count=1;done" % (mount_object.mountpoint))
            ret, _, _ = g.run(mount_object.client_system, cmd)
            self.assertEqual(ret, 0, "Failed to create files on mountpoint")
            g.log.info("Files created successfully on mountpoint")

        # Get Quota list without specifying the path
        g.log.info("Get Quota list for the volume %s", self.volname)
        quota_list1 = quota_fetch_list(self.mnode, self.volname, path=None)
        self.assertIsNotNone(quota_list1, ("Failed to get the quota list for "
                                           "the volume %s", self.volname))
        self.assertIn(path, quota_list1.keys(),
                      ("%s not part of the ""quota list %s even if "
                       "it is set on the volume %s", path,
                       quota_list1, self.volname))
        g.log.info("Successfully listed quota list %s of the "
                   "volume %s", quota_list1, self.volname)

        # Get Quota List with path mentioned in the command
        g.log.info("Get Quota list for path %s of the volume %s",
                   path, self.volname)
        quota_list2 = quota_fetch_list(self.mnode, self.volname, path=path)
        self.assertIsNotNone(quota_list2, ("Failed to get the quota list for "
                                           "path %s of the volume %s",
                                           path, self.volname))
        self.assertIn(path, quota_list2.keys(),
                      ("%s not part of the ""quota list %s even if "
                       "it is set on the volume %s", path,
                       quota_list2, self.volname))
        g.log.info("Successfully listed path %s in the quota list %s of the "
                   "volume %s", path, quota_list2, self.volname)

        # Validate both outputs of the list commands with and without paths
        g.log.info("Validating the output of quota list command with path and "
                   "without path is the same for volume %s.", self.volname)
        self.assertEqual(quota_list1,
                         quota_list2, ("The output of quota list for volume "
                                       "%s is different as compared among "
                                       "command with path and command without "
                                       "path.", self.volname))
        g.log.info("Successfully validated both outputs of quota list command "
                   "with and without path are the same in volume "
                   "%s.", self.volname)
