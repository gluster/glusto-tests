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
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_fetch_list,
                                          quota_limit_usage)
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.glusterdir import (mkdir,
                                           rmdir)
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online
)


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class QuotaNoDir(GlusterBaseClass):
    def setUp(self):
        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Creating volume %s", (self.volname))

        # Setting up the volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)

        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s" %
                                 self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_no_dir(self):
        """
        * Enable quota on the volume
        * Set the quota on the non-existing directory
        * Create the directory as above and set limit
        * Validate the quota on the volume
        * Delete the directory
        * Validate the quota on volume
        * Recreate the directory
        * Validate the quota on volume
        * Check for volume status for all processes being online.
        """
        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Non existent path to set quota limit
        path = "/foo"

        # Set Quota limit on /foo of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, err = quota_limit_usage(self.mnode, self.volname,
                                        path=path, limit="1GB")
        self.assertIn("No such file or directory", err, "Quota limit set "
                      "on path /foo which does not exist")

        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        # Create the directory on which limit was tried to be set
        ret = mkdir(client, "%s/foo" % (mount_dir))
        self.assertTrue(ret, ("Failed to create dir under %s-%s",
                              client, mount_dir))
        g.log.info("Directory 'foo' created successfully")

        # Set Quota limit on /foo of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, err = quota_limit_usage(self.mnode, self.volname,
                                        path=path, limit="1GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

        # Validate quota list
        g.log.info("Get Quota list for foo and see if hardlimit is 1GB")
        ret = quota_validate(self.mnode, self.volname, path=path,
                             hard_limit=1073741824)
        self.assertTrue(ret, "Quota validate Failed for dir foo")

        # Delete the directory
        ret = rmdir(client, "%s/foo" %
                    (mount_dir), force=True)
        self.assertTrue(ret, ("Failed to delete dir /foo"))
        g.log.info("Successfully deleted /foo")

        # Validate quota list
        g.log.info("Get empty quota list")
        quota_list1 = quota_fetch_list(self.mnode, self.volname, path=None)
        self.assertIsNone(quota_list1, ("unexpected quota list entries found"))
        g.log.info("Successfully validated quota limit usage for the "
                   "deleted directory foo")

        # Recreate the same deleted directory
        ret = mkdir(client, "%s/foo" % (mount_dir))
        self.assertTrue(ret, ("Failed to create dir under %s-%s",
                              client, mount_dir))
        g.log.info("Directory 'foo' created successfully")

        # Validate quota list
        g.log.info("Get Quota list for foo and see if hardlimit is N/A")
        ret = quota_validate(self.mnode, self.volname, path=path,
                             hard_limit='N/A')
        self.assertTrue(ret, "Quota validate Failed for dir foo")
        g.log.info("Successfully validated quota limit usage for the "
                   "recreated directory foo")

        # Verify volume's all process are online
        g.log.info("Volume %s: Verifying that all process are online",
                   self.volname)
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online ",
                              self.volname))
        g.log.info("Volume %s: All process are online", self.volname)

    def tearDown(self):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
