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
from glustolibs.gluster.quota_ops import quota_enable, quota_limit_usage
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import move_file
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed-replicated', 'replicated', 'dispersed', 'distributed',
           'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class TestQuotaRenamedDir(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        """
        Setup volume, mount volume and initialize necessary variables
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

    def test_quota_with_renamed_dir(self):
        """
        Verifying directory quota functionality with respect to
        the limit-usage option.
        If a directory has limit set on it and the same directory is renamed ,
        then on doing a quota list the changed name should be reflected.

        * Enable quota on volume
        * Create a directory 'foo' from client
        * Set quota limit of 1GB on /foo
        * Check if quota limit set is correct
        * Rename directory 'foo' to 'bar' from client
        * Check if quota limit set on 'bar' is same as before
        """

        # Enable Quota on the volume
        g.log.info("Enabling Quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertFalse(ret, "Failed to enable Quota on volume %s"
                         % self.volname)

        # Create a directory named 'foo' under any mount dir
        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        g.log.info("Creating dir named 'foo' from client %s", client)
        ret = mkdir(client, "%s/foo" % mount_dir)
        self.assertTrue(ret, "Failed to create dir under %s-%s"
                        % (client, mount_dir))
        g.log.info("Directory 'foo' created successfully")

        # Set Quota Limit of 1GB for dir foo
        g.log.info("Setting a quota limit of 1GB on directory 'foo' inside "
                   "volume %s", self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      "/foo", '1GB')
        self.assertFalse(ret, "Failed to set Quota for dir '/foo'")
        g.log.info("Set quota for dir '/foo' successfully")

        # Get the Quota list and check '/foo' has Quota Limit of 1GB
        g.log.info("Validating if the Quota limit set is correct for the "
                   "path '/foo' in volume %s", self.volname)
        ret = quota_validate(self.mnode, self.volname, path="/foo",
                             hard_limit=1073741824)
        self.assertTrue(ret, ("Quota Limit of 1GB was not set properly on the "
                              "path  /foo' in volume %s", self.volname))
        g.log.info("Successfully Validated Quota Limit of 1GB is set on the "
                   "path '/foo' in volume %s", self.volname)

        # Rename the dir foo to bar
        g.log.info("Renaming dir named 'foo' to 'bar' from client %s", client)
        ret = move_file(client, "%s/foo" % (mount_dir), "%s/bar" % (mount_dir))
        self.assertTrue(ret, "Failed to rename the directory 'foo' under "
                        "%s-%s" % (client, mount_dir))
        g.log.info("Renamed the directory 'foo' to 'bar' successfully")

        # Again get the quota list to check if directory /bar is present
        g.log.info("Validating if the Quota limit set is correct for the "
                   "path '/bar' in volume %s", self.volname)
        ret = quota_validate(self.mnode, self.volname, path="/bar",
                             hard_limit=1073741824)
        self.assertTrue(ret, ("Failed to validate quota limit on the directory"
                              " 'bar'"))
        g.log.info("Successfully Validated Quota Limit of 1GB is set on the "
                   "path '/bar' in volume %s", self.volname)
