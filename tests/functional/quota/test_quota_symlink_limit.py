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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_limit_usage)
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

    def test_quota_symlink_limit(self):
        """
        Verifying Directory Quota functionality with respect to limit-usage.
        Setting quota limit on a symlink should fail.

        * Enable quota
        * Set a quota limit on the volume
        * Create a directory
        * Create a symlink of the directory
        * Try to set quota limit on the symlink
        """

        # pylint: disable=too-many-statements
        # Enable Quota on the volume
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Path to set the Quota limit
        path = '/'

        # Set Quota limit of 100 MB on the directory 'foo' of the volume
        g.log.info("Set Quota Limit on the volume %s", self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="100MB")
        self.assertEqual(ret, 0, ("Failed to set Quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on the volume %s",
                   self.volname)

        # Create a directory 'foo' from the mount point
        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        g.log.info("Creating dir named 'foo' from client %s", client)
        ret = mkdir(client, "%s/foo" % mount_dir)
        self.assertTrue(ret, "Failed to create dir under %s-%s"
                        % (client, mount_dir))
        g.log.info("Directory 'foo' created successfully")

        # Create a symlink of the directory 'foo' from mount point
        g.log.info("Creating symlink of dir 'foo' from client %s", client)
        cmd = ("cd %s ; ln -s foo bar" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Failed to create symlink for directory foo")
        g.log.info("Successfully created symlink for the directory foo")

        # Try to set a quota limit on the symlink
        g.log.info("Set Quota Limit on the symlink 'bar' of the volume %s",
                   self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path="/bar", limit="100MB")
        self.assertEqual(ret, 1, ("Failed: Unexpected Quota limit set on the "
                                  "symlink successfully"))
        g.log.info("Successful: Quota limit failed to set on the symlink 'bar'"
                   " of the volume %s", self.volname)
