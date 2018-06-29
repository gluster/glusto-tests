#  Copyright (C) 2015-2019  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.lib_utils import get_size_of_mountpoint
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_limit_usage,
                                          quota_remove)
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed-replicated', 'replicated', 'dispersed', 'distributed',
           'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class QuotaStatvfs(GlusterBaseClass):

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

    def test_quota_statvfs(self):
        """
        Test statvfs calls return appropriate avaialable size with quota.

        * Enable Quota
        * Save the result from statvfs call
        * Set Quota limit of 1 GB on the root of the volume
        * Validate statvfs call honors quota
        * Remove quota limit from the Volume
        * Validate statvfs call reports old value of avialable space
        """
        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        mount_dir = self.mounts[0].mountpoint
        client = self.mounts[0].client_system

        # Save the result from statvfs call
        orig_avail_space = int(get_size_of_mountpoint(client, mount_dir))

        # Set Quota limit of 1 GB on the root of the volume
        g.log.info("Set Quota Limit on the path '/'")
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path='/', limit="1GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path '/'"))
        g.log.info("Successfully set the Quota limit on '/'")

        # Validate statvfs call honors quota
        avail_space_after_limit = int(get_size_of_mountpoint(client,
                                                             mount_dir))
        g.log.info("space %s", avail_space_after_limit)
        self.assertEqual(avail_space_after_limit * 1024, 1073741824,
                         "avialable space reported by statvfs does not honor \
                          quota limit on '/'")
        g.log.info("successfully validated statvfs honor quota limit on '/'")

        # Remove Quota limit from the Volume
        g.log.info("Remove Quota Limit set on path '/'")
        ret, _, _ = quota_remove(self.mnode, self.volname, path='/')
        self.assertEqual(ret, 0, ("Failed to remove quota limit on path '/' "))
        g.log.info("Successfully removed the Quota limit on path '/'")

        # Validate statvfs call reports old value of avialable space
        avail_space_after_remove = int(get_size_of_mountpoint(client,
                                                              mount_dir))
        g.log.info("space %s", avail_space_after_remove)
        self.assertEqual(avail_space_after_remove, orig_avail_space,
                         "avialable space reported by statvfs not restored \
                          after quota limit is removed on '/'")
        g.log.info("successfully validated statvfs shows original value")
