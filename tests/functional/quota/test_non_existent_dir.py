#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_libs import log_volume_info_and_status
from glustolibs.gluster.quota_ops import (enable_quota,
                                          set_quota_limit_usage)
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class QuotaNonExistentDir(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)
        g.log.info("Starting %s ", cls.__name__)

    def setUp(self):
        # SettingUp volume and Mounting the volume
        GlusterBaseClass.setUp.im_func(self)

        # Setting up the volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)

        g.log.info("Creating volume %s", (self.volname))
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s" %
                                 self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def test_non_existent_dir(self):
        # Displaying volume status and info
        g.log.info("Logging volume information and status")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status"
                              "failed on volume %s", self.volname))

        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = enable_quota(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Non existent path to set quota limit
        path = "/foo"

        # Set Quota limit on the root of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, err = set_quota_limit_usage(self.mnode, self.volname,
                                            path=path, limit="1GB")
        self.assertIn("No such file or directory", err, "Quota limit set "
                      "on path /foo which does not exist")

    def tearDown(self):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)
