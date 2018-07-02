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
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_fetch_list,
                                          quota_limit_usage)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class QuotaLimitDirBreadth(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [script_local_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

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
        GlusterBaseClass.tearDown.im_func(self)

    def test_quota_limit_dir_breadth(self):
        """
        Verifying directory quota functionality with respect to the
        limit-usage option. Set limits on various directories [breadth]
        and check for the quota list of all the directories.

        * Enable Quota
        * Create 10 directories and set limit of 1GB on each directory
        * Perform a quota list operation
        * Create some random amount of data inside each directory
        * Perform a quota list operation
        """
        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Create Directories in the mount point
        for mount_object in self.mounts:
            g.log.info("Creating Directories on %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            cmd = ('python %s create_deep_dir -d 0 -l 10 %s'
                   % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, cmd,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Get dir list
        g.log.info('Getting dir list in %s', self.volname)
        cmd = ('ls %s' % self.mounts[0].mountpoint)
        ret, out, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)
        dir_list = out.split()
        for dir_name in dir_list:
            dir_list[dir_list.index(dir_name)] = '/%s' % dir_name

        # Set limit of 1 GB on every directory created inside the mountpoint
        g.log.info("Set Quota Limit on each directory of the volume %s",
                   self.volname)
        for dir_name in dir_list:
            ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                          dir_name, '1GB')
            self.assertFalse(ret, "Failed to set Quota for dir %s" % dir_name)
            g.log.info("Set quota for dir %s successfully", dir_name)
        g.log.info("Successfully set the Quota limit on each path of the "
                   "volume %s", self.volname)

        # Get Quota List on every Directory of the Volume
        g.log.info("Get Quota list for every directory on the volume %s",
                   self.volname)
        for dir_name in dir_list:
            quota_list = quota_fetch_list(self.mnode, self.volname,
                                          path=dir_name)
            self.assertIsNotNone(quota_list, ("Failed to get the quota list "
                                              "for the volume %s",
                                              self.volname))
            hard_limit = quota_list[dir_name]['hard_limit']
            self.assertEqual(hard_limit, '1073741824',
                             "Hard limit does not match the actual "
                             "limit-usage set on the directory %s" % dir_name)

            g.log.info("Hard limit matches the actual limit-usage "
                       "set on the directory %s", dir_name)

        # Create some data inside each directory
        self.all_mounts_procs = []
        for mount_object in self.mounts:
            g.log.info("Creating Files on %s:%s",
                       mount_object.client_system,
                       mount_object.mountpoint)
            for i in range(1, 11):
                dir_name = "/user" + str(i)
                cmd = ("python %s create_files -f 10 --fixed-file-size 1M "
                       "%s/%s"
                       % (self.script_upload_path, mount_object.mountpoint,
                          dir_name))

                ret, _, _ = g.run(mount_object.client_system, cmd)
                self.assertFalse(ret, "Failed to create files in %s"
                                 % dir_name)
                g.log.info("Files created succesfully in %s", dir_name)

        # Get Quota list on Volume
        g.log.info("Get Quota list for every directory on the volume %s",
                   self.volname)
        for dir_name in dir_list:
            quota_list = quota_fetch_list(self.mnode, self.volname,
                                          path=dir_name)
            self.assertIsNotNone(quota_list, ("Failed to get the quota list "
                                              "for the volume %s",
                                              self.volname))
            hard_limit = quota_list[dir_name]['hard_limit']
            self.assertEqual(hard_limit, '1073741824',
                             "Hard limit does not match the actual "
                             "limit-usage set on the directory %s" % dir_name)

            g.log.info("Hard limit matches the actual limit-usage "
                       "set on the directory %s", dir_name)
