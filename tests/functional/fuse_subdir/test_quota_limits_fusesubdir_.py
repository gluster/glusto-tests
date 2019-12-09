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
import copy
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_limit_usage,
                                          is_quota_enabled,
                                          quota_fetch_list)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.auth_ops import set_auth_allow


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class FuseSubdirQuotaTest(GlusterBaseClass):
    """
    Test case validates fuse subdir functionality when quota is enabled
    on subdir
    """
    @classmethod
    def setUpClass(cls):
        """
        setup volume and mount volume
        calling GlusterBaseClass setUpClass
        """
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup and Mount Volume %s",
                   cls.volname)
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume "
                                 "and Mount_Volume %s" % cls.volname)
        g.log.info("Successful in Setup and Mount Volume %s", cls.volname)

    def test_subdir_with_quota_limit(self):

        # pylint: disable=too-many-statements
        """
        Mount the volume
        Create 2 subdir on mount point
        dir1-> /level1/subdir1 dir2->/dlevel1/dlevel2/dlevel3/subdir2
        Auth allow - Client1(/level1/subdir1),
        Client2(/dlevel1/dlevel2/dlevel3/subdir2)
        Mount the subdir1 on client 1 and subdir2 on client2
        Enable Quota
        Verify Quota is enabled on volume
        Set quota limit as 1GB and 2GB on both subdirs respectively
        Perform a quota list operation
        Perform IO's on both subdir until quota limit is almost hit for subdir1
        Again Perform a quota list operation
        Run IO's on Client 1.This should fail
        Run IO's on Client2.This should pass
        """

        # Create deep subdirectories  subdir1 and subdir2 on mount point
        ret = mkdir(self.mounts[0].client_system, "%s/level1/subdir1"
                    % self.mounts[0].mountpoint, parents=True)
        self.assertTrue(ret, ("Failed to create directory '/level1/subdir1' on"
                              "volume %s from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))
        ret = mkdir(self.mounts[0].client_system,
                    "%s/dlevel1/dlevel2/dlevel3/subdir2"
                    % self.mounts[0].mountpoint, parents=True)
        self.assertTrue(ret, ("Failed to create directory "
                              "'/dlevel1/dlevel2/dlevel3/subdir2' on"
                              "volume %s from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))
        # unmount volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Volumes Unmount failed")
        g.log.info("Volumes Unmounted successfully")

        # Set authentication on the subdirectory subdir1
        # and subdir2
        g.log.info('Setting authentication on directories subdir1 and subdir2'
                   'for client %s and %s', self.clients[0], self.clients[1])
        ret = set_auth_allow(self.volname, self.mnode,
                             {'/level1/subdir1': [self.clients[0]],
                              '/dlevel1/dlevel2/dlevel3/subdir2':
                              [self.clients[1]]})
        self.assertTrue(ret,
                        'Failed to set Authentication on volume %s'
                        % self.volume)

        # Creating mount list for subdirectories
        self.subdir_mounts = [copy.deepcopy(self.mounts[0]),
                              copy.deepcopy(self.mounts[1])]
        self.subdir_mounts[0].volname = "%s/level1/subdir1" % self.volname
        self.subdir_mounts[1].volname = ("%s/dlevel1/dlevel2/dlevel3/subdir2"
                                         % self.volname)

        # Mount Subdirectory "subdir1" on client 1 and "subdir2" on client 2
        for mount_obj in self.subdir_mounts:
            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount  %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully mounted subdirectories on client1"
                   "and clients 2")

        # Enable quota on volume
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Check if quota is enabled
        g.log.info("Validate Quota is enabled on the volume %s", self.volname)
        ret = is_quota_enabled(self.mnode, self.volname)
        self.assertTrue(ret, ("Quota is not enabled on the volume %s",
                              self.volname))
        g.log.info("Successfully Validated quota is enabled on volume %s",
                   self.volname)

        # Setting up path to set quota limit

        path1 = "/level1/subdir1"
        path2 = "/dlevel1/dlevel2/dlevel3/subdir2"

        # Set Quota limit on the subdirectory "subdir1"

        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path1, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path1, limit="1GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  " the volume %s", path1, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume "
                   "%s", path1, self.volname)

        # Set Quota limit on the subdirectory "subdir2"

        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path2, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path2, limit="2GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  " the volume %s", path2, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume "
                   "%s", path2, self.volname)

        # Get Quota List on the volume

        g.log.info("Get Quota list on the volume %s",
                   self.volname)
        quota_list = quota_fetch_list(self.mnode, self.volname)

        self.assertIsNotNone(quota_list, ("Failed to get the quota list "
                                          "of the volume %s",
                                          self.volname))

        # Check for subdir1 path in quota list

        self.assertIn(path1, quota_list.keys(),
                      ("%s not part of the quota list %s even if "
                       "it is set on the volume %s", path1,
                       quota_list, self.volname))

        # Check for subdir2 path in quota list

        self.assertIn(path2, quota_list.keys(),
                      ("%s not part of the quota list %s even if "
                       "it is set on the volume %s", path2,
                       quota_list, self.volname))
        g.log.info("Successfully listed quota list %s of the "
                   "volume %s", quota_list, self.volname)

        # Create near to 1GB of data on both subdir mounts

        for mount_object in self.subdir_mounts:
            g.log.info("Creating Files on %s:%s", mount_object.client_system,
                       mount_object.mountpoint)
            cmd = ("cd %s ; for i in `seq 1 1023` ;"
                   "do dd if=/dev/urandom of=file$i bs=1M "
                   "count=1;done" % (mount_object.mountpoint))
            ret, _, _ = g.run(mount_object.client_system, cmd)
            self.assertEqual(ret, 0, "Failed to create files on mountpoint")
            g.log.info("Files created successfully on mountpoint")

        # Again Get Quota List on the volume

        g.log.info("Get Quota list on the volume %s",
                   self.volname)
        quota_list = quota_fetch_list(self.mnode, self.volname)

        self.assertIsNotNone(quota_list, ("Failed to get the quota list "
                                          "of the volume %s",
                                          self.volname))

        # Check for subdir1 path in quota list

        self.assertIn(path1, quota_list.keys(),
                      ("%s not part of the quota list %s even if "
                       "it is set on the volume %s", path1,
                       quota_list, self.volname))

        # Check for subdir2 path in quota list

        self.assertIn(path2, quota_list.keys(),
                      ("%s not part of the quota list %s even if "
                       "it is set on the volume %s", path2,
                       quota_list, self.volname))
        g.log.info("Successfully listed quota list %s of the "
                   "volume %s", quota_list, self.volname)

        # Again run IO's to check if quota limit is adhere for subdir1

        # Start IO's on subdir1
        g.log.info("Creating Files on %s:%s", self.clients[0],
                   self.subdir_mounts[0].mountpoint)
        cmd = ("cd %s ; for i in `seq 1024 1500` ;"
               "do dd if=/dev/urandom of=file$i bs=1M "
               "count=1;done" % (self.subdir_mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        if ret == 0:
            raise ExecutionError("IO was expected to Fail."
                                 "But it got passed")
        else:
            g.log.info("IO's failed as expected on %s:%s as quota "
                       "limit reached already",
                       self.clients[0], self.subdir_mounts[0].mountpoint)

        # Start IO's on subdir2
        g.log.info("Creating Files on %s:%s", self.clients[1],
                   self.subdir_mounts[1].mountpoint)
        cmd = ("cd %s ; for i in `seq 1024 1500` ;"
               "do dd if=/dev/urandom of=file$i bs=1M "
               "count=1;done" % (self.subdir_mounts[1].mountpoint))
        ret, _, _ = g.run(self.clients[1], cmd)
        self.assertEqual(ret, 0, ("Failed to create files on %s"
                                  % self.clients[1]))
        g.log.info("Files created successfully on %s:%s",
                   self.clients[1], self.subdir_mounts[1].mountpoint)

    def tearDown(self):
        """
        Clean up the volume and umount subdirectories from client
        """

        # Unmount sub-directories from client
        # Test needs to continue if  unmount fail.Not asserting here.
        ret = self.unmount_volume(self.subdir_mounts)
        if ret:
            g.log.info("Successfully unmounted all the subdirectories")
        else:
            g.log.error("Failed to unmount sub-directories")

        # cleanup-volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Cleanup volume %s Completed Successfully", self.volname)
