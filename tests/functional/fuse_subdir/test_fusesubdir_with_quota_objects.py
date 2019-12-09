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
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.auth_ops import set_auth_allow
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_limit_objects,
                                          quota_set_soft_timeout,
                                          quota_set_hard_timeout,
                                          is_quota_enabled,
                                          quota_fetch_list_objects)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class SubdirWithQuotaObject(GlusterBaseClass):
    """
    This test case validates fuse subdir functionality with respect
    to quota objects when quota object limits are set on subdir and
    volume
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

    def test_subdir_with_quotaobject(self):

        # pylint: disable=too-many-statements
        """
        Mount the volume
        Create 1 subdir on mountpoint "d1"
        unmount volume
        Auth allow - Client1(d1),Client2(full volume)
        Mount the subdir "d1" on client1 and volume on client2
        Enable quota on volume
        Set quota object limit on subdir "d1" and volume
        subdir "d1" quota limit- 50
        Volume quota limit - 200
        Start writing 49 files on both subdir "d1" and volume
        Fetch quota limit object list
        Write 1 more file on subdir.This should fail
        Again reset quota object limit to 75 now on subdir "d1"
        Create 24 directories on subdir and volume.This should pass
        Fetch quota limit object list
        Create 1 more directory on subdir.This should fail
        Create 1 more directory on volume.This should pass
        """
        # Create  directory d1 on mount point
        ret = mkdir(self.mounts[0].client_system, "%s/d1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'd1' on "
                              "volume %s from client %s"
                              % (self.mounts[0].volname,
                                 self.mounts[0].client_system)))
        # unmount volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Volumes Unmount failed")
        g.log.info("Volumes Unmounted successfully")

        # Set authentication on the subdirectoy "d1" to access by client1
        # and volume to access by client2
        g.log.info('Setting authentication on subdirectory d1 to access '
                   'by client %s and on volume to access by client %s',
                   self.clients[0], self.clients[1])
        ret = set_auth_allow(self.volname, self.mnode,
                             {'/d1': [self.clients[0]],
                              '/': [self.clients[1]]})
        self.assertTrue(ret,
                        'Failed to set Authentication on volume %s'
                        % self.volume)

        # Creating mount list for mounting subdir mount and volume
        self.subdir_mounts = [copy.deepcopy(self.mounts[0]),
                              copy.deepcopy(self.mounts[1])]
        self.subdir_mounts[0].volname = "%s/d1" % self.volname

        # Mount Subdirectory d1 on client 1 and volume on client 2
        for mount_obj in self.subdir_mounts:
            ret = mount_obj.mount()
            self.assertTrue(ret, ("Failed to mount  %s on client"
                                  " %s" % (mount_obj.volname,
                                           mount_obj.client_system)))
            g.log.info("Successfully mounted %s on client %s",
                       mount_obj.volname,
                       mount_obj.client_system)
        g.log.info("Successfully mounted sub directory and volume to "
                   "authenticated clients")

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

        # Set quota-soft-timeout to 0
        g.log.info("Setting up soft timeout to 0")
        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, "0")
        self.assertEqual(ret, 0, ("Failed to set quota-soft-timeout"))
        g.log.info("Successfully set the quota-soft-timeout")

        # Set quota-hard-timeout to 0
        g.log.info("Setting up hard timeout with 0")
        ret, _, _ = quota_set_hard_timeout(self.mnode, self.volname, "0")
        self.assertEqual(ret, 0, ("Failed to set quota-hard-timeout"))
        g.log.info("successfully set the quota-hard-timeout")

        # Set Quota object limit on the subdir "d1" and on volume
        for mount_obj in self.subdir_mounts:
            if mount_obj.volname == "%s/d1" % self.volname:
                path1 = "/d1"
                limit = "50"
            else:
                path1 = "/"
                limit = "200"
            g.log.info("Set Quota Limit on the path %s of the volume %s",
                       path1, self.volname)
            ret, _, _ = quota_limit_objects(self.mnode, self.volname,
                                            path1, limit)
            self.assertEqual(ret, 0, ("Failed to set quota limit on path "
                                      "%s of the volume %s",
                                      path1, self.volname))
            g.log.info("Successfully set the quota limit on %s of the volume "
                       "%s", path1, self.volname)

        # Create near to 49 files on both subdir mount and volume mount
        for mount_object in self.subdir_mounts:
            g.log.info("Creating Files on %s:%s", mount_object.client_system,
                       mount_object.mountpoint)
            cmd = ("cd %s ; for i in `seq 1 49` ;"
                   "do touch $i;done "
                   % (mount_object.mountpoint))
            ret, _, _ = g.run(mount_object.client_system, cmd)
            self.assertEqual(ret, 0, "Failed to create files on mountpoint")
            g.log.info("Files created successfully on mountpoint")

        # Fetch Quota List object on the volume
        g.log.info("Get Quota list on the volume %s",
                   self.volname)
        quota_list = quota_fetch_list_objects(self.mnode, self.volname)

        self.assertIsNotNone(quota_list, ("Failed to get the quota list "
                                          "of the volume %s",
                                          self.volname))

        # Create 1 file on subdir to check if quota limit is
        # adhere by subdir d1
        g.log.info("Creating File on %s:%s", self.clients[0],
                   self.subdir_mounts[0].mountpoint)
        cmd = ("cd %s ; touch test "
               % (self.subdir_mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertNotEqual(ret, 0, ("File creation was expected to Fail."
                                     "But it got passed"))
        g.log.info("File creation failed as expected on %s:%s as quota"
                   " limit reached already",
                   self.clients[0], self.subdir_mounts[0].mountpoint)

        # Modify quota object limit for subdir from 50 to 75
        path1 = "/d1"
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path1, self.volname)
        ret, _, _ = quota_limit_objects(self.mnode, self.volname,
                                        path1, "75")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  " the volume %s", path1, self.volname))
        g.log.info("Successfully set the quota limit on %s of the volume "
                   "%s", path1, self.volname)

        # Create near to 25 directories on both subdir mount "d1" and volume
        for mount_object in self.subdir_mounts:
            g.log.info("Creating directories on %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            for i in range(0, 25):
                ret = mkdir(mount_object.client_system, "%s/dir%s"
                            % (mount_object.mountpoint, i), parents=True)
                self.assertTrue(ret, "Failed to create directories"
                                     "on mountpoint")
                g.log.info("Directories created successfully on mountpoint")

        # Get Quota List on the volume
        g.log.info("Get Quota list on the volume %s",
                   self.volname)
        quota_list = quota_fetch_list_objects(self.mnode, self.volname)
        self.assertIsNotNone(quota_list, ("Failed to get the quota list "
                                          "of the volume %s",
                                          self.volname))

        # Create 1 directory on subdir "d1" and volume to check if quota
        # limit is adhere by subdir d1 and volume
        for mount_object in self.subdir_mounts:
            g.log.info("Creating directory on %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            ret = mkdir(mount_object.client_system, "%s/dirTest"
                        % mount_object.mountpoint, parents=True)
            if mount_object.volname == "%s/d1" % self.volname:
                self.assertFalse(ret, "Directory creation was expected"
                                      "to Fail.But it got passed")
                g.log.info("Direction creation failed as expected on"
                           "subdir d1")
            else:
                self.assertTrue(ret, "Directory creation got failed"
                                     "on volume")
                g.log.info("Direction creation successful  on volume")

    def tearDown(self):
        """
        Clean up the volume and umount volume from client
        """
        # Unmount sub-directory and volume
        # Test needs to continue if  unmount fail.Not asserting here.
        ret = self.unmount_volume(self.subdir_mounts)
        if ret:
            g.log.info("Successfully unmounted the subdir and volume")
        else:
            g.log.error("Failed to unmount volume or subdir")

        # cleanup-volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Cleanup volume %s Completed Successfully", self.volname)
