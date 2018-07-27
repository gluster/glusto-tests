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
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class MultipleDirMappingClient(GlusterBaseClass):
    """
    Test case validates multiple dirs mapping to multiple client
    having access to mount respective dirs,with auth allow functionality
    """
    @classmethod
    def setUpClass(cls):
        """
        setup volume and mount volume
        calling GlusterBaseClass setUpClass
        """
        GlusterBaseClass.setUpClass.im_func(cls)
        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup and Mount Volume %s",
                   cls.volname)
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume "
                                 "and Mount_Volume %s" % cls.volname)
        g.log.info("Successful in Setup and Mount Volume %s", cls.volname)

    def test_subdir_mounts(self):
        """
        Mount the volume on client
        Create 2 subdir on client subdir1,subdir2
        Auth allow - Client1(subdir1),Client2(subdir2)
        Mount subdir1 on client1.Try Mounting subdir1 on client2
        Mount subdir2 on client2.Try Mounting subdir2 on client1
        """
        # Create 2 subdirectories
        cmd = ("mkdir %s/dir1 && mkdir %s/dir2"
               % (self.mounts[0].mountpoint,
                  self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create directories on mountpoint")
        g.log.info("Directories created successfully on mountpoint")

        # unmount volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Volumes UnMount failed")
        g.log.info("Volumes UnMounted successfully")

        # Set auth allow permission on subdirs
        cmd = ("gluster volume set %s auth.allow '/dir1(%s),/dir2(%s)'"
               % (self.volname, self.clients[0], self.clients[1]))
        g.run(self.mnode, cmd)

        # Sometimes the mount command is returning exit code as 0 in case of
        # mount failures as well
        # Hence not asserting while running mount command in test case.
        # Instead asserting on basis on performing grep on mount point
        # BZ 1590711

        self.mpoint = "/mnt/Mount_Point1"

        # Test Subdir2 mount on client 1
        _, _, _ = mount_volume("%s/dir2" % self.volname, self.mount_type,
                               self.mpoint, self.mnode, self.clients[0])
        cmd = ("mount | grep %s") % self.mpoint
        ret, _, _ = g.run(self.clients[0], cmd)
        if ret == 0:
            raise ExecutionError("%s/dir2 mount should fail,"
                                 "But dir2 mounted successfully on"
                                 "unauthorized client" % self.volname)
        g.log.info("%s/dir2 is not mounted on unauthorized client",
                   self.volname)
        # Test Subdir1 mount on client 1
        _, _, _ = mount_volume("%s/dir1" % self.volname, self.mount_type,
                               self.mpoint, self.mnode, self.clients[0])
        cmd = ("mount | grep %s") % self.mpoint
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, ("%s/dir1 mount failed"
                                  % self.volname))
        g.log.info("%s/dir1 is mounted Successfully", self.volname)
        # Test Subdir1 mount on client 2
        _, _, _ = mount_volume("%s/dir1" % self.volname, self.mount_type,
                               self.mpoint, self.mnode, self.clients[1])
        cmd = ("mount | grep %s") % self.mpoint
        ret, _, _ = g.run(self.clients[1], cmd)
        if ret == 0:
            raise ExecutionError("%s/dir1 mount should fail,"
                                 "But dir1 mounted successfully on"
                                 "unauthorized client" % self.volname)
        g.log.info("%s/dir1 is not mounted on unauthorized client",
                   self.volname)
        # Test Subdir2 mount on client 2
        _, _, _ = mount_volume("%s/dir2" % self.volname, self.mount_type,
                               self.mpoint, self.mnode, self.clients[1])
        cmd = ("mount | grep %s") % self.mpoint
        ret, _, _ = g.run(self.clients[1], cmd)
        self.assertEqual(ret, 0, ("%s/dir2 mount failed" % self.volname))
        g.log.info("%s/dir2 is mounted Successfully", self.volname)

    def tearDown(self):
        """
        Clean up the volume and umount volume from client
        """
        # Unmount Volume from client
        g.log.info("Starting to Unmount volume")
        for client in self.clients:
            ret, _, _ = umount_volume(client, self.mpoint,
                                      self.mount_type)
            if ret == 1:
                raise ExecutionError("Unmounting the mount point %s failed"
                                     % self.mpoint)
            g.log.info("Unmount Volume Successful")
            cmd = ("rm -rf %s") % self.mpoint
            ret, _, _ = g.run(client, cmd)
            g.log.info("Mount point %s deleted successfully", self.mpoint)

        # cleanup-volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Cleanup volume %s Completed Successfully", self.volname)
