#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""
    Description:
    A testcase to remove /var/log/glusterfs/ on client, mounting a volume
    and createing a file and a dir on it.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.glusterdir import (mkdir, get_dir_contents)
from glustolibs.gluster.mount_ops import mount_volume, umount_volume


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestRemoveCientLogDirAndMount(GlusterBaseClass):

    def setUp(self):
        GlusterBaseClass.setUp.im_func(self)

        # Creating Volume and mounting volume.
        ret = self.setup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)
        g.log.info("Volme created and mounted successfully : %s",
                   self.volname)

    def tearDown(self):

        # Resetting the /var/log/glusterfs on client
        # and archiving the present one.
        cmd = ('for file in `ls /var/log/glusterfs/`; do '
               'mv /var/log/glusterfs/$file'
               ' /var/log/glusterfs/`date +%s`-$file; done')
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Renaming all files failed")
        g.log.info("Successfully renamed files in"
                   " /var/log/glusterfs on client: %s",
                   self.mounts[0].client_system)
        cmd = ('mv /root/glusterfs/* /var/log/glusterfs/;'
               'rm -rf /root/glusterfs')
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0,
                         "Failed to move old files back to /var/log/glusterfs")
        g.log.info("Successfully moved files in"
                   " /var/log/glusterfs on client: %s",
                   self.mounts[0])

        # Unmounting the volume.
        ret, _, _ = umount_volume(mclient=self.mounts[0].client_system,
                                  mpoint=self.mounts[0].mountpoint)
        if ret:
            raise ExecutionError("Volume %s is not unmounted" % self.volname)
        g.log.info("Volume unmounted successfully : %s", self.volname)

        # clean up all volumes
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Unable to delete volume % s"
                                 % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        GlusterBaseClass.tearDown.im_func(self)

    def test_mount_after_removing_client_logs_dir(self):

        # pylint: disable=too-many-statements
        """
        Test Case:
        1. Create all types of volumes.
        2. Start all volumes.
        3. Delete /var/log/glusterfs folder on the client.
        4. Mount all the volumes one by one.
        5. Run IO on all the mount points.
        6. Check if logs are generated in /var/log/glusterfs/.
        """

        # Removing dir /var/log/glusterfs on client.
        cmd = 'mv /var/log/glusterfs /root/'
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Unable to remove /var/log/glusterfs dir")
        g.log.info("Successfully removed /var/log/glusterfs on client: %s",
                   self.mounts[0].client_system)

        # Mounting the volume.
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume %s is not mounted") % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)

        # Running IO on the mount point.
        # Creating a dir on the mount point.
        ret = mkdir(self.mounts[0].client_system,
                    self.mounts[0].mountpoint+"/dir2")
        self.assertTrue(ret, "Failed to create dir2")
        g.log.info("dir2 created successfully for %s", self.mounts[0])

        # Creating a file on the mount point.
        cmd = ('touch  %s/file' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create file")
        g.log.info("file created successfully for %s", self.mounts[0])

        # Checking if logs are regenerated or not.
        ret = get_dir_contents(self.mounts[0].client_system,
                               '/var/log/glusterfs/')
        self.assertIsNotNone(ret, 'Log files were not regenerated.')
        g.log.info("Log files were properly regenearted.")
