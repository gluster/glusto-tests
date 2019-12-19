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
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.glusterfile import get_file_stat


@runs_on([['replicated'],
          ['glusterfs']])
class AfrReadlinkTest(GlusterBaseClass):
    """
    Description:
        Test Case which tests the working of FOPS in AFR.
    """
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Override replica count to be 3
        if cls.volume_type == "replicated":
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp'}

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        self.bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(self.bricks_list, "unable to get list of bricks")

    def tearDown(self):
        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_readlink(self):
        # create file
        g.log.info("Creating %s/file.txt", self.mounts[0].mountpoint)
        cmd = ("echo 'hello_world' > %s/file.txt" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "File creation failed")
        g.log.info("Created %s/file.txt", self.mounts[0].mountpoint)

        # create symlink
        g.log.info("Creating %s/symlink.txt to %s/file.txt",
                   self.mounts[0].mountpoint, self.mounts[0].mountpoint)
        cmd = ("ln -s file.txt %s/symlink.txt" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "symlink creation failed")
        g.log.info("Created %s/symlink.txt to %s/file.txt",
                   self.mounts[0].mountpoint, self.mounts[0].mountpoint)

        # stat symlink on mount and verify file type and permission.
        g.log.info("Checking file permissions")
        path = ("%s/symlink.txt" % self.mounts[0].mountpoint)
        stat_dict = get_file_stat(self.clients[0], path)
        self.assertEqual(stat_dict['filetype'], 'symbolic link', "Expected "
                         "symlink but found %s" % stat_dict['filetype'])
        self.assertEqual(stat_dict['access'], '777', "Expected 777 "
                         "but found %s" % stat_dict['access'])
        g.log.info("File permissions for symlink.txt is 777 as expected")

        # readlink to verify contents
        g.log.info("Performing readlink on %s/symlink.txt",
                   self.mounts[0].mountpoint)
        cmd = ("readlink %s/symlink.txt" % self.mounts[0].mountpoint)
        _, val, _ = g.run(self.clients[0], cmd)
        content = val.strip()
        self.assertEqual(content, "file.txt", "Readlink error:got %s"
                         % content)
        g.log.info("readlink returned 'file.txt' as expected")

        # stat symlink on bricks and verify file type and permission.
        g.log.info("Checking file type and permissions on bricks")
        for brick in self.bricks_list:
            node, path = brick.split(':')
            filepath = path + "/symlink.txt"
            stat_dict = get_file_stat(node, filepath)
            self.assertEqual(stat_dict['filetype'], 'symbolic link', "Expected"
                             " symlink but found %s" % stat_dict['filetype'])
            g.log.info("file permission 777 for symlink.txt on %s", brick)
