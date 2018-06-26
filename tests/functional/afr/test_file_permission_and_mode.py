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
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterfile import get_file_stat
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['replicated'],
          ['glusterfs']])
class FileModeAndPermissionsTest(GlusterBaseClass):
    """
     Description:
    """
    @classmethod
    def setUpClass(cls):

        GlusterBaseClass.setUpClass.im_func(cls)

        # Override Volumes
        if cls.volume_type == "replicated":
            # Define 1x3 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp'}

    def setUp(self):
        GlusterBaseClass.setUp.im_func(self)

        for mount_object in self.mounts:
            # Create user qa
            g.log.info("Creating user 'qa'...")
            command = "useradd qa"
            ret, _, err = g.run(mount_object.client_system, command)

            if 'already exists' in err:
                g.log.warn("User 'qa' is already exists")
            else:
                g.log.info("User 'qa' is created successfully")

        g.log.info("Starting to Setup Volume %s", self.volname)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        self.bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(self.bricks_list, "unable to get list of bricks")

    def tearDown(self):
        # Deleting the user which was created in setUp
        for mount_object in self.mounts:
            # Delete user
            g.log.info('Deleting user qa...')
            command = "userdel -r qa"
            ret, _, err = g.run(mount_object.client_system, command)

            if 'does not exist' in err:
                g.log.warn('User qa is already deleted')
            else:
                g.log.info('User qa successfully deleted')
        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    def test_file_permissions(self):
        # create file
        fpath = self.mounts[0].mountpoint + "/file.txt"
        cmd = ("echo 'hello_world' > %s" % fpath)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "File creation failed")

        # check file is created on all bricks
        for brick in self.bricks_list:
            node, path = brick.split(':')
            filepath = path + "/file.txt"
            stat_dict = get_file_stat(node, filepath)
            self.assertIsNotNone(stat_dict, "stat on %s failed" % filepath)
            self.assertEqual(stat_dict['filetype'], 'regular file', "Expected"
                             " symlink but found %s" % stat_dict['filetype'])

        # get file stat info from client
        stat_dict = get_file_stat(self.clients[0], fpath)
        self.assertIsNotNone(stat_dict, "stat on %s failed" % fpath)
        self.assertEqual(stat_dict['uid'], '0', "Expected uid 0 but found %s"
                         % stat_dict['uid'])
        self.assertEqual(stat_dict['gid'], '0', "Expected gid 0 but found %s"
                         % stat_dict['gid'])
        self.assertEqual(stat_dict['access'], '644', "Expected permission 644 "
                         " but found %s" % stat_dict['access'])

        # change uid, gid and permission from client
        cmd = ("chown qa %s" % fpath)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "chown failed")

        cmd = ("chgrp qa %s" % fpath)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "chgrp failed")

        cmd = ("chmod 777 %s" % fpath)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "chown failed")

        # Verify that the changes are successful on client
        stat_dict = get_file_stat(self.clients[0], fpath)
        self.assertIsNotNone(stat_dict, "stat on %s failed" % fpath)
        self.assertEqual(stat_dict['uid'], '1000', "Expected uid 1000 (qa) but"
                         " found %s" % stat_dict['uid'])
        self.assertEqual(stat_dict['gid'], '1000', "Expected gid 1000 (qa) but"
                         " found %s" % stat_dict['gid'])
        self.assertEqual(stat_dict['access'], '777', "Expected permission 777 "
                         " but found %s" % stat_dict['access'])

        # Verify that the changes are successful on bricks as well
        for brick in self.bricks_list:
            node, path = brick.split(':')
            filepath = path + "/file.txt"
            stat_dict = get_file_stat(node, filepath)
            self.assertIsNotNone(stat_dict, "stat on %s failed" % fpath)
            self.assertEqual(stat_dict['uid'], '1000', "Expected uid 1000 (qa)"
                             " but found %s" % stat_dict['uid'])
            self.assertEqual(stat_dict['gid'], '1000', "Expected gid 1000 (qa)"
                             " but found %s" % stat_dict['gid'])
            self.assertEqual(stat_dict['access'], '777', "Expected permission"
                             " 777  but found %s" % stat_dict['access'])
