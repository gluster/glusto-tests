#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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

from glustolibs.gluster.constants import \
    TEST_LAYOUT_IS_COMPLETE as LAYOUT_IS_COMPLETE
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.io.utils import list_all_files_and_dirs_mounts
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.io.utils import \
     compare_dir_structure_mount_with_brick as compare_dir_structure
from glustolibs.gluster.lib_utils import (add_user, del_user)


@runs_on([['distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'replicated'],
          ['glusterfs']])
class DirChangePerm(GlusterBaseClass):
    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        brick_list = get_all_bricks(self.mnode, self.volname)
        # Add user on all nodes
        for brick in brick_list:
            brick_node, _ = brick.split(":")
            add_user(brick_node, "test_user1")
            add_user(brick_node, "test_user2")

        for mount_obj in self.mounts:
            add_user(mount_obj.client_system, "test_user1")
            add_user(mount_obj.client_system, "test_user2")

    def tearDown(self):

        brick_list = get_all_bricks(self.mnode, self.volname)
        for brick in brick_list:
            brick_node, _ = brick.split(":")
            del_user(brick_node, "test_user1")
            del_user(brick_node, "test_user2")

        for mount_obj in self.mounts:
            del_user(mount_obj.client_system, "test_user1")
            del_user(mount_obj.client_system, "test_user2")

        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_dir_change_perm(self):
        # pylint: disable=too-many-statements
        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        mount_obj = self.mounts[0]
        cmd = ('cd %s ; mkdir testdir; '
               'mkdir -p testdir/dir{1..10} '
               'touch testdir/file{1..10}') % (mount_obj.mountpoint)
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("IO is successful on mount %s", self.clients[0])

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(mount_obj)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

        # DHT Layout validation
        g.log.debug("Verifying hash layout values %s:%s",
                    self.clients[0], self.mounts[0].mountpoint)
        ret = validate_files_in_dir(self.clients[0], self.mounts[0].mountpoint,
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "layout is complete: FAILED")
        g.log.info("layout is complete: PASS")

        brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(brick_list, "Failed to get brick list")
        g.log.info("Successful in getting brick list %s", brick_list)

        cmd = ("find %s -mindepth 1 -maxdepth 1 -type d | "
               "xargs chown test_user1" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("Change user owner successfully for testdir on %s",
                   mount_obj.client_system)

        retval = compare_dir_structure(mount_obj.client_system,
                                       mount_obj.mountpoint,
                                       brick_list, 0)
        self.assertTrue(retval, "Failed to compare user permission for all"
                        " files/dir in mount directory with brick directory")
        g.log.info("User permission is same on mount and brick directory")

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1 -maxdepth 1"
               " -type d\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("directory is successfully accessed with different user")

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1 -maxdepth 1"
               " -type d | xargs chmod 777\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertNotEqual(rcode, 0, err)
        g.log.info("directory permission are not changed by different user")

        cmd = ("find %s -mindepth 1 -maxdepth 1 -type d | "
               "xargs chgrp test_user1" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("Change group owner successfully for testdir on %s",
                   mount_obj.client_system)

        retval = compare_dir_structure(mount_obj.client_system,
                                       mount_obj.mountpoint,
                                       brick_list, 1)
        self.assertTrue(retval, "Failed to compare group permission for all"
                        " files/dir in mount directory with brick directory")
        g.log.info("Group permission is same on mount and brick directory")

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1 -maxdepth 1"
               " -type d\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("directory is successfully accessed with different user")

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1 -maxdepth 1 -type d "
               "| xargs chmod 777\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertNotEqual(rcode, 0, err)
        g.log.info("directory permission are not changed by different user")

        cmd = ("find %s -mindepth 1 -maxdepth 1 -type d | xargs chmod 777"
               % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("Change permission 777 successfully for testdir on %s",
                   mount_obj.client_system)

        retval = compare_dir_structure(mount_obj.client_system,
                                       mount_obj.mountpoint,
                                       brick_list, 2)
        self.assertTrue(retval, "Failed to compare permission for all"
                        " files/dir in mount directory with brick directory")
        g.log.info("Permission is same on mount and brick directory")

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1 -maxdepth 1"
               " -type d\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("directory is successfully accessed with different user")

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1 -maxdepth 1"
               " -type d | xargs chmod 666\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertNotEqual(rcode, 0, err)
        g.log.info("directory permission are not changed by different user")
