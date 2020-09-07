#  Copyright (C) 2018-2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.io.utils import \
     compare_dir_structure_mount_with_brick as compare_dir_structure
from glustolibs.gluster.lib_utils import (add_user, del_user)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (
    list_all_files_and_dirs_mounts,
    wait_for_io_to_complete
)


@runs_on([['distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'replicated'],
          ['glusterfs']])
class DirChangePermRecursive(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

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

    def test_dir_change_perm_recursive(self):
        # pylint: disable=too-many-statements
        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for index, mount_obj in enumerate(self.mounts, start=1):
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path,
                       index + 10, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Wait for IO to complete
        g.log.info("Wait for IO to complete as IO validation did not "
                   "succeed in test method")
        ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on mount %s", self.clients[0])

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
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

        mount_obj = self.mounts[0]
        cmd = ("find %s -mindepth 1 -maxdepth 1 -type d | "
               "xargs chown -R test_user1" % (mount_obj.mountpoint))
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

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1"
               " -type d\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("directory is successfully accessed with different user")

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1"
               " -type d | xargs chmod 777\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertNotEqual(rcode, 0, err)
        g.log.info("directory permission are not changed by different user")

        cmd = ("find %s -mindepth 1 -maxdepth 1 -type d | "
               "xargs chgrp -R test_user1" % (mount_obj.mountpoint))
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

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1"
               " -type d\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("directory is successfully accessed with different user")

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1 -type d "
               "| xargs chmod 777\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertNotEqual(rcode, 0, err)
        g.log.info("directory permission are not changed by different user")

        cmd = ("find %s -mindepth 1 -maxdepth 1 -type d | xargs chmod -R 777"
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

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1"
               " -type d\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertEqual(rcode, 0, err)
        g.log.info("directory is successfully accessed with different user")

        cmd = ("su -l test_user2 -c \"find %s -mindepth 1"
               " -type d | xargs chmod 666\"" % (mount_obj.mountpoint))
        rcode, _, err = g.run(mount_obj.client_system, cmd)
        self.assertNotEqual(rcode, 0, err)
        g.log.info("directory permission are not changed by different user")
