#  Copyright (C) 2020  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable=too-many-statements, undefined-loop-variable
# pylint: disable=too-many-branches,too-many-locals,pointless-string-statement

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
import glustolibs.gluster.constants as k
from glustolibs.gluster.glusterfile import get_fattr
from glustolibs.gluster.glusterdir import get_dir_contents
from glustolibs.gluster.rebalance_ops import (
    wait_for_rebalance_to_complete, rebalance_start)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.gluster.heal_libs import monitor_heal_completion


@runs_on([['distributed-replicated',
           'distributed-dispersed',
           'distributed-arbiter'],
          ['glusterfs']])
class TestCustomxattrsOnNewBricks(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    @classmethod
    def tearDownClass(cls):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        cls.get_super_method(cls, 'tearDownClass')()

    def check_xattr(self, list_of_all_dirs):
        """
        Check the custom xattr on backend bricks for the directories.

        Args:
        list_of_all_dirs(list): List of dirs created on mount.

        Returns:
        Success/failure msg.
        """
        for direc in list_of_all_dirs:
            for brick in get_all_bricks(self.mnode, self.volname):
                host, brick_path = brick.split(':')
                brick_dir_path = brick_path + '/' + direc
                ret = get_fattr(host, brick_dir_path, 'user.foo')
                self.assertIsNotNone(ret, "Custom xattr is not displayed on"
                                     " the backend bricks ")
                g.log.info("Custom xattr %s is displayed on the back-end"
                           " bricks", ret)

    def test_healing_of_custom_xattrs_on_newly_added_bricks(self):
        """
        Description: Tests to check that the custom xattrs are healed on the
                     dirs when new bricks are added
        Steps :
        1) Create a volume.
        2) Mount the volume using FUSE.
        3) Create 100 directories on the mount point.
        4) Set the xattr on the directories.
        5) Add bricks to the volume and trigger rebalance.
        6) Check if all the bricks have healed.
        7) After rebalance completes, check the xattr for dirs on the newly
           added bricks.
        """
        # pylint: disable=too-many-function-args

        # Creating 1000 directories on volume root
        m_point = self.mounts[0].mountpoint
        command = 'mkdir -p ' + m_point + '/dir{1..100}'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, ("Directory creation failed on %s",
                                  self.mounts[0].mountpoint))
        g.log.info("Directories created successfully.")

        # Lookup on the mount point
        command = 'ls ' + m_point + '/'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "ls failed on parent directory")
        g.log.info("ls on parent directory: successful")

        # Setting up the custom xattr for all the directories on mount point
        m_point = self.mounts[0].mountpoint
        command = 'setfattr -n user.foo -v "foobar" ' + m_point + '/dir*'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to set the xattr on the"
                         " directories")
        g.log.info("Successfully set custom xattr on the directories")

        # Checking the layout of the directories on the back-end
        flag = validate_files_in_dir(self.clients[0],
                                     m_point,
                                     test_type=k.TEST_LAYOUT_IS_COMPLETE)
        self.assertTrue(flag, "Layout has some holes or overlaps")
        g.log.info("Layout is completely set")

        # Creating a list of directories on the mount point
        list_of_all_dirs = get_dir_contents(self.mounts[0].client_system,
                                            m_point)
        self.assertNotEqual(list_of_all_dirs, None, "Creation of directory"
                            " list failed.")
        g.log.info("Creation of directory list is successful.")

        # Checking the custom xattr on backend bricks for the directories
        self.check_xattr(list_of_all_dirs)

        # Expanding volume by adding bricks to the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Volume %s: Expand failed", self.volname))
        g.log.info("Volume %s: Expand success", self.volname)

        # Start Rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Volume %s: Failed to start rebalance",
                                  self.volname))
        g.log.info("Volume %s: Rebalance start success", self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s: Rebalance failed to complete",
                              self.volname))
        g.log.info("Volume %s: Rebalance is completed", self.volname)

        # Lookup on the mount point
        command = 'ls -laR ' + m_point + '/'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "ls failed on parent directory")
        g.log.info("ls on parent directory: successful")

        # Check if all the bricks are healed
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=900)
        self.assertTrue(ret, ("Heal is not complete for all bricks"))
        g.log.info("Healing is complete for all the bricks")

        # Checking the custom xattrs for all the directories on
        # back end bricks after rebalance is complete
        self.check_xattr(list_of_all_dirs)
