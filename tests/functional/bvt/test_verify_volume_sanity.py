#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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

"""
Description:
    This test verifies that file/directory creation or
    deletion doesn't leave behind any broken symlink on bricks.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.brick_libs import is_broken_symlinks_present_on_bricks
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed',
           'arbiter', 'distributed-arbiter'],
          ['glusterfs', 'nfs']])
class VerifyVolumeSanity(GlusterBaseClass):

    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        self.assertTrue(ret, ("Failed to Setup_Volume and Mount_Volume"))

    def test_volume_sanity(self):
        """
        Test case:
        1.Create 10 files and directories.
        2.Check if broken symlinks are present on brick path.
        3.Remove files and directories.
        4.Check if broken symlinks are present on brick path.
        """
        client = self.mounts[0].client_system
        mountpoint = self.mounts[0].mountpoint

        # Create some directories and files
        ret, _, _ = g.run(client, "mkdir %s/folder{1..10}" % mountpoint)
        self.assertFalse(ret, ("Failed to create directories on volume %s",
                               self.volname))
        ret, _, _ = g.run(client, "touch %s/folder{1..10}/file{1..10}" %
                          mountpoint)
        self.assertFalse(ret, ("Failed to create files on volume %s",
                               self.volname))
        g.log.info("Successfully created files and directories.")

        # Verify broken symlink on the bricks
        ret = is_broken_symlinks_present_on_bricks(self.mnode, self.volname)
        self.assertTrue(ret, "Error: Broken symlink found on brick paths")
        g.log.info("No broken symlinks found before deleting files and dirs.")

        # Delete the mountpoint contents
        ret, _, _ = g.run(client, "rm -rf %s/*" % mountpoint)
        self.assertFalse(ret, ("Failed to remove data from volume %s",
                               self.volname))
        g.log.info("Successfully removed all files and directories.")

        # Verify broken symlink on the bricks
        ret = is_broken_symlinks_present_on_bricks(self.mnode, self.volname)
        self.assertTrue(ret, "Error: Broken symlink found on brick paths")
        g.log.info("No broken symlinks found after deleting files and dirs.")

    def tearDown(self):

        # Stopping the volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        self.assertTrue(ret, ("Failed to Unmount Volume and Cleanup Volume"))

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
