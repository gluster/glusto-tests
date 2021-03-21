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
# GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Description:
    Testing Reserve limit in GlusterD
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (
    cleanup_volume,
    setup_volume)
from glustolibs.gluster.volume_ops import (
    set_volume_options,
    get_volume_list)
from glustolibs.gluster.mount_ops import (
    mount_volume,
    umount_volume)
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.io.utils import wait_for_io_to_complete
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestGlusterdDirWithinReserveLimit(GlusterBaseClass):
    """ Testing directory creation within reserve limit """

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        self.all_mounts_procs = []

        # Upload IO script for running IO on mounts
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                   "file_dir_ops.py")
        ret = upload_scripts(self.mounts[0].client_system,
                             self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to client")

    def _get_space_on_mount(self):
        # Getting the available space on mount.
        cmd = ("df --output=avail %s | grep '[0-9]'"
               % self.mounts[0].mountpoint)
        ret, out, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)
        return int(out)

    def test_glusterd_dir_creation_within_reserve_limit(self):
        """
        Test Glusterd create dir within reserve limit.
        1. Create a Distributed-replicated volume and start it.
        2. Enable storage.reserve limit on the created volume.
        3. Mount the volume on a client.
        4. Create files on mountpoint under the reserve limit.
        5. Create a directory and listing should show the directory.
        """
        # Create and start a volume
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")

        # Getting the list of bricks.
        self.brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(self.brick_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        # Setting storage.reserve to 99
        ret = set_volume_options(self.mnode, self.volname,
                                 {'storage.reserve': '99'})
        self.assertTrue(ret, "Failed to set storage reserve on %s"
                        % self.mnode)
        g.log.info("Storage reserve set successfully on %s", self.mnode)

        # Mounting the volume.
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume %s is not mounted") % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)

        # Computing number of files to be created.
        file_count = round(self._get_space_on_mount()*0.1)

        # Do some IO till the reserve limit is breached.
        cmd = (
            "/usr/bin/env python %s create_files "
            "-f %d --fixed-file-size 1k --base-file-name file %s"
            % (self.script_upload_path, file_count, self.mounts[0].mountpoint))
        proc = g.run_async(
            self.mounts[0].client_system, cmd, user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)

        # Wait for IO to complete
        self.assertTrue(wait_for_io_to_complete(self.all_mounts_procs,
                                                self.mounts[0]),
                        "IO failed as reserve limit is breached.")
        g.log.info("IO succeeded.")

        # Create a directory at mountpoint.
        cmd = ("mkdir -p %s/dir1" % (self.mounts[0].mountpoint))
        ret, out, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)

        # Do listing to get dir1 at mountpoint.
        cmd = ("ls %s | grep dir1 | wc -l" % (self.mounts[0].mountpoint))
        ret, out, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)
        self.assertEqual(int(out), 1, ("dir1 not created in the mountpoint."))

    def tearDown(self):
        """ tear Down Callback """
        # Unmount volume
        ret = umount_volume(mclient=self.mounts[0].client_system,
                            mpoint=self.mounts[0].mountpoint)
        if not ret:
            raise ExecutionError("Failed to Unmount the volume %s"
                                 % self.volname)
        g.log.info("Successful in Unmount of volume : %s", self.volname)

        # Cleanup the volume
        vol_list = get_volume_list(self.mnode)
        if not vol_list:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume %s"
                                     % self.volname)
            g.log.info("Volume deleted successfully : %s", volume)

        # Cleaning up the deleted volume bricks
        for brick in self.brick_list:
            node, brick_path = brick.split(r':')
            cmd = "rm -rf " + brick_path
            ret, _, _ = g.run(node, cmd)
            if ret:
                raise ExecutionError("Failed to delete the brick "
                                     "dirs of deleted volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
