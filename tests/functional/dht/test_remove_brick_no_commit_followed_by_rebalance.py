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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.io.utils import collect_mounts_arequal, validate_io_procs
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.volume_libs import (form_bricks_list_to_remove_brick,
                                            expand_volume)
from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)


@runs_on([['distributed', 'distributed-replicated',
           'distributed-dispersed', 'distributed-arbiter'],
          ['glusterfs']])
class TestRemoveBrickNoCommitFollowedByRebalance(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup and Mount Volume")

    def tearDown(self):

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_remove_brick_no_commit_followed_by_rebalance(self):
        """
        Description: Tests to check that there is no data loss when
                     remove-brick operation is stopped and then new bricks
                     are added to the volume.
         Steps :
         1) Create a volume.
         2) Mount the volume using FUSE.
         3) Create files and dirs on the mount-point.
         4) Calculate the arequal-checksum on the mount-point
         5) Start remove-brick operation on the volume.
         6) While migration is in progress, stop the remove-brick
            operation.
         7) Add-bricks to the volume and trigger rebalance.
         8) Wait for rebalance to complete.
         9) Calculate the arequal-checksum on the mount-point.
         """
        # Start IO on mounts
        m_point = self.mounts[0].mountpoint
        cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
               "--dir-length 10 --dir-depth 2 --max-num-of-dirs 1 "
               "--num-of-files 50 --file-type empty-file %s" % (
                   self.script_upload_path, m_point))
        proc = g.run_async(self.mounts[0].client_system,
                           cmd, user=self.mounts[0].user)
        g.log.info("IO on %s:%s is started successfully",
                   self.mounts[0].client_system, m_point)

        # Validate IO
        self.assertTrue(
            validate_io_procs([proc], self.mounts[0]),
            "IO failed on some of the clients"
        )

        # Calculate arequal-checksum before starting remove-brick
        ret, arequal_before = collect_mounts_arequal(self.mounts[0])
        self.assertTrue(ret, "Collecting arequal-checksum failed")

        # Form bricks list for volume shrink
        remove_brick_list = form_bricks_list_to_remove_brick(
            self.mnode, self.volname, subvol_name=1)
        self.assertIsNotNone(remove_brick_list, ("Volume %s: Failed to "
                                                 "form bricks list for "
                                                 "shrink", self.volname))
        g.log.info("Volume %s: Formed bricks list for shrink", self.volname)

        # Shrink volume by removing bricks
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list, "start")
        self.assertEqual(ret, 0, ("Volume %s shrink failed ",
                                  self.volname))
        g.log.info("Volume %s shrink started ", self.volname)

        # Log remove-brick status
        ret, out, _ = remove_brick(self.mnode, self.volname,
                                   remove_brick_list, "status")
        self.assertEqual(ret, 0, ("Remove-brick status failed on %s ",
                                  self.volname))

        # Check if migration is in progress
        if r'in progress' in out:
            # Stop remove-brick process
            g.log.info("Stop removing bricks from volume")
            ret, out, _ = remove_brick(self.mnode, self.volname,
                                       remove_brick_list, "stop")
            self.assertEqual(ret, 0, "Failed to stop remove-brick process")
            g.log.info("Stopped remove-brick process successfully")
        else:
            g.log.error("Migration for remove-brick is complete")

        # Sleep for 30 secs so that any running remove-brick process stops
        sleep(30)

        # Add bricks to the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Volume %s: Add-brick failed", self.volname))
        g.log.info("Volume %s: Add-brick successful", self.volname)

        # Tigger rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Volume %s: Failed to start rebalance",
                                  self.volname))
        g.log.info("Volume %s: Rebalance started ", self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, "Rebalance has not completed")
        g.log.info("Rebalance has completed successfully")

        # Calculate arequal-checksum on mount-point
        ret, arequal_after = collect_mounts_arequal(self.mounts[0])
        self.assertTrue(ret, "Collecting arequal-checksum failed")

        # Check if there is any data loss
        self.assertEqual(set(arequal_before), set(arequal_after),
                         ("There is data loss"))
        g.log.info("The checksum before and after rebalance is same."
                   " There is no data loss.")
