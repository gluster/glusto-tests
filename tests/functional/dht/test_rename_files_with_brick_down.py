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


from random import choice
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import get_all_bricks, bring_bricks_offline
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.gluster.volume_libs import get_volume_type
from glustolibs.gluster.glusterfile import create_link_file


@runs_on([['replicated', 'arbiter',
           'distributed', 'distributed-arbiter',
           'distributed-replicated'],
          ['glusterfs']])
class TestRenameFilesBrickDown(GlusterBaseClass):

    # pylint: disable=too-many-statements
    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError("Failed to Setup and Mount Volume")

    def tearDown(self):

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_rename_files_with_brick_down(self):
        """
        Description: Tests to check that there is no data loss when rename is
                      performed with a brick of volume down.
         Steps :
         1) Create a volume.
         2) Mount the volume using FUSE.
         3) Create 1000 files on the mount point.
         4) Create the soft-link for file{1..100}
         5) Create the hard-link for file{101..200}
         6) Check for the file count on the mount point.
         7) Begin renaming the files, in multiple iterations.
         8) Let few iterations of the rename complete successfully.
         9) Then while rename is still in progress, kill a brick part of the
            volume.
         10) Let the brick be down for sometime, such that the a couple
             of rename iterations are completed.
         11) Bring the brick back online.
         12) Wait for the IO to complete.
         13) Check if there is any data loss.
         14) Check if all the files are renamed properly.
         """
        # Creating 1000 files on volume root
        m_point = self.mounts[0].mountpoint
        command = 'touch ' + m_point + '/file{1..1000}_0'
        ret, _, _ = g.run(self.clients[0], command)
        self.assertEqual(ret, 0, "File creation failed on %s"
                         % m_point)
        g.log.info("Files successfully created on the mount point")

        # Create soft links for a few files
        for i in range(1, 100):
            ret = create_link_file(self.clients[0],
                                   '{}/file{}_0'.format(m_point, i),
                                   '{}/soft_link_file{}_0'.format(m_point, i),
                                   soft=True)
            self.assertTrue(ret, "Failed to create soft links for files")
        g.log.info("Created soft links for files successfully")

        # Create hard links for a few files
        for i in range(101, 200):
            ret = create_link_file(self.clients[0],
                                   '{}/file{}_0'.format(m_point, i),
                                   '{}/hard_link_file{}_0'.format(m_point, i),
                                   soft=False)
            self.assertTrue(ret, "Failed to create hard links for files")
        g.log.info("Created hard links for files successfully")

        # Calculate file count for the mount-point
        cmd = ("ls -lR %s/ | wc -l" % m_point)
        ret, count_before, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to get file count")
        g.log.info("File count before rename is:%s", count_before)

        # Start renaming the files in multiple iterations
        g.log.info("Starting to rename the files")
        all_mounts_procs = []
        cmd = ('for i in `seq 1 1000`; do for j in `seq 0 5`;do mv -f '
               '%s/file$i\\_$j %s/file$i\\_$(expr $j + 1); done; done'
               % (m_point, m_point))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Waiting for some time for a iteration of rename to complete
        g.log.info("Waiting for few rename iterations to complete")
        sleep(120)

        # Get the information about the bricks part of the volume
        brick_list = get_all_bricks(self.mnode, self.volname)

        # Kill a brick part of the volume
        ret = bring_bricks_offline(self.volname, choice(brick_list))
        self.assertTrue(ret, "Failed to bring brick offline")
        g.log.info("Successfully brought brick offline")

        # Let the brick be down for some time
        g.log.info("Keeping brick down for few minutes")
        sleep(60)

        # Bring the brick online using gluster v start force
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, "Volume start with force failed")
        g.log.info("Volume start with force successful")

        # Close connection and check if rename has completed
        ret, _, _ = proc.async_communicate()
        self.assertEqual(ret, 0, "Rename is not completed")
        g.log.info("Rename is completed")

        # Do lookup on the files
        # Calculate file count from mount
        cmd = ("ls -lR %s/ | wc -l" % m_point)
        ret, count_after, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to do lookup and"
                         "get file count")
        g.log.info("Lookup successful. File count after"
                   " rename is:%s", count_after)

        # Check if there is any data loss
        self.assertEqual(int(count_before), int(count_after),
                         "The file count before and after"
                         " rename is not same. There is data loss.")
        g.log.info("The file count before and after rename is same."
                   " No data loss occurred.")

        # Checking if all files were renamed Successfully
        ret = get_volume_type(brick_list[0] + "/")
        if ret in ("Replicate", "Disperse", "Arbiter", "Distributed-Replicate",
                   "Distribute-Disperse", "Distribute-Arbiter"):
            cmd = ("ls -lR %s/file*_6 | wc -l" % m_point)
            ret, out, _ = g.run(self.clients[0], cmd)
            self.assertEqual(int(out), 1000, "Rename failed on some files")
            g.log.info("All the files are renamed successfully")
