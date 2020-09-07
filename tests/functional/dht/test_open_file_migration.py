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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.io.utils import open_file_fd
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete,
                                              get_rebalance_status)


@runs_on([['distributed', 'replicated', 'arbiter',
           'dispersed'],
          ['glusterfs']])
class TestOpenFileMigration(GlusterBaseClass):
    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    def tearDown(self):
        """
        Cleanup and umount volume
        """
        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

        # Cleanup and umount volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

    def test_open_file_migration(self):
        """
        Description: Checks that files with open fd are migrated successfully.

        Steps :
        1) Create a volume.
        2) Mount the volume using FUSE.
        3) Create files on volume mount.
        4) Open fd for the files and keep on doing read write operations on
           these files.
        5) While fds are open, add bricks to the volume and trigger rebalance.
        6) Wait for rebalance to complete.
        7) Wait for write on open fd to complete.
        8) Check for any data loss during rebalance.
        9) Check if rebalance has any failures.
        """
        # Create files and open fd for the files on mount point
        m_point = self.mounts[0].mountpoint
        cmd = ('cd {}; for i in `seq 261 1261`;do touch testfile$i;'
               'done'.format(m_point))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to create files")
        g.log.info("Successfully created files")
        proc = open_file_fd(m_point, 2, self.clients[0],
                            start_range=301, end_range=400)

        # Calculate file count for the mount-point
        cmd = ("ls -lR {}/testfile* | wc -l".format(m_point))
        ret, count_before, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to get file count")
        g.log.info("File count before rebalance is:%s", count_before)

        # Add bricks to the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume %s",
                              self.volname))
        g.log.info("Expanding volume is successful on "
                   "volume %s", self.volname)

        # Trigger rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start rebalance")
        g.log.info("Rebalance is started")

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=300)
        self.assertTrue(ret, ("Rebalance failed on volume %s",
                              self.volname))
        g.log.info("Rebalance is successful on "
                   "volume %s", self.volname)

        # Close connection and check if write on open fd has completed
        ret, _, _ = proc.async_communicate()
        self.assertEqual(ret, 0, "Write on open fd"
                         " has not completed yet")
        g.log.info("Write completed on open fd")

        # Calculate file count for the mount-point
        cmd = ("ls -lR {}/testfile* | wc -l".format(m_point))
        ret, count_after, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to get file count")
        g.log.info("File count after rebalance is:%s", count_after)

        # Check if there is any data loss
        self.assertEqual(int(count_before), int(count_after),
                         "The file count before and after"
                         " rebalance is not same."
                         " There is data loss.")
        g.log.info("The file count before and after rebalance is same."
                   " No data loss occurred.")

        # Check if rebalance has any failures
        ret = get_rebalance_status(self.mnode, self.volname)
        no_of_failures = ret['aggregate']['failures']
        self.assertEqual(int(no_of_failures), 0,
                         "Failures in rebalance")
        g.log.info("No failures in rebalance")
