#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['distributed', 'distributed-replicated'],
          ['glusterfs']])
class TestVerifyRebalanceWithFileLock(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()
        cls.script = "/usr/share/glustolibs/io/scripts/file_lock.py"
        if not upload_scripts(cls.clients, [cls.script]):
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        Setup and mount volume
        """
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume(mounts=[self.mounts[0]]):
            raise ExecutionError("Failed to Setup and Mount Volume")

        self.m_point = self.mounts[0].mountpoint
        self.first_client = self.mounts[0].client_system
        self.lock_cmd = ("/usr/bin/env python {} -f {}/test_file -t 200"
                         .format(self.script, self.m_point))

    def _create_file_and_hold_lock(self):
        """ Creates a file and holds lock on the file created"""
        cmd = ("cd {}; dd if=/dev/zero of=test_file bs=10M count=1;"
               .format(self.m_point))
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertEqual(ret, 0, "Failed to create file")

        # Acquire lock to the file created
        self.proc = g.run_async(self.first_client, self.lock_cmd)

    def _expand_volume_and_verify_rebalance(self):
        """ Expands the volume, trigger rebalance and verify file is copied"""

        # Expand the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to expand the volume")

        # Trigger rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1200)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

    def _verify_file_lock(self):
        """ Verifies file lock on file before been released by 1st program"""

        ret, _, _ = g.run(self.mounts[0].client_system, self.lock_cmd)
        self.assertEqual(ret, 1, "Unexpected: acquired lock before released")
        g.log.info("Expected : Lock can't be acquired before being released")

        # Wait till the lock is been released
        ret, _, _ = self.proc.async_communicate()
        self.assertEqual(ret, 0, "File lock process failed")

    def test_verify_rebalance_with_file_lock(self):
        """
        Steps:
        1. Create a distributed or distributed-replicate volume and
           populate some data
        2. Hold exclusive lock on a file (can use flock)
        3. Add-brick and rebalance (make sure this file gets migrated)
        4. Again from another program try to hold exclusive lock on this file
        """
        # Create a File
        self._create_file_and_hold_lock()

        # Expand the volume
        self._expand_volume_and_verify_rebalance()

        # Try getting lock to the file while the lock is still held by another
        self._verify_file_lock()

    def tearDown(self):
        # Unmount and cleanup original volume
        if not self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]]):
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
