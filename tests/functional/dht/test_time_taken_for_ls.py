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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed'], ['glusterfs']])
class TestTimeForls(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume(self.mounts):
            raise ExecutionError("Failed to Setup and mount volume")

        self.is_io_running = False

    def tearDown(self):

        if self.is_io_running:
            self._validate_io()

        if not self.unmount_volume_and_cleanup_volume(self.mounts):
            raise ExecutionError("Failed to Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _validate_io(self):
        """Validare I/O threads running on mount point"""
        io_success = []
        for proc in self.proc_list:
            try:
                ret, _, _ = proc.async_communicate()
                if ret:
                    io_success.append(False)
                    break
                io_success.append(True)
            except ValueError:
                io_success.append(True)
        return all(io_success)

    def test_time_taken_for_ls(self):
        """
        Test case:
        1. Create a volume of type distributed-replicated or
           distributed-arbiter or distributed-dispersed and start it.
        2. Mount the volume to clients and create 2000 directories
           and 10 files inside each directory.
        3. Wait for I/O to complete on mount point and perform ls
           (ls should complete within 10 seconds).
        """
        # Creating 2000 directories on the mount point
        ret, _, _ = g.run(self.mounts[0].client_system,
                          "cd %s; for i in {1..2000};do mkdir dir$i;done"
                          % self.mounts[0].mountpoint)
        self.assertFalse(ret, 'Failed to create 2000 dirs on mount point')

        # Create 5000 files inside each directory
        dirs = ('{1..100}', '{101..200}', '{201..300}', '{301..400}',
                '{401..500}', '{501..600}', '{601..700}', '{701..800}',
                '{801..900}', '{901..1000}', '{1001..1100}', '{1101..1200}',
                '{1201..1300}', '{1301..1400}', '{1401..1500}', '{1501..1600}',
                '{1801..1900}', '{1901..2000}')
        self.proc_list, counter = [], 0
        while counter < 18:
            for mount_obj in self.mounts:
                ret = g.run_async(mount_obj.client_system,
                                  "cd %s;for i in %s;do "
                                  "touch dir$i/file{1..10};done"
                                  % (mount_obj.mountpoint, dirs[counter]))
                self.proc_list.append(ret)
                counter += 1
        self.is_io_running = True

        # Check if I/O is successful or not
        ret = self._validate_io()
        self.assertTrue(ret, "Failed to create Files and dirs on mount point")
        self.is_io_running = False
        g.log.info("Successfully created files and dirs needed for the test")

        # Run ls on mount point which should get completed within 10 seconds
        ret, _, _ = g.run(self.mounts[0].client_system,
                          "cd %s; timeout 10 ls"
                          % self.mounts[0].mountpoint)
        self.assertFalse(ret, '1s taking more than 10 seconds')
        g.log.info("ls completed in under 10 seconds")
