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
from glustolibs.gluster.glusterfile import get_md5sum
from glustolibs.gluster.volume_libs import get_subvols, shrink_volume
from glustolibs.gluster.dht_test_utils import find_hashed_subvol
from glustolibs.io.utils import validate_io_procs, wait_for_io_to_complete


@runs_on([['distributed-replicated', 'distributed-dispersed',
           'distributed-arbiter', 'distributed'], ['glusterfs']])
class TestRemoveBrickWithOpenFD(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)
        self.is_copy_running = False

    def tearDown(self):

        # If I/O processes are running wait from them to complete
        if self.is_copy_running:
            if not wait_for_io_to_complete(self.list_of_io_processes,
                                           self.mounts):
                raise ExecutionError("Failed to wait for I/O to complete")

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_remove_brick_with_open_fd(self):
        """
        Test case:
        1. Create volume, start it and mount it.
        2. Open file datafile on mount point and start copying /etc/passwd
           line by line(Make sure that the copy is slow).
        3. Start remove-brick of the subvol to which has datafile is hashed.
        4. Once remove-brick is complete compare the checksum of /etc/passwd
           and datafile.
        """
        # Open file datafile on mount point and start copying /etc/passwd
        # line by line
        ret, out, _ = g.run(self.mounts[0].client_system,
                            "cat /etc/passwd | wc -l")
        self.assertFalse(ret, "Failed to get number of lines of /etc/passwd")
        cmd = ("cd {}; exec 30<> datafile ;for i in `seq 1 {}`; do "
               "head -n $i /etc/passwd | tail -n 1 >> datafile; sleep 10; done"
               .format(self.mounts[0].mountpoint, out.strip()))

        self.list_of_io_processes = [
            g.run_async(self.mounts[0].client_system, cmd)]
        self.is_copy_running = True

        # Start remove-brick of the subvol to which has datafile is hashed
        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']
        number = find_hashed_subvol(subvols, "/", 'datafile')[1]

        ret = shrink_volume(self.mnode, self.volname, subvol_num=number)
        self.assertTrue(ret, "Failed to remove-brick from volume")
        g.log.info("Remove-brick rebalance successful")

        # Validate if I/O was successful or not.
        ret = validate_io_procs(self.list_of_io_processes, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.is_copy_running = False

        # Compare md5checksum of /etc/passwd and datafile
        md5_of_orginal_file = get_md5sum(self.mounts[0].client_system,
                                         '/etc/passwd')
        self.assertIsNotNone(md5_of_orginal_file,
                             'Unable to get md5 checksum of orignial file')
        md5_of_copied_file = get_md5sum(
            self.mounts[0].client_system, '{}/datafile'.format(
                self.mounts[0].mountpoint))
        self.assertIsNotNone(md5_of_copied_file,
                             'Unable to get md5 checksum of copied file')
        self.assertEqual(md5_of_orginal_file.split(" ")[0],
                         md5_of_copied_file.split(" ")[0],
                         "md5 checksum of original and copied file didn't"
                         " match")
        g.log.info("md5 checksum of original and copied files are same")
