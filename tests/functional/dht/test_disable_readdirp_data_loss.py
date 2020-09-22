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
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.glusterdir import get_dir_contents


@runs_on([['distributed-dispersed'], ['glusterfs']])
class TestDisableReaddirpDataLoss(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume():
            raise ExecutionError("Failed to Setup_Volume %s" % self.volname)

    def tearDown(self):

        # Unmount volume if mounted
        if self.currently_mounted_clients:
            if not self.unmount_volume(self.currently_mounted_clients):
                raise ExecutionError("Failed to unmount Volume")

        # Cleanup volume
        if not self.cleanup_volume():
            raise ExecutionError("Failed to Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _mount_on_a_client(self, mountobj):
        """Mount volume on one client and update list"""
        ret = self.mount_volume([mountobj])
        self.assertTrue(ret, "Failed to mount volume on client")
        self.currently_mounted_clients.append(mountobj)

    def _perfrom_lookups_on_mount_point(self, node, mountpoint):
        """Perform lookups on a given mount point"""
        ret = get_dir_contents(node, mountpoint)
        self.assertEqual(len(ret), 8,
                         "8 dirs not present on mount point %s on %s"
                         % (node, mountpoint))
        g.log.info("Lookup successful on node %s and mount point %s",
                   node, mountpoint)

    def test_disable_readdirp_data_loss(self):
        """
        Test case:
        1. Create a 2 x (4+2) disperse volume and start it.
        2. Disable performance.force-readdirp and dht.force-readdirp.
        3. Mount the volume on one client and create 8 directories.
        4. Do a lookup on the mount using the same mount point,
           number of directories should be 8.
        5. Mount the volume again on a different client and check
           if number of directories is the same or not.
        """
        # List to determine if volume is mounted or not
        self.currently_mounted_clients = []

        # Disable performance.force-readdirp and dht.force-readdirp
        for option, val in (("performance.force-readdirp", "disable"),
                            ("dht.force-readdirp", "off")):
            ret = set_volume_options(self.mnode, self.volname, {option: val})
            self.assertTrue(ret, "Failed to set volume option %s to %s"
                            % (option, val))
        g.log.info("Successfully disabled performance.force-readdirp and "
                   "dht.force-readdirp")

        # Mount the volume on one client and create 8 directories
        self._mount_on_a_client(self.mounts[0])
        ret, _, _ = g.run(self.mounts[0].client_system,
                          "mkdir %s/dir{1..8}" % self.mounts[0].mountpoint)
        self.assertFalse(ret, "Failed to create 8 directories on mount point")
        g.log.info("Successfully mounted and create 8 dirs on mount point")

        # Do a lookup on the mount using the same mount point,
        # number of directories should be 8
        self._perfrom_lookups_on_mount_point(self.mounts[0].client_system,
                                             self.mounts[0].mountpoint)

        # Mount the volume again on a different client and check
        # if number of directories is the same or not
        self._mount_on_a_client(self.mounts[1])
        self._perfrom_lookups_on_mount_point(self.mounts[1].client_system,
                                             self.mounts[1].mountpoint)
