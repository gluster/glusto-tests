#  Copyright (C) 2021  Red Hat, Inc. <http://www.redhat.com>
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

""" Description:
        Test cases in this module tests the authentication allow feature
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.auth_ops import set_auth_allow
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete)


@runs_on([['distributed-replicated', 'distributed-dispersed'], ['glusterfs']])
class FuseAuthAllow(GlusterBaseClass):
    """
    Tests to verify auth.allow feature on fuse mount.
    """
    @classmethod
    def setUpClass(cls):
        """
        Create and start volume
        """
        cls.get_super_method(cls, 'setUpClass')()
        # Create and start volume
        ret = cls.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup "
                                 "and start volume %s" % cls.volname)

    def _authenticated_mount(self, mount_obj):
        """
        Mount volume on authenticated client

        Args:
            mount_obj(obj): Object of GlusterMount class
        """
        # Mount volume
        ret = mount_obj.mount()
        self.assertTrue(ret, ("Failed to mount %s on client %s" %
                              (mount_obj.volname,
                               mount_obj.client_system)))
        g.log.info("Successfully mounted %s on client %s", mount_obj.volname,
                   mount_obj.client_system)

        # Verify mount
        ret = mount_obj.is_mounted()
        self.assertTrue(ret, ("%s is not mounted on client %s"
                              % (mount_obj.volname, mount_obj.client_system)))
        g.log.info("Verified: %s is mounted on client %s",
                   mount_obj.volname, mount_obj.client_system)

    def _add_brick_rebalance(self):
        """Create files,Perform Add brick and wait for rebalance to complete"""
        # Create files on mount point using dd command
        cmd = ('cd %s;for i in {1..100000};'
               'do dd if=/dev/urandom bs=1024 count=1 of=file$i;done;'
               % (self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to createfiles on mountpoint")
        g.log.info("Successfully created files on mountpoint")

        # Add brick to volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)

        # Trigger rebalance and wait for it to complete
        ret, _, _ = rebalance_start(self.mnode, self.volname,
                                    force=True)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1200)
        self.assertTrue(ret, "Rebalance is not yet complete on the volume "
                             "%s" % self.volname)
        g.log.info("Rebalance successfully completed")

    def test_auth_allow_with_rebalance(self):
        """
        Validating the FUSE authentication volume options with rebalance
        Steps:
        1. Setup and start volume
        2. Set auth.allow on volume for client1 using ip of client1
        3. Mount volume on client1.
        4. Create files on mount point using dd command
        5. Perform add brick operation
        6. Trigger rebalance
        7. Set auth.allow on volume for client1 using hostname of client1.
        8. Repeat steps from 3 to 7
        """
        # Setting authentication on volume for client1 using ip
        auth_dict = {'all': [self.mounts[0].client_system]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set authentication")

        # Mounting volume on client1
        self._authenticated_mount(self.mounts[0])

        # Create files,perform add-brick,trigger rebalance
        self._add_brick_rebalance()

        # Unmount volume from client1
        ret = self.mounts[0].unmount()
        self.assertTrue(ret, ("Failed to unmount volume %s from client %s"
                              % (self.volname, self.mounts[0].client_system)))

        # Obtain hostname of client1
        ret, hostname_client1, _ = g.run(self.mounts[0].client_system,
                                         "hostname")
        self.assertEqual(ret, 0, ("Failed to obtain hostname of client %s"
                                  % self.mounts[0].client_system))
        g.log.info("Obtained hostname of client. IP- %s, hostname- %s",
                   self.mounts[0].client_system, hostname_client1.strip())

        # Setting authentication on volume for client1 using hostname
        auth_dict = {'all': [hostname_client1.strip()]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set authentication")

        # Mounting volume on client1
        self._authenticated_mount(self.mounts[0])

        # Create files,perform add-brick and trigger rebalance
        self._add_brick_rebalance()

    def tearDown(self):
        """
        Cleanup volume
        """
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume.")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
