#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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
        Test cases in this module tests mount operation on clients having
        authentication to mount using combination of  FQDN and IP address.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.auth_ops import set_auth_allow
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class AuthAllowIpFqdn(GlusterBaseClass):
    """
    Tests to verify authentication feature on fuse mount using a combination
    of IP and fqdn.
    """
    @classmethod
    def setUpClass(cls):
        """
        Create and start volume
        """
        cls.get_super_method(cls, 'setUpClass')()
        # Create and start volume
        g.log.info("Starting volume setup process %s", cls.volname)
        ret = cls.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup "
                                 "and start volume %s" % cls.volname)
        g.log.info("Successfully created and started the volume: %s",
                   cls.volname)

    def mount_and_verify(self, mount_obj):
        """
        Mount volume/sub-directory and verify whether it is mounted.

        Args:
            mount_obj(obj): Object of GlusterMount class
        """
        # Mount volume/sub-directory
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

    def test_auth_allow_ip_fqdn(self):
        """
        Verify auth.allow feature using a combination of client ip and fqdn.
        Steps:
        1. Setup and start volume
        2. Set auth.allow on volume using ip of client1 and hostname of
           client2.
        3. Mount the volume on client1 and client2.
        5. Create directory d1 on client1 mountpoint.
        6. Unmount the volume from client1 and client2.
        7. Set auth.allow on d1 using ip of client1 and hostname of client2.
        8. Mount d1 on client1 and client2.
        9. Unmount d1 from client1 and client2.
        """
        # Obtain hostname of client2
        ret, hostname_client2, _ = g.run(self.mounts[1].client_system,
                                         "hostname")
        self.assertEqual(ret, 0, ("Failed to obtain hostname of client %s"
                                  % self.mounts[1].client_system))
        hostname_client2 = hostname_client2.strip()
        g.log.info("Obtained hostname of client. IP- %s, hostname- %s",
                   self.mounts[1].client_system, hostname_client2)

        # Setting authentication on volume using ip of client1 and hostname of
        # client2.
        auth_dict = {'all': [self.mounts[0].client_system, hostname_client2]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set authentication")
        g.log.info("Successfully set authentication on volume")

        # Mount volume on client1
        self.mount_and_verify(self.mounts[0])

        # Mount volume on client2
        self.mount_and_verify(self.mounts[1])

        g.log.info("Successfully mounted volume on client1 and client2.")

        # Creating directory d1 on mounted volume
        ret = mkdir(self.mounts[0].client_system, "%s/d1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'd1' in volume %s "
                              "from client %s"
                              % (self.volname, self.mounts[0].client_system)))

        # Unmount volume from client1.
        ret = self.mounts[0].unmount()
        self.assertTrue(ret, "Failed to unmount volume from client1.")

        # Unmount volume from client2.
        ret = self.mounts[1].unmount()
        self.assertTrue(ret, "Failed to unmount volume from client2.")

        # Setting authentication on d1 using ip of client1 and hostname of
        # client2.
        auth_dict = {'/d1': [self.mounts[0].client_system, hostname_client2]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set authentication")
        g.log.info("Successfully set authentication on volume")

        # Modify GlusterMount objects for mounting sub-directory d1.
        self.mounts[0].volname = "%s/d1" % self.volname
        self.mounts[1].volname = "%s/d1" % self.volname

        # Mount sub-directory d1 on client1
        self.mount_and_verify(self.mounts[0])

        # Mount sub-directory d1 on client2
        self.mount_and_verify(self.mounts[1])

        g.log.info("Successfully mounted sub-dir d1 on client1 and client2.")

        # Unmount sub-directory d1 from client1.
        ret = self.mounts[0].unmount()
        self.assertTrue(ret, "Failed to unmount volume from client1.")

        # Unmount sub-directory d1 from client2.
        ret = self.mounts[1].unmount()
        self.assertTrue(ret, "Failed to unmount volume from client2.")

    def tearDown(self):
        """
        Cleanup volume
        """
        g.log.info("Cleaning up volume")
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume.")
        g.log.info("Volume cleanup was successful.")
