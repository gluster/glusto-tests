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
        Test cases in this module tests the authentication allow feature
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.auth_ops import set_auth_allow


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class FuseAuthAllow(GlusterBaseClass):
    """
    Tests to verify auth.allow feature on fuse mount.
    """
    @classmethod
    def setUpClass(cls):
        """
        Create and start volume
        """
        GlusterBaseClass.setUpClass.im_func(cls)
        # Create and start volume
        g.log.info("Starting volume setup process %s", cls.volname)
        ret = cls.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup "
                                 "and start volume %s" % cls.volname)
        g.log.info("Successfully created and started the volume: %s",
                   cls.volname)

    def authenticated_mount(self, mount_obj):
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

    def unauthenticated_mount(self, mount_obj):
        """
        Try to mount volume on unauthenticated client
        Args:
            mount_obj(obj): Object of GlusterMount class
        """
        # Try to mount volume and verify
        # Sometimes the mount command is returning exit code as 0 in case of
        # mount failures as well.
        # Hence not asserting while running mount command in test case.
        # Instead asserting only if it is actually mounted.
        # BZ 1590711
        mount_obj.mount()

        # Verify mount
        ret = mount_obj.is_mounted()
        if ret:
            # Mount operation did not fail as expected. Cleanup the mount.
            if not mount_obj.unmount():
                g.log.error("Failed to unmount %s from client %s",
                            mount_obj.volname, mount_obj.client_system)
        self.assertFalse(ret, ("Mount operation did not fail as "
                               "expected. Mount operation of "
                               "%s on client %s passed. "
                               "Mount point: %s"
                               % (mount_obj.volname,
                                  mount_obj.client_system,
                                  mount_obj.mountpoint)))
        g.log.info("Mount operation of %s on client %s failed as "
                   "expected", mount_obj.volname, mount_obj.client_system)

    def is_auth_failure(self, client_ip, previous_log_statement=''):
        """
        Check if the mount failure is due to authentication error
        Args:
            client_ip(str): IP of client in which mount failure has to be
                verified.
            previous_log_statement(str): AUTH_FAILED message of previous mount
                failure due to auth error(if any). This is used to distinguish
                between the current and previous message.
        Return(str):
            Latest AUTH_FAILED event log message.
        """
        # Command to find the log file
        cmd = "ls /var/log/glusterfs/ -1t | head -1"
        ret, out, _ = g.run(client_ip, cmd)
        self.assertEqual(ret, 0, "Failed to find the log file.")

        # Command to fetch latest AUTH_FAILED event log message.
        cmd = "grep AUTH_FAILED /var/log/glusterfs/%s | tail -1" % out.strip()
        ret, current_log_statement, _ = g.run(client_ip, cmd)
        self.assertEqual(ret, 0, "Mount failure is not due to auth error")

        # Check whether the AUTH_FAILED log is of the latest mount failure
        self.assertNotEqual(current_log_statement.strip(),
                            previous_log_statement,
                            "Mount failure is not due to authentication "
                            "error")
        g.log.info("Mount operation has failed due to authentication error")
        return current_log_statement.strip()

    def test_auth_allow(self):
        """
        Verify auth.allow feature using client ip and hostname.
        Steps:
        1. Setup and start volume
        2. Set auth.allow on volume for client1 using ip of client1
        3. Mount volume on client1.
        4. Try to mount volume on client2. This should fail.
        5. Check the client2 logs for AUTH_FAILED event.
        6. Unmount the volume from client1.
        7. Set auth.allow on volume for client1 using hostname of client1.
        8. Repeat steps 3 to 5.
        9. Unmount the volume from client1.
        """
        # Setting authentication on volume for client1 using ip
        auth_dict = {'all': [self.mounts[0].client_system]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set authentication")
        g.log.info("Successfully set authentication on volume")

        # Mounting volume on client1
        self.authenticated_mount(self.mounts[0])

        # Trying to mount volume on client2
        self.unauthenticated_mount(self.mounts[1])

        # Verify whether mount failure on client2 is due to auth error
        log_msg = self.is_auth_failure(self.mounts[1].client_system)
        prev_log_statement = log_msg

        g.log.info("Verification of auth.allow on volume using client IP is "
                   "successful")

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
        g.log.info("Successfully set authentication on volume")

        # Mounting volume on client1
        self.authenticated_mount(self.mounts[0])

        # Trying to mount volume on client2
        self.unauthenticated_mount(self.mounts[1])

        # Verify whether mount failure on client2 is due to auth error
        log_msg = self.is_auth_failure(self.mounts[1].client_system,
                                       prev_log_statement)
        prev_log_statement = log_msg

        g.log.info("Verification of auth.allow on volume using client "
                   "hostname is successful")

        # Unmount volume from client1
        ret = self.mounts[0].unmount()
        self.assertTrue(ret, ("Failed to unmount volume %s from client %s"
                              % (self.volname, self.mounts[0].client_system)))

    def tearDown(self):
        """
        Cleanup volume
        """
        g.log.info("Cleaning up volume")
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume.")
        g.log.info("Volume cleanup was successful.")
