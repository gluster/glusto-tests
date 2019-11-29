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
        using auth.allow and auth.reject volume options
"""
import copy
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.auth_ops import set_auth_allow, set_auth_reject


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class FuseAuthRejectAllow(GlusterBaseClass):
    """
    Tests to verify auth.reject and auth.allow volume options in volume and
    sub-directory level on fuse mount.
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

    def authenticated_mount(self, mount_obj):
        """
        Mount volume/sub-directory on authenticated client

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
        Try to mount volume/sub-directoty on unauthenticated client
        Args:
            mount_obj(obj): Object of GlusterMount class
        """
        # Try to mount volume/sub-directory and verify
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

    def test_auth_reject_allow(self):
        """
        Verify auth.reject and auth.allow volume options in volume level using
        both client ip and hostname.
        Verify auth.reject and auth.allow volume options in sub-directory
        level using both client ip and hostname.
        Steps:
        1. Create and start volume.
        2. Set auth.reject on volume for client1 using ip of client1.
        3. Set auth.allow on volume for client2 using ip of client2.
        4. Try to mount volume on client1. This should fail.
        5. Check the client1 log for AUTH_FAILED event.
        6. Mount volume on client2.
        7. Unmount the volume from client2.
        8. Set auth.reject on volume for client1 using hostname of client1.
        9. Set auth.allow on volume for client2 using hostname of client2.
        10. Repeat steps 4 to 6
        11. Create directory d1 on client2 mountpoint.
        12. Unmount the volume from client2.
        13. Set auth.reject on d1 for client1 using ip of client1.
        14. Set auth.allow on d1 for client2 using ip of client2.
        15. Try to mount d1 on client1. This should fail.
        16. Check the client1 log for AUTH_FAILED event.
        17. Mount d1 on client2.
        18. Unmount d1 from client2.
        19. Set auth.reject on d1 for client1 using hostname of client1.
        20. Set auth.allow on d1 for client2 using hostname of client2.
        21. Repeat steps 15 to 18.
        """
        # pylint: disable = too-many-statements
        # Setting auth.reject on volume for client1 using ip
        auth_dict = {'all': [self.mounts[0].client_system]}
        ret = set_auth_reject(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set auth.reject volume option.")
        g.log.info("Successfully set auth.reject option on volume")

        # Setting auth.allow on volume for client2 using ip
        auth_dict = {'all': [self.mounts[1].client_system]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set auth.allow volume option")
        g.log.info("Successfully set auth.allow option on volume")

        # Trying to mount volume on client1
        self.unauthenticated_mount(self.mounts[0])

        # Verify whether mount failure on client1 is due to auth error
        log_msg = self.is_auth_failure(self.mounts[0].client_system)
        prev_log_statement = log_msg

        # Mounting volume on client2
        self.authenticated_mount(self.mounts[1])

        g.log.info("Verification of auth.reject and auth.allow options on "
                   "volume using client IP is successful")

        # Unmount volume from client2
        ret = self.mounts[1].unmount()
        self.assertTrue(ret, ("Failed to unmount volume %s from client %s"
                              % (self.volname, self.mounts[1].client_system)))

        # Obtain hostname of client1
        ret, hostname_client1, _ = g.run(self.mounts[0].client_system,
                                         "hostname")
        self.assertEqual(ret, 0, ("Failed to obtain hostname of client %s"
                                  % self.mounts[0].client_system))
        g.log.info("Obtained hostname of client. IP- %s, hostname- %s",
                   self.mounts[0].client_system, hostname_client1.strip())

        # Obtain hostname of client2
        ret, hostname_client2, _ = g.run(self.mounts[1].client_system,
                                         "hostname")
        self.assertEqual(ret, 0, ("Failed to obtain hostname of client %s"
                                  % self.mounts[1].client_system))
        g.log.info("Obtained hostname of client. IP- %s, hostname- %s",
                   self.mounts[1].client_system, hostname_client2.strip())

        # Setting auth.reject on volume for client1 using hostname
        auth_dict = {'all': [hostname_client1.strip()]}
        ret = set_auth_reject(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set auth.reject volume option.")
        g.log.info("Successfully set auth.reject option on volume")

        # Setting auth.allow on volume for client2 using hostname
        auth_dict = {'all': [hostname_client2.strip()]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set auth.allow volume option")
        g.log.info("Successfully set auth.allow option on volume")

        # Trying to mount volume on client1
        self.unauthenticated_mount(self.mounts[0])

        # Verify whether mount failure on client1 is due to auth error
        log_msg = self.is_auth_failure(self.mounts[0].client_system,
                                       prev_log_statement)
        prev_log_statement = log_msg

        # Mounting volume on client2
        self.authenticated_mount(self.mounts[1])

        g.log.info("Verification of auth.reject and auth.allow options on "
                   "volume using client hostname is successful")

        # Creating sub directory d1 on mounted volume
        ret = mkdir(self.mounts[1].client_system, "%s/d1"
                    % self.mounts[1].mountpoint)
        self.assertTrue(ret, ("Failed to create directory 'd1' in volume %s "
                              "from client %s"
                              % (self.volname, self.mounts[1].client_system)))

        # Unmount volume from client2
        ret = self.mounts[1].unmount()
        self.assertTrue(ret, ("Failed to unmount volume %s from client %s"
                              % (self.volname, self.mounts[1].client_system)))

        # Setting auth.reject on d1 for client1 using ip
        auth_dict = {'/d1': [self.mounts[0].client_system]}
        ret = set_auth_reject(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set auth.reject volume option.")
        g.log.info("Successfully set auth.reject option.")

        # Setting auth.allow on d1 for client2 using ip
        auth_dict = {'/d1': [self.mounts[1].client_system]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set auth.allow volume option")
        g.log.info("Successfully set auth.allow option.")

        # Creating mount object for sub-directory mount on client1
        mount_obj_client1 = copy.deepcopy(self.mounts[0])
        mount_obj_client1.volname = "%s/d1" % self.volname

        # Creating mount object for sub-directory mount on client2
        mount_obj_client2 = copy.deepcopy(self.mounts[1])
        mount_obj_client2.volname = "%s/d1" % self.volname

        # Trying to mount d1 on client1
        self.unauthenticated_mount(mount_obj_client1)

        # Verify whether mount failure on client1 is due to auth error
        log_msg = self.is_auth_failure(mount_obj_client1.client_system,
                                       prev_log_statement)
        prev_log_statement = log_msg

        # Mounting d1 on client2
        self.authenticated_mount(mount_obj_client2)

        g.log.info("Verification of auth.reject and auth.allow options on "
                   "sub-directory level using client IP is successful")

        # Unmount d1 from client2
        ret = mount_obj_client2.unmount()
        self.assertTrue(ret, ("Failed to unmount %s from client %s"
                              % (mount_obj_client2.volname,
                                 mount_obj_client2.client_system)))

        # Setting auth.reject on d1 for client1 using hostname
        auth_dict = {'/d1': [hostname_client1.strip()]}
        ret = set_auth_reject(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set auth.reject volume option.")
        g.log.info("Successfully set auth.reject option.")

        # Setting auth.allow on d1 for client2 using hostname
        auth_dict = {'/d1': [hostname_client2.strip()]}
        ret = set_auth_allow(self.volname, self.mnode, auth_dict)
        self.assertTrue(ret, "Failed to set auth.allow volume option")
        g.log.info("Successfully set auth.allow option.")

        # Trying to mount d1 on client1
        self.unauthenticated_mount(mount_obj_client1)

        # Verify whether mount failure on client1 is due to auth error
        self.is_auth_failure(mount_obj_client1.client_system,
                             prev_log_statement)

        # Mounting d1 on client2
        self.authenticated_mount(mount_obj_client2)

        g.log.info("Verification of auth.reject and auth.allow options on "
                   "sub-directory level using client hostname is successful")

        # Unmount d1 from client2
        ret = mount_obj_client2.unmount()
        self.assertTrue(ret, ("Failed to unmount %s from client %s"
                              % (mount_obj_client2.volname,
                                 mount_obj_client2.client_system)))

    def tearDown(self):
        """
        Cleanup volume
        """
        g.log.info("Cleaning up volume")
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume.")
        g.log.info("Volume cleanup was successful.")
