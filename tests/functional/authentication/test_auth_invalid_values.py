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

"""
    Description:
        Test cases in this module tests negative scenario of authentication
        feature by giving invalid values.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import is_volume_exported


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class AuthInvalidValues(GlusterBaseClass):
    """
    Tests to verify negative scenario in authentication allow and reject
    options by giving invalid values
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

    def set_invalid_auth(self, auth_opt, values_list):
        """
        Try to set invalid values on authentication options.

        Args:
            auth_opt(str): Authentication option which has to be set.
            values_list(list): List of invalid values.
        Return(bool):
            True if set command failed due to invalid value.
            False if the failure is due to some other reason.
        """
        error_msg_fuse = "not a valid internet-address-list"
        error_msg_nfs = "not a valid mount-auth-address"

        # Try to set invalid values.
        for value in values_list:
            auth_cmd = ("gluster volume set %s %s \"%s\""
                        % (self.volname, auth_opt, value))
            ret, _, err = g.run(self.mnode, auth_cmd)
            self.assertNotEqual(ret, 0, "Command to set %s value as %s didn't"
                                        " fail as expected." % (auth_opt,
                                                                value))

            # Verify whether the failure is due to invalid value.
            if self.mount_type == "nfs":
                if error_msg_nfs not in err:
                    g.log.error("Command to set %s value as %s has failed due"
                                " to unknown reason.", auth_opt, value)
                    return False

            if self.mount_type == "glusterfs":
                if error_msg_fuse not in err:
                    g.log.error("Command to set %s value as %s has failed due"
                                " to unknown reason.", auth_opt, value)
                    return False

            g.log.info("Expected: Command to set %s value as %s has"
                       " failed due to invalid value.", auth_opt, value)
        return True

    def test_auth_invalid_values(self):
        """
        Verify negative scenario in authentication allow and reject options by
        trying to set invalid values.
        Steps:
        1. Create and start volume.
        2. Try to set the value "a/a", "192.{}.1.2", "/d1(a/a)",
           "/d1(192.{}.1.2)" separately in auth.allow option.
        3. Try to set the value "a/a", "192.{}.1.2", "/d1(a/a)",
           "/d1(192.{}.1.2)" separately in auth.reject option.
        4. Steps 2 and 3 should fail due to error "not a valid
           internet-address-list"
        5. Verify volume is exported as nfs.
        6. Try to set the value "a/a", "192.{}.1.2", "/d1(a/a)",
           "/d1(192.{}.1.2)" separately in nfs.rpc-auth-allow option.
        7. Try to set the value "a/a", "192.{}.1.2", "/d1(a/a)",
           "/d1(192.{}.1.2)" separately in nfs.rpc-auth-reject option.
        8. Steps 6 and 7 should fail due to error "not a valid
           mount-auth-address"
        """
        invalid_values = ["a/a", "192.{}.1.2", "/d1(a/a)", "/d1(192.{}.1.2)"]

        if self.mount_type == "glusterfs":
            # Try to set invalid values in auth.allow option.
            ret = self.set_invalid_auth("auth.allow", invalid_values)
            self.assertTrue(ret, "Failure of command to set auth.allow value "
                                 "is not because of invalid values.")
            g.log.info("Successfully verified auth.allow set command using"
                       " invalid values. Command failed as expected.")

            # Try to set invalid values in auth.reject option.
            ret = self.set_invalid_auth("auth.reject", invalid_values)
            self.assertTrue(ret, "Failure of command to set auth.reject value"
                                 " is not because of invalid values.")
            g.log.info("Successfully verified auth.reject set command using"
                       " invalid values. Command failed as expected.")

        if self.mount_type == "nfs":
            # Check whether volume is exported as gnfs
            ret = is_volume_exported(self.mnode, self.volname,
                                     self.mount_type)
            self.assertTrue(ret, "Volume is not exported as nfs")

            # Enable nfs.addr-namelookup option.
            ret = set_volume_options(self.mnode, self.volname,
                                     {"nfs.addr-namelookup": "enable"})
            self.assertTrue(ret, "Failed to enable nfs.addr-namelookup "
                                 "option.")

            # Try to set invalid values in nfs.rpc-auth-allow option.
            ret = self.set_invalid_auth("nfs.rpc-auth-allow", invalid_values)
            self.assertTrue(ret, "Command failure to set nfs.rpc-auth-allow"
                                 " value is not because of invalid values.")
            g.log.info("Successfully verified nfs.rpc-auth-allow set command"
                       " using invalid values. Command failed as expected.")

            # Try to set invalid values in nfs.rpc-auth-reject option.
            self.set_invalid_auth("nfs.rpc-auth-reject", invalid_values)
            self.assertTrue(ret, "Command failure to set nfs.rpc-auth-reject"
                                 " value is not because of invalid values.")
            g.log.info("Successfully verified nfs.rpc-auth-reject set command"
                       " using invalid values. Command failed as expected.")

    def tearDown(self):
        """
        Cleanup volume
        """
        g.log.info("Cleaning up volume")
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume.")
        g.log.info("Volume cleanup was successful.")
