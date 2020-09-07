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

"""
    Description:
    Test Cases in this module tests Gluster against SELinux Labels and Policies
"""

import pytest
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass
from glustolibs.gluster.glusterfile import file_exists


class TestGlusterAgainstSELinux(GlusterBaseClass):
    """Glusterd checks against SELinux Labels and Policies
    """

    @staticmethod
    def run_cmd(host, cmd, opts='', operate_on=''):
        if opts:
            opts = '-'+opts
        command = "{} {} {}".format(cmd, opts, operate_on)
        rcode, rout, rerr = g.run(host, command)
        if not rcode:
            return True, rout

        g.log.error("On '%s', '%s' returned '%s'", host, command, rerr)
        return False, rout

    @pytest.mark.test_selinux_label
    def test_selinux_label(self):
        """
        TestCase:
        1. Check the existence of '/usr/lib/firewalld/services/glusterfs.xml'
        2. Validate the owner of this file as 'glusterfs-server'
        3. Validate SELinux label context as 'system_u:object_r:lib_t:s0'
        """

        fqpath = '/usr/lib/firewalld/services/glusterfs.xml'

        for server in self.all_servers_info:
            # Check existence of xml file
            self.assertTrue(file_exists(server, fqpath), "Failed to verify "
                            "existence of '{}' in {} ".format(fqpath, server))
            g.log.info("Validated the existence of required xml file")

            # Check owner of xml file
            status, result = self.run_cmd(server, 'rpm', 'qf', fqpath)
            self.assertTrue(status, "Fail: Not able to find owner for {} on "
                            "{}".format(fqpath, server))
            exp_str = 'glusterfs-server'
            self.assertIn(exp_str, result, "Fail: Owner of {} should be "
                          "{} on {}".format(fqpath, exp_str, server))

            # Validate SELinux label
            status, result = self.run_cmd(server, 'ls', 'lZ', fqpath)
            self.assertTrue(status, "Fail: Not able to find SELinux label "
                            "for {} on {}".format(fqpath, server))
            exp_str = 'system_u:object_r:lib_t:s0'
            self.assertIn(exp_str, result, "Fail: SELinux label on {}"
                          "should be {} on {}".format(fqpath, exp_str, server))
