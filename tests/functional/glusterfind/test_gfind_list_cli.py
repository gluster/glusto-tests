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
from glustolibs.gluster.glusterfind_ops import (gfind_list, gfind_create,
                                                gfind_delete)


@runs_on([['distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed', 'distributed', 'arbiter',
           'dispersed', 'replicated'], ['glusterfs']])
class TestGlusterFindListCLI(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume():
            raise ExecutionError("Failed to Setup_Volume %s" % self.volname)

    def tearDown(self):

        # Cleanup glusterfind session and volume
        ret, _, _ = gfind_delete(self.mnode, self.volname, self.session)
        if ret:
            raise ExecutionError("Failed to delete session '%s'"
                                 % self.session)

        if not self.cleanup_volume():
            raise ExecutionError("Failed to Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _check_glusterfind_list_output(self, out):
        """Check if glusterfind list output is proper or not."""
        out = list(
            filter(None, list(filter(None, out.split("\n")))[2].split(" ")))
        self.assertEqual(out[0], self.session,
                         "Unexpected: Session name not poper in output")
        self.assertEqual(out[1], self.volname,
                         "Unecpected: Volume name not proper in output")

    def test_gfind_list_cli(self):
        """
        Verifying the glusterfind list command functionality with valid
        and invalid values for the required and optional parameters.

        * Create a volume
        * Create a session on the volume and call glusterfind list with the
          following combinations:
            - Valid values for optional parameters
            - Invalid values for optional parameters

        NOTE:
          There are no required parameters for glusterfind list command.
        """
        # Creating a glusterfind session
        self.session = "session1"
        ret, _, _ = gfind_create(self.mnode, self.volname, self.session)
        self.assertEqual(ret, 0, "Glusterfind session creation for the "
                                 "volume %s failed" % self.volname)

        # Checking output of glusterfind list
        ret, out, _ = gfind_list(self.mnode)
        self.assertEqual(ret, 0, "Glusterfind list failed")
        self._check_glusterfind_list_output(out)
        g.log.info("glusterfind list cmd validation without any param passed")

        # Check output for glusterfind list with valid and invalid volume name
        for volume, expected_value, validation in ((self.volname, 0, 'valid'),
                                                   ("abc", 1, 'invalid')):
            ret, out, _ = gfind_list(self.mnode, volname=volume)
            self.assertEqual(ret, expected_value,
                             "Glusterfind list --volume check with %s "
                             "parameter failed" % validation)
            if not ret:
                self._check_glusterfind_list_output(out)
        g.log.info("glusterind list cmd check with --volume param passed")

        # Check output for glusterfind list with valid and invalid session name
        for session, expected_value, validation in ((self.session, 0, 'valid'),
                                                    ("abc", 1, 'invalid')):
            ret, out, _ = gfind_list(self.mnode, sessname=session)
            self.assertEqual(ret, expected_value,
                             "Glusterfind list --session check with %s "
                             "parameter failed" % validation)
            if not ret:
                self._check_glusterfind_list_output(out)
        g.log.info("glusterfind list cmd check with --session param passed")

        # Check output of glusterind list with debug parameter
        ret, _, _ = gfind_list(self.mnode, debug=True)
        self.assertEqual(ret, 0, "Glusterfind list --debug parameter failed")
        g.log.info("glusterfind list cmd check with --debug param passed")
