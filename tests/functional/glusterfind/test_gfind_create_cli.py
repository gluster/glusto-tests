#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY :or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.glusterfind_ops import (gfind_create, gfind_delete)


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestGlusterFindCreateCLI(GlusterBaseClass):
    """
    GlusterFindCreateCLI contains tests which verifies the glusterfind create
    command functionality.
    """

    def setUp(self):
        """
        setup volume for test case
        """

        # calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup %s", self.volname)
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup_Volume %s" % self.volname)
        g.log.info("Successful in Setup Volume %s", self.volname)

    def tearDown(self):
        """
        tearDown for every test
        Clean up the volume
        """

        # Deleting the sessions created
        sessionlist = ['validsession', 'validoptsession']
        for sess in sessionlist:
            ret, _, _ = gfind_delete(self.mnode, self.volname, sess)
            if ret != 0:
                raise ExecutionError("Failed to delete session '%s'" % sess)
            g.log.info("Successfully deleted session '%s'", sess)

        # stopping the volume
        g.log.info("Starting to Cleanup Volume")
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_gfind_create_cli(self):
        """
        Verifying the glusterfind create command functionality with valid
        and invalid values for the required and optional parameters.

        * Create a volume
        * Create a session on the volume with the following combinations:
            - Valid values for required parameters
            - Invalid values for required parameters
            - Valid values for optional parameters
            - Invalid values for optional parameters

            Required parameters: volname and sessname
            Optional parameters: debug, force, reset-session-time
        """

        # pylint: disable=too-many-statements
        # Create a session with valid inputs for required parameters
        session1 = 'validsession'
        g.log.info("Creating a session for the volume %s with valid values"
                   "for the required parameters", self.volname)
        ret, _, _ = gfind_create(self.mnode, self.volname, session1)
        self.assertEqual(ret, 0, ("Unexpected: Creation of a session for the "
                                  "volume %s failed", self.volname))
        g.log.info("Successful in validating the glusterfind create command "
                   "with valid values for required parameters")

        # Create a session with invalid inputs for required parameters
        session2 = 'invalidsession'
        g.log.info("Creating a session with invalid values for the "
                   "required parameters")
        ret, _, _ = gfind_create(self.mnode, 'invalidvolumename', session2)
        self.assertNotEqual(ret, 0, ("Unexpected: Creation of a session is "
                                     "Successful with invalid values"))
        g.log.info("Successful in validating the glusterfind create command "
                   "with invalid values for required parameters")

        # Create a session with valid inputs for optional parameters
        session3 = 'validoptsession'
        g.log.info("Creating a session for the volume %s with valid values"
                   "for the optional parameters", self.volname)
        ret, _, _ = gfind_create(self.mnode, self.volname, session3,
                                 force=True, resetsesstime=True, debug=True)
        self.assertEqual(ret, 0, ("Unexpected: Creation of a session for the "
                                  "volume %s failed", self.volname))
        g.log.info("Successful in validating the glusterfind create command "
                   "with valid values for optional parameters")

        # Create a session with invalid inputs for optional parameters
        session4 = 'invalidoptsession'
        g.log.info("Creating a session with invalid values for the "
                   "optional parameters")
        cmd = ("glusterfind create %s %s --debg --frce --resetsessiontime"
               % (session4, self.volname))
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertNotEqual(ret, 0, ("Unexpected: Creation of a session is "
                                     "Successful with invalid values"))
        g.log.info("Successful in validating the glusterfind create command "
                   "with invalid values for required parameters")
