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
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfind_ops import (gfind_create,
                                                gfind_list,
                                                gfind_delete)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class GlusterFindDeleteCLI(GlusterBaseClass):
    """
    GlusterFindDeleteCLI contains tests which verifies the glusterfind delete
    command functionality.
    """

    def setUp(self):
        """
        Setup volume and mount volume
        """

        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume %s" % self.volname)
        g.log.info("Successful in Setup Volume %s", self.volname)
        self.session = 'test-session-%s' % self.volname

    def tearDown(self):
        """
        Clean up the volume
        """

        # Check if glusterfind list contains any sessions
        # If session exists, then delete it
        g.log.info("Performing glusterfind list to check if the session "
                   "exists")
        ret, _, _ = gfind_list(self.mnode, volname=self.volname,
                               sessname=self.session)
        if ret == 0:
            g.log.error("Unexpected: Glusterfind list shows existing session")
            delret, _, _ = gfind_delete(self.mnode, self.volname, self.session)
            if delret != 0:
                raise ExecutionError("Failed to delete the session")
            g.log.info("Successfully deleted the session")
        g.log.info("Successful: No session is listed")

        # stopping the volume
        g.log.info("Starting to Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

        # calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_gfind_delete_cli(self):
        """
        Verifying the glusterfind delete command functionality with valid
        and invalid values for the required and optional parameters.

        * Create a volume
        * Create a session on the volume
        * Perform glusterfind list to check if session is created
        * Delete the glusterfind session with the following combinations:
            - Valid values for required parameters
            - Invalid values for required parameters
            - Valid values for optional parameters
            - Invalid values for optional parameters
        * Perform glusterfind list to check if session is deleted

            Required parameters: volname and sessname
            Optional parameters: debug
        """

        # pylint: disable=too-many-statements
        # Creating a session for the volume
        g.log.info("Creating a session for the volume %s", self.volname)
        ret, _, _ = gfind_create(self.mnode, self.volname, self.session)
        self.assertEqual(ret, 0, ("Unexpected: Creation of a session for the "
                                  "volume %s failed" % self.volname))
        g.log.info("Successfully created a session for the volume %s",
                   self.volname)

        # Perform glusterfind list to check if session exists
        g.log.info("Performing glusterfind list to check if the session is "
                   "created")
        ret, _, _ = gfind_list(self.mnode, volname=self.volname,
                               sessname=self.session)
        self.assertEqual(ret, 0, "Failed to list the glusterfind session")
        g.log.info("Successfully listed the glusterfind session")

        # Delete the glusterfind session using the invalid values for optional
        # parameters
        g.log.info("Deleting the session with invalid values for the optional "
                   "parameters")
        ret, _, _ = g.run(self.mnode, ("glusterfind delete %s %s --dbug"
                                       % (self.volname, self.session)))
        self.assertNotEqual(ret, 0, "Unexpected: glusterfind session deleted "
                            "even with invalid value for optional parameters")
        g.log.info("Successful: glusterfind delete failed with invalid value "
                   "for optional parameters")

        # Delete the glusterfind session using the valid values for required
        # and optional parameters
        g.log.info("Deleting the session with valid values for the required "
                   "and optional parameters")
        ret, _, _ = gfind_delete(self.mnode, self.volname, self.session,
                                 debug=True)
        self.assertEqual(ret, 0, "Unexpected: Failed to delete the session "
                         "using the valid values for the required and "
                         "optional parameters")
        g.log.info("Successfully deleted the session using valid values for "
                   "required and optional parameters")

        # Perform glusterfind list to check if the session exists
        g.log.info("Performing glusterfind list to validate that the session "
                   "is deleted")
        ret, _, _ = gfind_list(self.mnode, volname=self.volname,
                               sessname=self.session)
        self.assertNotEqual(ret, 0, "Unexpected: glusterfind sessions is being"
                            " listed even after being deleted")
        g.log.info("Successful: No glusterfind session is listed")
