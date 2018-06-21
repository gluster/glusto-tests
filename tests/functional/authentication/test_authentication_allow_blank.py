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

""" Description
Test Case in this module is to set value of auth.allow to empty string
and check if it throws an error
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_init import is_glusterd_running
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import cleanup_volume


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'],
          ['glusterfs']])
class AuthAllowEmptyString(GlusterBaseClass):
    """
    Tests to verify auth.allow functionality on Volume and Fuse subdir
    """
    def setUp(self):
        """
        Setup Volume
        """
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup volume")
        g.log.info("Volume %s has been setup successfully", self.volname)

        # Calling GlusterBaseClass Setup
        GlusterBaseClass.setUp.im_func(self)

    def test_validate_authallow(self):
        """
        -Set Authentication allow as empty string for volume
        -Check if glusterd is running
        """
        # pylint: disable=too-many-statements

        # Set Authentication to blank string for volume
        option = {"auth.allow": " "}
        ret = set_volume_options(self.mnode, self.volname,
                                 option)
        self.assertFalse(ret, ("Unexpected: Authentication set successfully "
                               "for Volume with option: %s" % option))
        g.log.info("Expected: Failed to set authentication for Volume with "
                   "option: %s", option)

        # Check if glusterd is running
        ret = is_glusterd_running(self.servers)
        self.assertEqual(ret, 0, "Glusterd service not running")
        g.log.info("Expected : Glusterd service running")

    def tearDown(self):
        """
        TearDown for Volume
        Volume Cleanup
        """
        ret = cleanup_volume(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Failed to Cleanup the "
                                 "Volume %s" % self.volname)
        g.log.info("Volume deleted successfully "
                   ": %s", self.volname)
