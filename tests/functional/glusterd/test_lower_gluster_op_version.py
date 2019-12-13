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
        Test Cases in this module related to Glusterd volume reset validation
        with bitd, scrub and snapd daemons running or not
"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.volume_libs import (get_volume_options,
                                            set_volume_options)


@runs_on([['replicated'], ['glusterfs']])
class LowerGlusterOpVersion(GlusterBaseClass):

    def setUp(self):

        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed")
        g.log.info("Volume created successfully : %s", self.volname)

    def tearDown(self):

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
        # stopping the volume and Cleaning up the volume
        ret = cleanup_volume(self.mnode, self.volname)
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume")
        g.log.info("Volume deleted successfully : %s", self.volname)

    def test_lower_gluster_op_version(self):
        """
        - Create volume
        - Get the volume op-version
        - Set the valid lower op-version
        - Set the invalid op-version
        """

        # Get the volume op-version
        ret = get_volume_options(self.mnode, self.volname,
                                 'cluster.op-version')
        self.assertIsNotNone(ret, "Failed to get the op-version")
        g.log.info("Successfully get the op-version")

        # Lowest opversion is 30000
        lowest_op_version = 30000
        invalid_op_version = "abc"
        lower_op_version_dict = {'cluster.op-version': lowest_op_version}
        invalid_op_version_dict = {'cluster.op-version': invalid_op_version}

        # Set the volume option with lower op-version
        ret = set_volume_options(self.mnode, 'all',
                                 lower_op_version_dict)
        self.assertFalse(ret, "Expected: Should not be able to set lower "
                         "op-version \n Actual: Successfully set the lower"
                         " op-version")
        g.log.info("Failed to set op-version %s as "
                   "expected", lowest_op_version)

        # Setting invalid opversion
        ret = set_volume_options(self.mnode, 'all',
                                 invalid_op_version_dict)
        self.assertFalse(ret, "Expected: Should not be able to set invalid "
                         "op-version \n Actual: Successfully set the invalid"
                         " op-version")
        g.log.info("Failed to set op-version %s as "
                   "expected", invalid_op_version)
