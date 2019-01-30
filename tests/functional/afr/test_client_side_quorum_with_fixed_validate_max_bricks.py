#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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
        Test Cases in this module tests the client side quorum.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (
    set_volume_options, get_subvols)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class ClientSideQuorumTests(GlusterBaseClass):
    """
    ClientSideQuorumTests contains tests which verifies the
    client side quorum Test Cases
    """
    @classmethod
    def setUpClass(cls):
        """
        Upload the necessary scripts to run tests.
        """

        # calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_abs_path = "/usr/share/glustolibs/io/scripts/file_dir_ops.py"
        cls.script_upload_path = script_abs_path
        ret = upload_scripts(cls.clients, script_abs_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    def setUp(self):
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        tearDown for every test
        """

        # stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_client_side_quorum_with_fixed_validate_max_bricks(self):
        """
        Test Script with Client Side Quorum with fixed should validate
        maximum number of bricks to accept

        * set cluster quorum to fixed
        * set cluster.quorum-count to higher number which is greater than
          number of replicas in a sub-voulme
        * Above step should fail

        """

        # set cluster.quorum-type to fixed
        options = {"cluster.quorum-type": "fixed"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s is %s", self.volname,
                   num_subvols)

        # get the number of bricks in replica set
        num_bricks_in_subvol = len(subvols_dict['volume_subvols'][0])
        g.log.info("Number of bricks in each replica set : %s",
                   num_bricks_in_subvol)

        # set cluster.quorum-count to higher value than the number of bricks in
        # repliac set
        start_range = num_bricks_in_subvol + 1
        end_range = num_bricks_in_subvol + 30
        for i in range(start_range, end_range):
            options = {"cluster.quorum-count": "%s" % i}
            g.log.info("setting %s for the volume %s", options, self.volname)
            ret = set_volume_options(self.mnode, self.volname, options)
            self.assertFalse(ret, ("Able to set %s for volume %s, quorum-count"
                                   " should not be greater than number of"
                                   " bricks in replica set"
                                   % (options, self.volname)))
        g.log.info("Expected: Unable to set %s for volume %s, "
                   "quorum-count should be less than number of bricks "
                   "in replica set", options, self.volname)
