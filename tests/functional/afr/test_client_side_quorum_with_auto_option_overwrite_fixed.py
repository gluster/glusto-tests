#  Copyright (C) 2016-2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_ops import get_volume_options
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.io.utils import is_io_procs_fail_with_error


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs']])
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
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    def setUp(self):
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

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
        self.get_super_method(self, 'tearDown')()

    def test_client_side_quorum_with_auto_option_overwrite_fixed(self):
        """
        Test Script to verify the Client Side Quorum with auto option

        * check the default value of cluster.quorum-type
        * try to set any junk value to cluster.quorum-type
          other than {none,auto,fixed}
        * check the default value of cluster.quorum-count
        * set cluster.quorum-type to fixed and cluster.quorum-count to 1
        * start I/O from the mount point
        * kill 2 of the brick process from the each replica set.
        * set cluster.quorum-type to auto

        """
        # pylint: disable=too-many-locals,too-many-statements
        # check the default value of cluster.quorum-type
        option = "cluster.quorum-type"
        g.log.info("Getting %s for the volume %s", option, self.volname)
        option_dict = get_volume_options(self.mnode, self.volname, option)
        self.assertIsNotNone(option_dict, ("Failed to get %s volume option"
                                           " for volume %s"
                                           % (option, self.volname)))
        self.assertEqual(option_dict['cluster.quorum-type'], 'auto',
                         ("Default value for %s is not auto"
                          " for volume %s" % (option, self.volname)))
        g.log.info("Succesfully verified default value of %s for volume %s",
                   option, self.volname)

        # set the junk value to cluster.quorum-type
        junk_values = ["123", "abcd", "fixxed", "Aauto"]
        for each_junk_value in junk_values:
            options = {"cluster.quorum-type": "%s" % each_junk_value}
            g.log.info("setting %s for the volume "
                       "%s", options, self.volname)
            ret = set_volume_options(self.mnode, self.volname, options)
            self.assertFalse(ret, ("Able to set junk value %s for "
                                   "volume %s" % (options, self.volname)))
            g.log.info("Expected: Unable to set junk value %s "
                       "for volume %s", options, self.volname)

        # check the default value of cluster.quorum-count
        option = "cluster.quorum-count"
        g.log.info("Getting %s for the volume %s", option, self.volname)
        option_dict = get_volume_options(self.mnode, self.volname, option)
        self.assertIsNotNone(option_dict, ("Failed to get %s volume option"
                                           " for volume %s"
                                           % (option, self.volname)))
        self.assertEqual(option_dict['cluster.quorum-count'], '(null)',
                         ("Default value for %s is not null"
                          " for volume %s" % (option, self.volname)))
        g.log.info("Successful in getting %s for the volume %s",
                   option, self.volname)

        # set cluster.quorum-type to fixed and cluster.quorum-count to 1
        options = {"cluster.quorum-type": "fixed",
                   "cluster.quorum-count": "1"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # create files
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        cmd = ("/usr/bin/env python %s create_files "
               "-f 10 --base-file-name file %s" % (
                   self.script_upload_path,
                   self.mounts[0].mountpoint))
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "IO failed on %s with '%s'"
                         % (self.mounts[0].client_system, err))

        # get the subvolumes
        g.log.info("starting to get subvolumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s is %s",
                   self.volname, num_subvols)

        # bring bricks offline( 2 bricks ) for all the subvolumes
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            g.log.info("sub-volume %s brick list : %s",
                       i, subvol_brick_list)
            bricks_to_bring_offline = subvol_brick_list[0:2]
            g.log.info("Going to bring down the brick process "
                       "for %s", bricks_to_bring_offline)
            ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
            self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                                  "check the log file for more details."))
            g.log.info("Brought down the brick process "
                       "for %s successfully", bricks_to_bring_offline)

        # create files
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        cmd = ("/usr/bin/env python %s create_files "
               "-f 10 --base-file-name second_file %s" % (
                   self.script_upload_path,
                   self.mounts[0].mountpoint))
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "IO failed on %s with '%s'"
                         % (self.mounts[0].client_system, err))

        # set cluster.quorum-type to auto
        options = {"cluster.quorum-type": "auto"}
        g.log.info("setting %s for volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for "
                              "volume %s" % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # create files
        all_mounts_procs = []
        g.log.info("Starting IO on mountpount...")
        g.log.info("mounts: %s", self.mounts)
        cmd = ("mkdir %s/newdir && touch %s/newdir/myfile{1..3}.txt"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with "
                   "Transport endpoint is not connected")
        ret, _ = is_io_procs_fail_with_error(self, all_mounts_procs,
                                             self.mounts, self.mount_type)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on not connected transport endpoint"))
        g.log.info("EXPECTED: Transport endpoint is not connected"
                   " while creating files")
