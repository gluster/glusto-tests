#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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

""" Build Verification Tests (BVT) : Component Verification Tests (CVT)
        Test Cases in this module tests the basic gluster operations sanity
        while IO is in progress. These tests verifies basic gluster features
        which should not be broken at all.

        Basic Gluster Operations tested:
            - add-brick
            - rebalance
            - set volume options which changes the client graphs
        TODO:
            - remove-brick
            - n/w failure followed by heal
            - replace-brick
            - enable quota
            - collecting snapshot
            - attach-tier, detach-tier
"""

import pytest
import time
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterVolumeBaseClass,
                                                   runs_on)
from glustolibs.gluster.volume_libs import enable_and_validate_volume_options
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online)
from glustolibs.gluster.volume_libs import (log_volume_info_and_status,
                                            expand_volume)
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete,
                                              rebalance_status)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs, log_mounts_info,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.gluster.exceptions import ExecutionError


class GlusterBasicFeaturesSanityBaseClass(GlusterVolumeBaseClass):
    """ BaseClass for all the gluster basic features sanity tests. """
    @classmethod
    def setUpClass(cls):
        """Setup Volume, Create Mounts and upload the necessary scripts to run
        tests.
        """
        # Sets up volume, mounts
        GlusterVolumeBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts")

        cls.counter = 1
        """int: Value of counter is used for dirname-start-num argument for
        file_dir_ops.py create_deep_dirs_with_files.

        The --dir-length argument value for
        file_dir_ops.py create_deep_dirs_with_files is set to 10
        (refer to the cmd in setUp method). This means every mount will create
        10 top level dirs. For every mountpoint/testcase to create new set of
        dirs, we are incrementing the counter by --dir-length value i.e 10
        in this test suite.

        If we are changing the --dir-length to new value, ensure the counter
        is also incremented by same value to create new set of files/dirs.
        """

    def setUp(self):
        """setUp starts the io from all the mounts.
            IO creates deep dirs and files.
        """
        # Calling BaseClass setUp
        GlusterVolumeBaseClass.setUp.im_func(self)

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 15 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (self.script_upload_path,
                                            self.counter,
                                            mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10
        self.io_validation_complete = False

        # Adding a delay of 15 seconds before test method starts. This
        # is to ensure IO's are in progress and giving some time to fill data
        time.sleep(15)

    def tearDown(self):
        """If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status
        """
        # Wait for IO to complete if io validation is not executed in the
        # test method
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")
        GlusterVolumeBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):
        """Cleanup data from mount and cleanup volume.
        """
        # Log Mounts info
        g.log.info("Log mounts info")
        log_mounts_info(cls.mounts)

        GlusterVolumeBaseClass.tearDownClass.im_func(cls)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class TestGlusterExpandVolumeSanity(GlusterBasicFeaturesSanityBaseClass):
    """Sanity tests for Expanding Volume"""
    @pytest.mark.bvt_cvt
    def test_expanding_volume_when_io_in_progress(self):
        """Test expanding volume (Increase distribution) using existing
        servers bricks when IO is in progress.

        Description:
            - add bricks
            - starts rebalance
            - wait for rebalance to complete
            - validate IO
        """
        # Log Volume Info and Status before expanding the volume.
        g.log.info("Logging volume info and Status before expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Expanding volume by adding bricks to the volume when IO in progress
        g.log.info("Start adding bricks to volume when IO in progress")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume when IO in "
                              "progress on volume %s", self.volname))
        g.log.info("Expanding volume when IO in progress is successful on "
                   "volume %s", self.volname)

        # Wait for gluster processes to come online
        time.sleep(30)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Start Rebalance
        g.log.info("Starting Rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Check Rebalance status
        g.log.info("Checking Rebalance status")
        ret, _, _ = rebalance_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to get rebalance status for the "
                                  "volume %s", self.volname))
        g.log.info("Successfully got rebalance status of the volume %s",
                   self.volname)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        # Check Rebalance status after rebalance is complete
        g.log.info("Checking Rebalance status")
        ret, _, _ = rebalance_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to get rebalance status for the "
                                  "volume %s", self.volname))
        g.log.info("Successfully got rebalance status of the volume %s",
                   self.volname)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class TestGlusterVolumeSetSanity(GlusterBasicFeaturesSanityBaseClass):
    """ Sanity tests for Volume Set operation
    """
    @pytest.mark.bvt_cvt
    def test_volume_set_when_io_in_progress(self):
        """Set Volume options which changes the client graphs while IO is
        in progress.

        Description:
            - set volume option uss, shard to 'enable' and
                validate it is successful
            - validate IO to be successful
        """
        # List of volume options to set
        volume_options_list = ["features.uss", "features.shard"]

        # enable and validate the volume options
        g.log.info("Setting the volume options: %s", volume_options_list)
        ret = enable_and_validate_volume_options(self.mnode, self.volname,
                                                 volume_options_list,
                                                 time_delay=30)
        self.assertTrue(ret, ("Unable to enable the volume options: %s",
                              volume_options_list))
        g.log.info("Successfully enabled all the volume options: %s",
                   volume_options_list)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")
