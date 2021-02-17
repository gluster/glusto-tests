#  Copyright (C) 2015-2020  Red Hat, Inc. <http://www.redhat.com>
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
            - enable quota
            - collecting snapshot
            - remove-brick
            - n/w failure followed by heal
            - replace-brick
"""
import time

from glusto.core import Glusto as g
import pytest

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.volume_libs import enable_and_validate_volume_options
from glustolibs.gluster.volume_libs import (
    verify_all_process_of_volume_are_online)
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status, expand_volume, shrink_volume,
    replace_brick_from_volume, wait_for_volume_process_to_be_online)
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_rebalance_to_complete,
                                              rebalance_status)
from glustolibs.gluster.brick_libs import (select_bricks_to_bring_offline,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline)
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.quota_ops import (quota_enable, quota_disable,
                                          quota_limit_usage,
                                          is_quota_enabled,
                                          quota_fetch_list)
from glustolibs.gluster.snap_ops import (snap_create, get_snap_list,
                                         snap_activate, snap_deactivate)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 view_snaps_from_mount,
                                 wait_for_io_to_complete)
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.exceptions import ExecutionError


class GlusterBasicFeaturesSanityBaseClass(GlusterBaseClass):
    """ BaseClass for all the gluster basic features sanity tests. """
    @classmethod
    def setUpClass(cls):
        """Upload the necessary scripts to run tests.
        """
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        cls.counter = 1

        # Temporary code:
        # Additional checks to gather infomartion from all
        # servers for Bug 1810901 and setting log level to debug.
        ret = set_volume_options(cls.mnode, 'all',
                                 {'cluster.daemon-log-level': 'DEBUG'})
        if not ret:
            g.log.error('Failed to set cluster.daemon-log-level to DEBUG')
        # int: Value of counter is used for dirname-start-num argument for
        # file_dir_ops.py create_deep_dirs_with_files.

        # The --dir-length argument value for file_dir_ops.py
        # create_deep_dirs_with_files is set to 10 (refer to the cmd in setUp
        # method). This means every mount will create
        # 10 top level dirs. For every mountpoint/testcase to create new set of
        # dirs, we are incrementing the counter by --dir-length value i.e 10 in
        # this test suite.

        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

    def setUp(self):
        """
        - Setup Volume and Mount Volume
        - setUp starts the io from all the mounts.
        - IO creates deep dirs and files.
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

        # Temporary code:
        # Additional checks to gather infomartion from all
        # servers for Bug 1810901 and setting log level to debug.
        for opt in ('diagnostics.brick-log-level',
                    'diagnostics.client-log-level',
                    'diagnostics.brick-sys-log-level',
                    'diagnostics.client-sys-log-level'):
            ret = set_volume_options(self.mnode, self.volname,
                                     {opt: 'DEBUG'})
            if not ret:
                g.log.error('Failed to set volume option %s', opt)

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path,
                       self.counter, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter += 10
        self.io_validation_complete = False

        # Adding a delay of 15 seconds before test method starts. This
        # is to ensure IO's are in progress and giving some time to fill data
        time.sleep(15)

    def tearDown(self):
        """If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Unmount Volume and Cleanup Volume
        """
        # Wait for IO to complete if io validation is not executed in the
        # test method
        if not self.io_validation_complete:
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")

            # List all files and dirs created
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")

        # Unmount Volume and Cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()


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
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Expanding volume by adding bricks to the volume when IO in progress
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume when IO in "
                              "progress on volume %s", self.volname))

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))

        # Log Volume Info and Status after expanding the volume
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))

        # Start Rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Log Rebalance status
        g.log.info("Log Rebalance status")
        _, _, _ = rebalance_status(self.mnode, self.volname)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname,
                                             timeout=1800)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        # Check Rebalance status after rebalance is complete
        ret, _, _ = rebalance_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to get rebalance status for the "
                                  "volume %s", self.volname))
        g.log.info("Successfully got rebalance status of the volume %s",
                   self.volname)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")


@runs_on([['distributed', 'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class TestGlusterShrinkVolumeSanity(GlusterBasicFeaturesSanityBaseClass):
    """Sanity tests for Shrinking Volume"""
    @pytest.mark.bvt_cvt
    def test_shrinking_volume_when_io_in_progress(self):
        """Test shrinking volume (Decrease distribute count) using existing
        servers bricks when IO is in progress.

        Description:
            - remove brick (start, status, commit)
            - validate IO
        """
        # Log Volume Info and Status before shrinking the volume.
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Shrinking volume by removing bricks from volume when IO in progress
        ret = shrink_volume(self.mnode, self.volname)

        self.assertTrue(ret, ("Failed to shrink the volume when IO in "
                              "progress on volume %s", self.volname))
        g.log.info("Shrinking volume when IO in progress is successful on "
                   "volume %s", self.volname)

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))

        # Log Volume Info and Status after shrinking the volume
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")


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
        ret = enable_and_validate_volume_options(self.mnode, self.volname,
                                                 volume_options_list,
                                                 time_delay=30)
        self.assertTrue(ret, ("Unable to enable the volume options: %s",
                              volume_options_list))

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class TestQuotaSanity(GlusterBasicFeaturesSanityBaseClass):
    """ Sanity tests for Gluster Quota
    """
    @pytest.mark.bvt_cvt
    def test_quota_enable_disable_enable_when_io_in_progress(self):
        """Enable, Disable and Re-enable Quota on the volume when IO is
            in progress.
        """
        # Enable Quota
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Check if quota is enabled
        ret = is_quota_enabled(self.mnode, self.volname)
        self.assertTrue(ret, ("Quota is not enabled on the volume %s",
                              self.volname))
        g.log.info("Successfully Validated quota is enabled on volume %s",
                   self.volname)

        # Path to set quota limit
        path = "/"

        # Set Quota limit on the root of the volume
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="1GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  " the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

        # quota_fetch_list
        quota_list = quota_fetch_list(self.mnode, self.volname, path=path)
        self.assertIsNotNone(quota_list, ("Failed to get the quota list for "
                                          "path %s of the volume %s",
                                          path, self.volname))
        self.assertIn(path, quota_list.keys(),
                      ("%s not part of the ""quota list %s even if "
                       "it is set on the volume %s", path,
                       quota_list, self.volname))
        g.log.info("Successfully listed path %s in the quota list %s of the "
                   "volume %s", path, quota_list, self.volname)

        # Disable quota
        ret, _, _ = quota_disable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to disable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully disabled quota on the volume %s",
                   self.volname)

        # Check if quota is still enabled (expected : Disabled)
        ret = is_quota_enabled(self.mnode, self.volname)
        self.assertFalse(ret, ("Quota is still enabled on the volume %s "
                               "(expected: Disable) ", self.volname))
        g.log.info("Successfully Validated quota is disabled on volume %s",
                   self.volname)

        # Enable Quota
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Check if quota is enabled
        ret = is_quota_enabled(self.mnode, self.volname)
        self.assertTrue(ret, ("Quota is not enabled on the volume %s",
                              self.volname))
        g.log.info("Successfully Validated quota is enabled on volume %s",
                   self.volname)

        # quota_fetch_list
        quota_list = quota_fetch_list(self.mnode, self.volname, path=path)
        self.assertIsNotNone(quota_list, ("Failed to get the quota list for "
                                          "path %s of the volume %s",
                                          path, self.volname))
        self.assertIn(path, quota_list.keys(),
                      ("%s not part of the quota list %s even if "
                       "it is set on the volume %s", path,
                       quota_list, self.volname))
        g.log.info("Successfully listed path %s in the quota list %s of the "
                   "volume %s", path, quota_list, self.volname)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs', 'cifs']])
class TestSnapshotSanity(GlusterBasicFeaturesSanityBaseClass):
    """ Sanity tests for Gluster Snapshots
    """
    @pytest.mark.bvt_cvt
    def test_snapshot_basic_commands_when_io_in_progress(self):
        """Create, List, Activate, Enable USS (User Serviceable Snapshot),
            Viewing Snap of the volume from mount, De-Activate
            when IO is in progress.
        """
        snap_name = "snap_cvt"
        # Create Snapshot
        ret, _, _ = snap_create(self.mnode, self.volname, snap_name)
        self.assertEqual(ret, 0, ("Failed to create snapshot with name %s "
                                  " of the volume %s", snap_name,
                                  self.volname))
        g.log.info("Successfully created snapshot %s of the volume %s",
                   snap_name, self.volname)

        # List Snapshot
        snap_list = get_snap_list(self.mnode)
        self.assertIsNotNone(snap_list, "Unable to get the Snapshot list")
        self.assertIn(snap_name, snap_list,
                      ("snapshot %s not listed in Snapshot list", snap_name))
        g.log.info("Successfully listed snapshot %s in gluster snapshot list",
                   snap_name)

        # Activate the snapshot
        ret, _, _ = snap_activate(self.mnode, snap_name)
        self.assertEqual(ret, 0, ("Failed to activate snapshot with name %s "
                                  " of the volume %s", snap_name,
                                  self.volname))
        g.log.info("Successfully activated snapshot %s of the volume %s",
                   snap_name, self.volname)

        # Enable USS on the volume.
        uss_options = ["features.uss"]
        if self.mount_type == "cifs":
            uss_options.append("features.show-snapshot-directory")
        ret = enable_and_validate_volume_options(self.mnode, self.volname,
                                                 uss_options,
                                                 time_delay=30)
        self.assertTrue(ret, ("Unable to enable uss options %s on volume %s",
                              uss_options, self.volname))
        g.log.info("Successfully enabled uss options %s on the volume: %s",
                   uss_options, self.volname)

        # Viewing snapshot from mount
        ret = view_snaps_from_mount(self.mounts, snap_name)
        self.assertTrue(ret, ("Failed to View snap %s from mounts", snap_name))
        g.log.info("Successfully viewed snap %s from mounts", snap_name)

        # De-Activate the snapshot
        ret, _, _ = snap_deactivate(self.mnode, snap_name)
        self.assertEqual(ret, 0, ("Failed to deactivate snapshot with name %s "
                                  " of the volume %s", snap_name,
                                  self.volname))
        g.log.info("Successfully deactivated snapshot %s of the volume %s",
                   snap_name, self.volname)

        # Viewing snapshot from mount (.snaps shouldn't be listed from mount)
        for mount_obj in self.mounts:
            ret = view_snaps_from_mount(mount_obj, snap_name)
            self.assertFalse(ret, ("Still able to View snap %s from mount "
                                   "%s:%s", snap_name,
                                   mount_obj.client_system,
                                   mount_obj.mountpoint))
            g.log.info("%s not listed under .snaps from mount %s:%s",
                       snap_name, mount_obj.client_system,
                       mount_obj.mountpoint)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class TestGlusterReplaceBrickSanity(GlusterBasicFeaturesSanityBaseClass):
    """Sanity tests for Replacing faulty bricks"""
    @pytest.mark.bvt_cvt
    def test_replace_brick_when_io_in_progress(self):
        """Test replacing brick using existing servers bricks when IO is
            in progress.

        Description:
            - replace_brick
            - wait for heal to complete
            - validate IO
        """
        # Log Volume Info and Status before replacing brick from the volume.
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Replace brick from a sub-volume
        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        self.servers, self.all_servers_info)
        self.assertTrue(ret, "Failed to replace faulty brick from the volume")
        g.log.info("Successfully replaced faulty brick from the volume")

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))

        # Log Volume Info and Status after replacing the brick
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))

        # Wait for self-heal to complete
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=1800)
        self.assertTrue(ret, "Self heal didn't complete even after waiting "
                        "for 30 minutes. 30 minutes is too much a time for "
                        "current test workload")

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")


# This test is disabled on nfs because of bug 1473668. A patch to apply the
# workaround mentioned on the bug could not make this test green either.
@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'],
          ['glusterfs', 'cifs']])
class TestGlusterHealSanity(GlusterBasicFeaturesSanityBaseClass):
    """Sanity tests for SelfHeal"""
    @pytest.mark.bvt_cvt
    def test_self_heal_when_io_in_progress(self):
        """Test self-heal is successful when IO is in progress.

        Description:
            - simulate brick down.
            - bring bricks online
            - wait for heal to complete
            - validate IO
        """
        # pylint: disable=too-many-statements
        # Check if volume type is dispersed. If the volume type is
        # dispersed, set the volume option 'disperse.optimistic-change-log'
        # to 'off'
        # Refer to: https://bugzilla.redhat.com/show_bug.cgi?id=1470938
        # pylint: disable=unsupported-membership-test
        if 'dispersed' in self.volume_type and 'nfs' in self.mount_type:
            g.log.info("Set volume option 'disperse.optimistic-change-log' "
                       "to 'off' on a dispersed volume . "
                       "Refer to bug: "
                       "https://bugzilla.redhat.com/show_bug.cgi?id=1470938")
            ret = set_volume_options(self.mnode, self.volname,
                                     {'disperse.optimistic-change-log': 'off'})
            self.assertTrue(ret, ("Failed to set the volume option %s to "
                                  "off on volume %s",
                                  'disperse.optimistic-change-log',
                                  self.volname))
            g.log.info("Successfully set the volume option "
                       "'disperse.optimistic-change-log' to 'off'")

        # Log Volume Info and Status before simulating brick failure
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = bricks_to_bring_offline_dict['volume_bricks']

        # Bring bricks offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, ("Failed to bring bricks: %s offline",
                              bricks_to_bring_offline))

        # Log Volume Info and Status
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Validate if bricks are offline
        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, ("Not all the bricks in list: %s are offline",
                              bricks_to_bring_offline))

        # Add delay before bringing bricks online
        time.sleep(40)

        # Bring bricks online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, ("Failed to bring bricks: %s online",
                              bricks_to_bring_offline))
        g.log.info("Successfully brought all bricks:%s online",
                   bricks_to_bring_offline)

        # Wait for volume processes to be online
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname,
                                                   timeout=400)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))

        # Log Volume Info and Status
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Verify volume's all process are online
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Wait for self-heal to complete
        g.log.info("Wait for self-heal to complete")
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=1800)
        self.assertTrue(ret, "Self heal didn't complete even after waiting "
                        "for 30 minutes. 30 minutes is too much a time for "
                        "current test workload")
        g.log.info("self-heal is successful after replace-brick operation")

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
