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
        Test Cases in this module tests the nfs ganesha exports,
        refresh configs, cluster enable/disable functionality.
"""

from copy import deepcopy
import os
import re
from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.nfs_ganesha_libs import (
    NfsGaneshaClusterSetupClass,
    wait_for_nfs_ganesha_volume_to_get_exported,
    wait_for_nfs_ganesha_volume_to_get_unexported)
from glustolibs.gluster.nfs_ganesha_ops import (
    is_nfs_ganesha_cluster_in_healthy_state, set_acl, refresh_config,
    enable_nfs_ganesha, disable_nfs_ganesha, export_nfs_ganesha_volume,
    unexport_nfs_ganesha_volume)
from glustolibs.gluster.volume_ops import (volume_stop, volume_start)
from glustolibs.gluster.volume_libs import (
    get_volume_options, setup_volume, cleanup_volume,
    log_volume_info_and_status, wait_for_volume_process_to_be_online,
    volume_exists)
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.lib_utils import get_servers_unused_bricks_dict


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaVolumeExports(NfsGaneshaClusterSetupClass):
    """
        Tests to verify Nfs Ganesha exports, cluster enable/disable
        functionality.
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup nfs-ganesha if not exists.
        """
        cls.get_super_method(cls, 'setUpClass')()

        # Setup nfs-ganesha if not exists.
        ret = cls.setup_nfs_ganesha()
        if not ret:
            raise ExecutionError("Failed to setup nfs-ganesha cluster")
        g.log.info("nfs-ganesha cluster is healthy")

    def setUp(self):
        """
        Setup and mount volume
        """
        g.log.info("Starting to setup and mount volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s"
                                 % self.volname)
        g.log.info("Successful in setup and mount volume %s", self.volname)

    def test_nfs_ganesha_export_after_vol_restart(self):
        """
        Tests script to check nfs-ganesha volume gets exported after
        multiple volume restarts.
        """
        for i in range(1, 6):
            g.log.info("Testing nfs ganesha export after volume stop/start."
                       "Count : %s", str(i))

            # Stopping volume
            ret = volume_stop(self.mnode, self.volname)
            self.assertTrue(ret, ("Failed to stop volume %s" % self.volname))

            # Waiting for few seconds for volume unexport. Max wait time is
            # 120 seconds.
            ret = wait_for_nfs_ganesha_volume_to_get_unexported(self.mnode,
                                                                self.volname)
            self.assertTrue(ret, ("Failed to unexport volume %s after "
                                  "stopping volume" % self.volname))

            # Starting volume
            ret = volume_start(self.mnode, self.volname)
            self.assertTrue(ret, ("Failed to start volume %s" % self.volname))

            # Waiting for few seconds for volume export. Max wait time is
            # 120 seconds.
            ret = wait_for_nfs_ganesha_volume_to_get_exported(self.mnode,
                                                              self.volname)
            self.assertTrue(ret, ("Failed to export volume %s after "
                                  "starting volume" % self.volname))

    def test_nfs_ganesha_enable_disable_cluster(self):
        """
        Tests script to check nfs-ganehsa volume gets exported after
        multiple enable/disable of cluster.
        """
        for i in range(1, 6):
            g.log.info("Executing multiple enable/disable of nfs ganesha "
                       "cluster. Count : %s ", str(i))

            ret, _, _ = disable_nfs_ganesha(self.mnode)
            self.assertEqual(ret, 0, "Failed to disable nfs-ganesha cluster")

            sleep(2)
            vol_option = get_volume_options(self.mnode, self.volname,
                                            option='ganesha.enable')
            if vol_option is None:
                self.assertEqual(ret, 0, ("Failed to get ganesha.enable volume"
                                          " option for %s " % self.volume))

            self.assertEqual(vol_option.get('ganesha.enable'), 'off', "Failed "
                             "to unexport volume by default after disabling "
                             "cluster")

            ret, _, _ = enable_nfs_ganesha(self.mnode)
            self.assertEqual(ret, 0, "Failed to enable nfs-ganesha cluster")

            # Check nfs-ganesha status
            for itr in range(5):
                ret = is_nfs_ganesha_cluster_in_healthy_state(self.mnode)
                if ret:
                    g.log.info("nfs-ganesha cluster is healthy")
                    break
                g.log.warning("nfs-ganesha cluster is not healthy. "
                              "Iteration: %s", str(itr))
                self.assertEqual(itr, 4, "Wait timeout: nfs-ganesha cluster "
                                         "is not healthy")
                sleep(3)

            vol_option = get_volume_options(self.mnode, self.volname,
                                            option='ganesha.enable')
            if vol_option is None:
                self.assertEqual(ret, 0, ("Failed to get ganesha.enable volume"
                                          " option for %s " % self.volume))

            self.assertEqual(vol_option.get('ganesha.enable'), 'off', "Volume "
                             "%s is exported by default after disable and "
                             "enable of cluster which is unexpected." %
                             self.volname)

            # Export volume after disable and enable of cluster
            ret, _, _ = export_nfs_ganesha_volume(mnode=self.mnode,
                                                  volname=self.volname)
            self.assertEqual(ret, 0, ("Failed to export volume %s "
                                      "after disable and enable of cluster"
                                      % self.volname))

            # Wait for volume to get exported
            ret = wait_for_nfs_ganesha_volume_to_get_exported(self.mnode,
                                                              self.volname)
            self.assertTrue(ret, "Volume %s is not exported after setting "
                                 "ganesha.enable 'on'" % self.volname)
            g.log.info("Exported volume after enabling nfs-ganesha cluster")

    def test_nfs_ganesha_exportID_after_vol_restart(self):
        """
        Tests script to check nfs-ganesha volume gets exported with same
        Export ID after multiple volume restarts.
        Steps:
        1. Create and Export the Volume
        2. Stop and Start the volume multiple times
        3. Check for export ID
           Export ID should not change
        """
        for i in range(1, 4):
            g.log.info("Testing nfs ganesha exportID after volume stop and "
                       "start.\n Count : %s", str(i))

            # Stopping volume
            ret = volume_stop(self.mnode, self.volname)
            self.assertTrue(ret, ("Failed to stop volume %s" % self.volname))
            g.log.info("Volume is stopped")

            # Waiting for few seconds for volume unexport. Max wait time is
            # 120 seconds.
            ret = wait_for_nfs_ganesha_volume_to_get_unexported(self.mnode,
                                                                self.volname)
            self.assertTrue(ret, ("Failed to unexport volume %s after "
                                  "stopping volume" % self.volname))
            g.log.info("Volume is unexported via ganesha")

            # Starting volume
            ret = volume_start(self.mnode, self.volname)
            self.assertTrue(ret, ("Failed to start volume %s" % self.volname))
            g.log.info("Volume is started")

            # Waiting for few seconds for volume export. Max wait time is
            # 120 seconds.
            ret = wait_for_nfs_ganesha_volume_to_get_exported(self.mnode,
                                                              self.volname)
            self.assertTrue(ret, ("Failed to export volume %s after "
                                  "starting volume" % self.volname))
            g.log.info("Volume is exported via ganesha")

            # Check for Export ID
            cmd = ("cat /run/gluster/shared_storage/nfs-ganesha/exports/"
                   "export.*.conf | grep Export_Id | grep -Eo '[0-9]'")
            ret, out, _ = g.run(self.mnode, cmd)
            self.assertEqual(ret, 0, "Unable to get export ID of the volume %s"
                             % self.volname)
            g.log.info("Successful in getting volume export ID: %s " % out)
            self.assertEqual(out.strip("\n"), "2",
                             "Export ID changed after export and unexport "
                             "of volume: %s" % out)
            g.log.info("Export ID of volume is same after export "
                       "and export: %s" % out)

    def tearDown(self):
        """
        Unexport volume
        Unmount and cleanup volume
        """
        # Unexport volume
        unexport_nfs_ganesha_volume(self.mnode, self.volname)
        ret = wait_for_nfs_ganesha_volume_to_get_unexported(self.mnode,
                                                            self.volname)
        if not ret:
            raise ExecutionError("Volume %s is not unexported" % self.volname)

        # Unmount volume
        ret = self.unmount_volume(self.mounts)
        if ret:
            g.log.info("Successfully unmounted the volume")
        else:
            g.log.error("Failed to unmount volume")

        # Cleanup volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Cleanup volume %s completed successfully", self.volname)

    @classmethod
    def tearDownClass(cls):
        cls.get_super_method(cls, 'tearDownClass')(
            delete_nfs_ganesha_cluster=False)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaVolumeExportsWithIO(NfsGaneshaClusterSetupClass):
    """
    Tests to verify nfs ganesha features when IO is in progress.
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup nfs-ganesha if not exists.
        Upload IO scripts to clients
        """
        cls.get_super_method(cls, 'setUpClass')()

        # Setup nfs-ganesha if not exists.
        ret = cls.setup_nfs_ganesha()
        if not ret:
            raise ExecutionError("Failed to setup nfs-ganesha cluster")
        g.log.info("nfs-ganesha cluster is healthy")

        # Upload IO scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        Setup and mount volume
        """
        g.log.info("Starting to setup and mount volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s"
                                 % self.volname)
        g.log.info("Successful in setup and mount volume %s", self.volname)

    def test_nfs_ganesha_multiple_refresh_configs(self):
        """
        Tests script to check nfs-ganehsa volume gets exported and IOs
        are running after running multiple refresh configs.
        """
        self.acl_check_flag = False

        # Starting IO on the mounts
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        for itr in range(1, 7):
            # Enabling/Disabling ACLs to modify the export configuration
            # before running refresh config
            if itr % 2 == 0:
                # Disable ACL
                ret = set_acl(self.mnode, self.volname, False, False)
                self.assertTrue(ret, ("Failed to disable acl on %s"
                                      % self.volname))
                self.acl_check_flag = False
            else:
                # Enable ACL
                ret = set_acl(self.mnode, self.volname, True, False)
                self.assertTrue(ret, ("Failed to enable acl on %s"
                                      % self.volname))
                self.acl_check_flag = True

            ret = refresh_config(self.mnode, self.volname)
            self.assertTrue(ret, ("Failed to run refresh config"
                                  "for volume %s" % self.volname))
            g.log.info("Refresh-config completed. Iteration:%s", str(itr))

        # Check nfs-ganesha cluster status
        ret = is_nfs_ganesha_cluster_in_healthy_state(self.mnode)
        self.assertTrue(ret, "nfs-ganesha cluster is not healthy after "
                             "multiple refresh-config operations")

        # Validate IO
        g.log.info("Validating IO")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all IO")

    def tearDown(self):
        """
        Disbale ACL if enabled
        Unexport volume
        Unmount and cleanup volume
        """
        if self.acl_check_flag:
            ret = set_acl(self.mnode, self.volname, False)
            if not ret:
                raise ExecutionError("Failed to disable acl on %s"
                                     % self.volname)
            self.acl_check_flag = False

        # Unexport volume
        unexport_nfs_ganesha_volume(self.mnode, self.volname)
        ret = wait_for_nfs_ganesha_volume_to_get_unexported(self.mnode,
                                                            self.volname)
        if not ret:
            raise ExecutionError("Volume %s is not unexported." % self.volname)

        # Unmount volume
        ret = self.unmount_volume(self.mounts)
        if ret:
            g.log.info("Successfully unmounted the volume")
        else:
            g.log.error("Failed to unmount volume")

        # Cleanup volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Cleanup volume %s completed successfully", self.volname)

    @classmethod
    def tearDownClass(cls):
        cls.get_super_method(cls, 'tearDownClass')(
            delete_nfs_ganesha_cluster=False)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaMultiVolumeExportsWithIO(NfsGaneshaClusterSetupClass):
    """
    Tests to verify multiple volumes gets exported when IO is in progress.
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup nfs-ganesha if not exists.
        Upload IO scripts to clients
        """
        cls.get_super_method(cls, 'setUpClass')()

        # Setup nfs-ganesha if not exists.
        ret = cls.setup_nfs_ganesha()
        if not ret:
            raise ExecutionError("Failed to setup nfs-ganesha cluster")
        g.log.info("nfs-ganesha cluster is healthy")

        # Upload IO scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        """
        Setup and mount volume
        """
        g.log.info("Starting to setup and mount volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s"
                                 % self.volname)
        g.log.info("Successful in setup and mount volume %s", self.volname)

    def test_nfs_ganesha_export_with_multiple_volumes(self):
        """
        Test case to verify multiple volumes gets exported when IO is in
        progress.
        """
        # Starting IO on the mounts
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Create and export five new volumes
        for i in range(5):
            # Check availability of bricks to create new volume
            num_of_unused_bricks = 0

            servers_unused_bricks_dict = get_servers_unused_bricks_dict(
                self.mnode, self.all_servers, self.all_servers_info)
            for each_server_unused_bricks_list in list(
                    servers_unused_bricks_dict.values()):
                num_of_unused_bricks = (num_of_unused_bricks +
                                        len(each_server_unused_bricks_list))

            if num_of_unused_bricks < 2:
                self.assertNotEqual(i, 0, "New volume cannot be created due "
                                          "to unavailability of bricks.")
                g.log.warning("Tried to create five new volumes. But could "
                              "create only %s volume due to unavailability "
                              "of bricks.", str(i))
                break

            self.volume['name'] = "nfsvol" + str(i)
            self.volume['voltype']['type'] = 'distributed'
            self.volume['voltype']['replica_count'] = 1
            self.volume['voltype']['dist_count'] = 2

            new_vol = self.volume['name']

            # Create volume
            ret = setup_volume(mnode=self.mnode,
                               all_servers_info=self.all_servers_info,
                               volume_config=self.volume, force=True)
            if not ret:
                self.assertTrue(ret, "Setup volume [%s] failed" % self.volume)

            g.log.info("Wait for volume processes to be online")
            ret = wait_for_volume_process_to_be_online(self.mnode, new_vol)
            self.assertTrue(ret, "Volume %s process not online despite "
                                 "waiting for 300 seconds" % new_vol)

            # Export volume with nfs ganesha
            ret, _, _ = export_nfs_ganesha_volume(mnode=self.mnode,
                                                  volname=new_vol)
            self.assertEqual(ret, 0, ("Failed to export volume %s "
                                      "using nfs-ganesha" % new_vol))

            # Wait for volume to get exported
            ret = wait_for_nfs_ganesha_volume_to_get_exported(self.mnode,
                                                              new_vol)
            self.assertTrue(ret, "Volume %s is not exported after setting "
                                 "ganesha.enable 'on'" % new_vol)
            g.log.info("Exported nfs-ganesha volume %s", new_vol)

            # Log Volume Info and Status
            ret = log_volume_info_and_status(self.mnode, new_vol)
            self.assertTrue(ret, "Logging volume %s info and status failed"
                            % new_vol)

        # Validate IO
        g.log.info("Validating IO")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all IO")

    def tearDown(self):
        """
        Unexport volumes
        Unmount and cleanup volumes
        """
        # Cleanup volumes created in test case
        for i in range(5):
            vol = "nfsvol" + str(i)
            # Unexport and cleanup volume if exists
            if volume_exists(self.mnode, vol):
                unexport_nfs_ganesha_volume(self.mnode, vol)
                ret = wait_for_nfs_ganesha_volume_to_get_unexported(
                    self.mnode, vol)
                if not ret:
                    raise ExecutionError("Volume %s is not unexported." % vol)

                ret = cleanup_volume(mnode=self.mnode, volname=vol)
                if not ret:
                    raise ExecutionError("Cleanup volume %s failed" % vol)

        # Unexport pre existing volume
        unexport_nfs_ganesha_volume(self.mnode, self.volname)
        ret = wait_for_nfs_ganesha_volume_to_get_unexported(self.mnode,
                                                            self.volname)
        if not ret:
            raise ExecutionError("Volume %s is not unexported." % self.volname)

        # Unmount pre existing volume
        ret = self.unmount_volume(self.mounts)
        if ret:
            g.log.info("Successfully unmounted the volume")
        else:
            g.log.error("Failed to unmount volume")

        # Cleanup pre existing volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Cleanup volume %s completed successfully", self.volname)

    @classmethod
    def tearDownClass(cls):
        cls.get_super_method(cls, 'tearDownClass')(
            delete_nfs_ganesha_cluster=False)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaSubDirExportsWithIO(NfsGaneshaClusterSetupClass):
    """
    Tests to verify nfs-ganesha sub directory exports.
    """
    @classmethod
    def setUpClass(cls):
        """
        Setup nfs-ganesha if not exists.
        Upload IO scripts to clients
        """
        cls.get_super_method(cls, 'setUpClass')()

        # Setup nfs-ganesha if not exists.
        ret = cls.setup_nfs_ganesha()
        if not ret:
            raise ExecutionError("Failed to setup nfs-ganesha cluster")
        g.log.info("nfs-ganesha cluster is healthy")

        # Upload IO scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def start_and_wait_for_io_to_complete(self, mount_objs):
        """
        This module start IO on clients and wait for io to complete.
        Args:
            mount_objs (lis): List of mounts
        Returns:
            True if IO is success, False otherwise.
        """
        # Start IO on mounts
        g.log.info("Starting IO on all mounts.")
        self.all_mounts_procs = []
        for mount_obj in mount_objs:
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 15 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 10 %s" % (
                       self.script_upload_path,
                       self.dir_start, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.dir_start += 10

        # Adding a delay of 15 seconds giving some time to fill data
        sleep(15)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        if not ret:
            g.log.error("IO failed on some of the clients")
            return False
        return True

    def setUp(self):
        """
        Setup and mount volume
        Write data on mounts and select sub-directory required for the test
        and unmount the existing mounts.
        """
        # Counter for directory start number
        self.dir_start = 1

        g.log.info("Starting to setup and mount volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume %s"
                                 % self.volname)
        g.log.info("Successful in setup and mount volume %s", self.volname)

        ret = self.start_and_wait_for_io_to_complete(self.mounts)
        self.assertTrue(ret, "IO failed on one or more clients")

        mountpoint = self.mounts[0].mountpoint
        client = self.mounts[0].client_system

        # Select the subdirectory required for the test.
        cmd = "find %s -type d -links 2 | grep -ve '.trashcan'" % mountpoint
        ret, out, _ = g.run(client, cmd)
        if ret != 0:
            raise ExecutionError("Failed to list the deep level directories")
        self.subdir_path = out.split("\n")[0]

        _rc = True
        for mount_obj in self.mounts:
            ret = mount_obj.unmount()
            if not ret:
                g.log.error("Unable to unmount volume '%s:%s' on '%s:%s'",
                            mount_obj.server_system, mount_obj.volname,
                            mount_obj.client_system, mount_obj.mountpoint)
                _rc = False
        if not _rc:
            raise ExecutionError("Unmount of all mounts are not successful")

        # Mount objects for sub-directory mount
        self.sub_dir_mounts = deepcopy(self.mounts)

    def test_nfs_ganesha_subdirectory_mount_from_client_side(self):
        """
        Tests script to verify nfs-ganesha subdirectory mount from client side
        succeeds and able to write IOs.
        """
        for mount_obj in self.sub_dir_mounts:
            subdir_to_mount = self.subdir_path.replace(mount_obj.mountpoint,
                                                       '')
            if not subdir_to_mount.startswith(os.path.sep):
                subdir_to_mount = os.path.sep + subdir_to_mount

            mount_obj.volname = mount_obj.volname + subdir_to_mount
            if not mount_obj.is_mounted():
                ret = mount_obj.mount()
                self.assertTrue(ret, ("Unable to mount volume '%s:%s' "
                                      "on '%s:%s'"
                                      % (mount_obj.server_system,
                                         mount_obj.volname,
                                         mount_obj.client_system,
                                         mount_obj.mountpoint)))

        ret = self.start_and_wait_for_io_to_complete(self.sub_dir_mounts)
        self.assertTrue(ret, ("Failed to write IOs when sub directory is"
                              " mounted from client side"))
        g.log.info("IO successful on all clients")

    def test_nfs_ganesha_subdirectory_mount_from_server_side(self):
        """
        Tests script to verify nfs ganesha subdirectory mount from server
        side succeeds and able to write IOs.
        """
        subdir_to_mount = self.subdir_path.replace(self.mounts[0].mountpoint,
                                                   '')
        if not subdir_to_mount.startswith(os.path.sep):
            subdir_to_mount = os.path.sep + subdir_to_mount

        subdir = self.volname + subdir_to_mount

        for mount_obj in self.sub_dir_mounts:
            mount_obj.volname = subdir

        export_file = ("/var/run/gluster/shared_storage/nfs-ganesha/exports/"
                       "export.%s.conf" % self.volname)
        cmd = (r"sed -i  s/'Path = .*'/'Path = \"\/%s\";'/g %s"
               % (re.escape(subdir), export_file))
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, ("Unable to change Path info to %s in %s"
                                  % ("/" + subdir, export_file)))

        cmd = ("sed -i  's/volume=.*/& \\n volpath=\"%s\";/g' %s"
               % (re.escape(subdir_to_mount), export_file))
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, ("Unable to add volpath info to %s in %s"
                                  % ("/" + subdir, export_file)))

        cmd = (r"sed -i  s/'Pseudo=.*'/'Pseudo=\"\/%s\";'/g %s"
               % (re.escape(subdir), export_file))
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, ("Unable to change pseudo Path info to "
                                  "%s in %s" % ("/" + subdir, export_file)))

        # Stop and start volume to take the modified export file to effect.
        # Stopping volume
        ret = volume_stop(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to stop volume %s" % self.volname))

        # Waiting for few seconds for volume unexport. Max wait time is
        # 120 seconds.
        ret = wait_for_nfs_ganesha_volume_to_get_unexported(self.mnode,
                                                            self.volname)
        self.assertTrue(ret, ("Failed to unexport volume %s after "
                              "stopping volume" % self.volname))

        # Starting volume
        ret = volume_start(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to start volume %s" % self.volname))

        # Waiting for few seconds for volume export. Max wait time is
        # 120 seconds.
        ret = wait_for_nfs_ganesha_volume_to_get_exported(self.mnode, subdir)
        self.assertTrue(ret, ("Failed to export sub directory %s after "
                              "starting volume" % subdir))

        for mount_obj in self.sub_dir_mounts:
            if not mount_obj.is_mounted():
                ret = mount_obj.mount()
                self.assertTrue(ret, ("Unable to mount volume '%s:%s' "
                                      "on '%s:%s'"
                                      % (mount_obj.server_system,
                                         mount_obj.volname,
                                         mount_obj.client_system,
                                         mount_obj.mountpoint)))

        ret = self.start_and_wait_for_io_to_complete(self.sub_dir_mounts)
        self.assertTrue(ret, ("Failed to write IOs when sub directory is"
                              " mounted from server side"))
        g.log.info("IO successful on clients")

    def tearDown(self):
        """
        Unexport volume
        Unmount and cleanup volume
        """
        # Unexport volume
        ret, _, _ = unexport_nfs_ganesha_volume(self.mnode, self.volname)
        if ret != 0:
            raise ExecutionError("Failed to unexport volume %s" % self.volname)

        # Unmount volume
        ret = self.unmount_volume(self.sub_dir_mounts)
        if ret:
            g.log.info("Successfully unmounted the volume")
        else:
            g.log.error("Failed to unmount volume")

        # Cleanup volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Cleanup volume %s completed successfully", self.volname)

    @classmethod
    def tearDownClass(cls):
        cls.get_super_method(cls, 'tearDownClass')(
            delete_nfs_ganesha_cluster=False)
