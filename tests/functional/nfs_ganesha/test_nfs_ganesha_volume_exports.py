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
        Test Cases in this module tests the nfs ganesha exports,
        refresh configs, cluster enable/disable functionality.
"""

import time
import os
import re
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.nfs_ganesha_libs import (
    NfsGaneshaVolumeBaseClass,
    wait_for_nfs_ganesha_volume_to_get_exported,
    wait_for_nfs_ganesha_volume_to_get_unexported,
    NfsGaneshaIOBaseClass)
from glustolibs.gluster.nfs_ganesha_ops import (enable_acl, disable_acl,
                                                run_refresh_config,
                                                enable_nfs_ganesha,
                                                disable_nfs_ganesha,
                                                export_nfs_ganesha_volume,
                                                unexport_nfs_ganesha_volume)
from glustolibs.gluster.volume_ops import (volume_stop, volume_start,
                                           get_volume_info)
from glustolibs.gluster.volume_libs import (get_volume_options, setup_volume,
                                            cleanup_volume, is_volume_exported,
                                            log_volume_info_and_status)
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaVolumeExports(NfsGaneshaVolumeBaseClass):
    """
        Tests to verify Nfs Ganesha exports, cluster enable/disable
        functionality.
    """

    @classmethod
    def setUpClass(cls):
        NfsGaneshaVolumeBaseClass.setUpClass.im_func(cls)

    def test_nfs_ganesha_export_after_vol_restart(self):
        """
        Tests script to check nfs-ganesha volume gets exported after
        multiple volume restarts.
        """

        for i in range(5):
            g.log.info("Testing nfs ganesha export after volume stop/start."
                       "Count : %s", str(i))

            # Stoping volume
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

        for i in range(5):
            g.log.info("Executing multiple enable/disable of nfs ganesha "
                       "cluster. Count : %s ", str(i))

            ret, _, _ = disable_nfs_ganesha(self.mnode)
            self.assertEqual(ret, 0, ("Failed to disable nfs-ganesha cluster"))

            time.sleep(2)
            vol_option = get_volume_options(self.mnode, self.volname,
                                            option='ganesha.enable')
            if vol_option is None:
                self.assertEqual(ret, 0, ("Failed to get ganesha.enable volume"
                                          " option for %s " % self.volume))

            self.assertEqual(vol_option.get('ganesha.enable'), 'off', "Failed "
                             "to unexport volume by default after disabling "
                             "cluster")

            ret, _, _ = enable_nfs_ganesha(self.mnode)
            self.assertEqual(ret, 0, ("Failed to enable nfs-ganesha cluster"))

            time.sleep(2)
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
        ret, _, _ = export_nfs_ganesha_volume(
            mnode=self.mnode, volname=self.volname)
        self.assertEqual(ret, 0, ("Failed to export volume %s "
                                  "after disable and enable of cluster"
                                  % self.volname))
        time.sleep(5)

        # List the volume exports
        _, _, _ = g.run(self.mnode, "showmount -e")

    @classmethod
    def tearDownClass(cls):

        (NfsGaneshaVolumeBaseClass.
         tearDownClass.
         im_func(cls,
                 teardown_nfs_ganesha_cluster=False))


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaVolumeExportsWithIO(NfsGaneshaIOBaseClass):
    """
    Tests to verfiy nfs ganesha features when IO is in progress.
    """

    def test_nfs_ganesha_multiple_refresh_configs(self):
        """
        Tests script to check nfs-ganehsa volume gets exported and IOs
        are running after running multiple refresh configs.
        """

        self.acl_check_flag = False

        for i in range(6):
            # Enabling/Disabling ACLs to modify the export configuration
            # before running refresh config
            if i % 2 == 0:
                ret = disable_acl(self.mnode, self. volname)
                self.assertTrue(ret, ("Failed to disable acl on %s"
                                      % self.volname))
                self.acl_check_flag = False
            else:
                ret = enable_acl(self.mnode, self. volname)
                self.assertTrue(ret, ("Failed to enable acl on %s"
                                      % self.volname))
                self.acl_check_flag = True

            ret = run_refresh_config(self.mnode, self. volname)
            self.assertTrue(ret, ("Failed to run refresh config"
                                  "for volume %s" % self.volname))

            time.sleep(2)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

    def tearDown(self):
        if self.acl_check_flag:
            ret = disable_acl(self.mnode, self. volname)
            if not ret:
                raise ExecutionError("Failed to disable acl on %s"
                                     % self.volname)
            self.acl_check_flag = False

        NfsGaneshaIOBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):

        (NfsGaneshaIOBaseClass.
         tearDownClass.
         im_func(cls,
                 teardown_nfsganesha_cluster=False))


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaMultiVolumeExportsWithIO(NfsGaneshaIOBaseClass):
    """
    Tests to verfiy multiple volumes gets exported when IO is in progress.
    """

    def test_nfs_ganesha_export_with_multiple_volumes(self):
        """
        Testcase to verfiy multiple volumes gets exported when IO is in
        progress.
        """

        for i in range(5):
            self.volume['name'] = "nfsvol" + str(i)
            self.volume['voltype']['type'] = 'distributed'
            self.volume['voltype']['replica_count'] = 1
            self.volume['voltype']['dist_count'] = 2

            # Create volume
            ret = setup_volume(mnode=self.mnode,
                               all_servers_info=self.all_servers_info,
                               volume_config=self.volume, force=True)
            if not ret:
                self.assertTrue(ret, ("Setup volume %s failed" % self.volume))
            time.sleep(5)

            # Export volume with nfs ganesha, if it is not exported already
            vol_option = get_volume_options(self.mnode, self.volume['name'],
                                            option='ganesha.enable')
            self.assertIsNotNone(vol_option, "Failed to get ganesha.enable "
                                 "volume option for %s" % self.volume['name'])
            if vol_option['ganesha.enable'] != 'on':
                ret, _, _ = export_nfs_ganesha_volume(
                    mnode=self.mnode, volname=self.volume['name'])
                self.assertEqual(ret, 0, "Failed to export volume %s as NFS "
                                 "export" % self.volume['name'])
                time.sleep(5)
            else:
                g.log.info("Volume %s is exported already",
                           self.volume['name'])

            # Waiting for few seconds for volume export. Max wait time is
            # 120 seconds.
            ret = wait_for_nfs_ganesha_volume_to_get_exported(self.mnode,
                                                              (self.
                                                               volume['name']))
            self.assertTrue(ret, ("Failed to export volume %s after "
                                  "starting volume when IO is running on "
                                  "another volume" % self.volume['name']))

            # Log Volume Info and Status
            ret = log_volume_info_and_status(self.mnode, self.volume['name'])
            self.assertTrue(ret, "Logging volume %s info and status failed"
                            % self.volume['name'])

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        self.assertTrue(ret, "IO failed on some of the clients")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

    def tearDown(self):

        # Clean up the volumes created specific for this tests.
        for i in range(5):
            volname = "nfsvol" + str(i)
            volinfo = get_volume_info(self.mnode, volname)
            if volinfo is None or volname not in volinfo:
                g.log.info("Volume %s does not exist in %s",
                           volname, self.mnode)
                continue

            # Unexport volume, if it is not unexported already
            vol_option = get_volume_options(self.mnode, volname,
                                            option='ganesha.enable')
            if vol_option is None:
                raise ExecutionError("Failed to get ganesha.enable volume "
                                     " option for %s " % volname)
            if vol_option['ganesha.enable'] != 'off':
                if is_volume_exported(self.mnode, volname, "nfs"):
                    ret, _, _ = unexport_nfs_ganesha_volume(
                        mnode=self.mnode, volname=volname)
                    if ret != 0:
                        raise ExecutionError("Failed to unexport volume %s "
                                             % volname)
                    time.sleep(5)
            else:
                g.log.info("Volume %s is unexported already", volname)

            _, _, _ = g.run(self.mnode, "showmount -e")

            ret = cleanup_volume(mnode=self.mnode, volname=volname)
            if not ret:
                raise ExecutionError("cleanup volume %s failed" % volname)

        NfsGaneshaIOBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):

        (NfsGaneshaIOBaseClass.
         tearDownClass.
         im_func(cls,
                 teardown_nfsganesha_cluster=False))


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaSubDirExportsWithIO(NfsGaneshaIOBaseClass):
    """
    Tests to verfiy nfs ganesha sub directory exports.
    """

    def start_and_wait_for_io_to_complete(self):
        """This module starts IO from clients and waits for io to complate.
        Returns True, if io gets completed successfully. Otherwise, False
        """

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 15 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 10 %s" % (self.script_upload_path,
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

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        if not ret:
            g.log.error("IO failed on some of the clients")
            return False

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        if not ret:
            g.log.error("Failed to list all files and dirs")
            return False
        g.log.info("Listing all files and directories is successful")
        return True

    def setUp(self):
        """setUp writes data from all mounts and selects subdirectory
           required for the test and unmount the existing mounts.
        """

        NfsGaneshaIOBaseClass.setUp.im_func(self)

        # Validate IO
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.io_validation_complete = True
        if not ret:
            raise ExecutionError("IO failed on some of the clients")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        if not ret:
            raise ExecutionError("Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

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
            raise ExecutionError("Unmount of all mounts are not "
                                 "successful")

    def test_nfs_ganesha_subdirectory_mount_from_client_side(self):
        """
        Tests script to verify nfs ganesha subdirectory mount from client side
        succeeds and able to write IOs.
        """

        for mount_obj in self.mounts:
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

        ret = self.start_and_wait_for_io_to_complete()
        self.assertTrue(ret, ("Failed to write IOs when sub directory is"
                              " mounted from client side"))

    def test_nfs_ganesha_subdirectory_mount_from_server_side(self):
        """
        Tests script to verify nfs ganesha subdirectory mount from server
        side succeeds and able to write IOs.
        """
        subdir_to_mount = self.subdir_path.replace(self.mounts[0].mountpoint,
                                                   '')
        if not subdir_to_mount.startswith(os.path.sep):
            subdir_to_mount = os.path.sep + subdir_to_mount

        for mount_obj in self.mounts:
            mount_obj.volname = mount_obj.volname + subdir_to_mount

        export_file = ("/var/run/gluster/shared_storage/nfs-ganesha/exports/"
                       "export.%s.conf" % self.volname)
        cmd = ("sed -i  s/'Path = .*'/'Path = \"\/%s\";'/g %s"
               % (re.escape(self.mounts[0].volname), export_file))
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, ("Unable to change Path info to %s in %s"
                                  % ("/" + self.mounts[0].volname,
                                     export_file)))

        cmd = ("sed -i  's/volume=.*/& \\n volpath=\"%s\";/g' %s"
               % (re.escape(subdir_to_mount), export_file))
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, ("Unable to add volpath info to %s in %s"
                                  % ("/" + self.mounts[0].volname,
                                     export_file)))

        cmd = ("sed -i  s/'Pseudo=.*'/'Pseudo=\"\/%s\";'/g %s"
               % (re.escape(self.mounts[0].volname), export_file))
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, ("Unable to change pseudo Path info to "
                                  "%s in %s" % ("/" + self.mounts[0].volname,
                                                export_file)))

        # Stop and start volume to take the modified export file to effect.
        # Stoping volume
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
                                                          (self.mounts[0].
                                                           volname))
        self.assertTrue(ret, ("Failed to export sub directory %s after "
                              "starting volume" % self.mounts[0].volname))

        for mount_obj in self.mounts:
            if not mount_obj.is_mounted():
                ret = mount_obj.mount()
                self.assertTrue(ret, ("Unable to mount volume '%s:%s' "
                                      "on '%s:%s'"
                                      % (mount_obj.server_system,
                                         mount_obj.volname,
                                         mount_obj.client_system,
                                         mount_obj.mountpoint)))

        ret = self.start_and_wait_for_io_to_complete()
        self.assertTrue(ret, ("Failed to write IOs when sub directory is"
                              " mounted from server side"))

    def tearDown(self):
        """setUp starts the io from all the mounts.
            IO creates deep dirs and files.
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

        NfsGaneshaIOBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):

        (NfsGaneshaIOBaseClass.
         tearDownClass.
         im_func(cls,
                 teardown_nfsganesha_cluster=False))
