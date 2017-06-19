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
                                                export_nfs_ganesha_volume)
from glustolibs.gluster.volume_ops import volume_stop, volume_start
from glustolibs.gluster.volume_libs import get_volume_options
import time
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts)
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
                       "Count : %s " % str(i))

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
                       "cluster. Count : %s " % str(i))

            ret, _, _ = disable_nfs_ganesha(self.mnode)
            self.assertEqual(ret, 0, ("Failed to disable nfs-ganesha cluster"))

            time.sleep(2)
            vol_option = get_volume_options(self.mnode, self.volname,
                                            option='ganesha.enable')
            if vol_option is None:
                self.assertEqual(ret, 0, ("Failed to get ganesha.enable volume"
                                          " option for %s " % self.volume))

            if vol_option['ganesha.enable'] != 'off':
                self.assertTrue(False, ("Failed to unexport volume by default "
                                        "after disabling cluster"))

            ret, _, _ = enable_nfs_ganesha(self.mnode)
            self.assertEqual(ret, 0, ("Failed to enable nfs-ganesha cluster"))

            time.sleep(2)
            vol_option = get_volume_options(self.mnode, self.volname,
                                            option='ganesha.enable')
            if vol_option is None:
                self.assertEqual(ret, 0, ("Failed to get ganesha.enable volume"
                                          " option for %s " % self.volume))

            if vol_option['ganesha.enable'] != 'off':
                self.assertTrue(False, ("Volume %s is exported by default "
                                        "after disable and enable of cluster"
                                        "which is unexpected."
                                        % self.volname))

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
