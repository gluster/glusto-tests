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
        Test Cases in this module tests the nfs ganesha feature
        while running different IO patterns.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.nfs_ganesha_libs import NfsGaneshaClusterSetupClass
from glustolibs.gluster.lib_utils import install_epel
from glustolibs.io.utils import run_bonnie, run_fio, run_mixed_io


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaWithDifferentIOPatterns(NfsGaneshaClusterSetupClass):
    """
        Tests Nfs Ganesha stability by running different IO Patterns
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

        # Install epel
        if not install_epel(cls.clients):
            raise ExecutionError("Failed to install epel")

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

    def test_run_bonnie_from_multiple_clients(self):
        """
        Run bonnie test
        """
        directory_to_run = []
        for mount in self.mounts:
            directory_to_run.append(mount.mountpoint)

        # Running Bonnie tests from multiple clients
        ret = run_bonnie(self.clients, directory_to_run)
        self.assertTrue(ret, ("Bonnie test failed while running tests on %s"
                              % self.clients))

        # pcs status output
        _, _, _ = g.run(self.servers[0], "pcs status")

    def test_run_fio_from_multiple_clients(self):
        """
        Run fio
        """
        directory_to_run = []
        for mount in self.mounts:
            directory_to_run.append(mount.mountpoint)

        # Running fio tests from multiple clients
        ret = run_fio(self.clients, directory_to_run)
        self.assertTrue(ret, ("fio test failed while running tests on %s"
                              % self.clients))

        # pcs status output
        _, _, _ = g.run(self.servers[0], "pcs status")

    def test_run_mixed_io_from_multiple_clients(self):
        """
        Run multiple IOs
        """
        directory_to_run = []
        for mount in self.mounts:
            directory_to_run.append(mount.mountpoint)

        # Running mixed IOs from multiple clients
        # TODO: parametrizing io_tools and get the inputs from user.
        io_tools = ['bonnie', 'fio']
        ret = run_mixed_io(self.clients, io_tools, directory_to_run)
        self.assertTrue(ret, "IO failed on one or more clients.")

        # pcs status output
        _, _, _ = g.run(self.servers[0], "pcs status")

    def tearDown(self):
        """
        Unmount and cleanup volume
        """
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
