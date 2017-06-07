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
from glustolibs.gluster.nfs_ganesha_libs import NfsGaneshaVolumeBaseClass
from glustolibs.gluster.lib_utils import install_epel
from glustolibs.io.utils import run_bonnie, run_fio, run_mixed_io


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaWithDifferentIOPatterns(NfsGaneshaVolumeBaseClass):
    """
        Tests Nfs Ganesha stability by running different IO Patterns
    """

    @classmethod
    def setUpClass(cls):
        NfsGaneshaVolumeBaseClass.setUpClass.im_func(cls)
        if not install_epel(cls.clients):
            raise ExecutionError("Failed to install epel")

    def test_run_bonnie_from_multiple_clients(self):

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

        directory_to_run = []
        for mount in self.mounts:
            directory_to_run.append(mount.mountpoint)

        # Running mixed IOs from multiple clients
        # TODO: parametrizing io_tools and get the inputs from user.
        io_tools = ['bonnie', 'fio']
        ret = run_mixed_io(self.clients, io_tools, directory_to_run)
        self.assertTrue(ret, ("fio test failed while running tests on %s"
                              % self.clients))

        # pcs status output
        _, _, _ = g.run(self.servers[0], "pcs status")

    @classmethod
    def tearDownClass(cls):
        (NfsGaneshaVolumeBaseClass.
         tearDownClass.
         im_func(cls,
                 teardown_nfs_ganesha_cluster=False))
