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
        Test Cases in this module test NFS-Ganesha Sanity.
"""
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import runs_on
from glustolibs.gluster.nfs_ganesha_libs import (
    NfsGaneshaVolumeBaseClass,
    NfsGaneshaIOBaseClass)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['nfs']])
class TestNfsGaneshaSanity(NfsGaneshaVolumeBaseClass):
    """
        Tests to verify NFS Ganesha Sanity.
    """

    @classmethod
    def setUpClass(cls):
        NfsGaneshaVolumeBaseClass.setUpClass.im_func(cls)

    def test_nfs_ganesha_HA_Basic_IO(self):
        """
        Tests to create an HA cluster and run basic IO
        """

        # Starting IO on the mounts.Let's do iozone first.
        for mount_obj in self.mounts:
            # Make sure you run relevant setup playbooks,view README !
            g.log.info("Running iozone on %s", mount_obj.client_system)
            cmd = ("cd %s ;iozone -a" % (mount_obj.mountpoint))
            ret, out, err = g.run(mount_obj.client_system, cmd)
            if ret == 0:
                g.log.info(" Iozone run successful")
            else:
                g.log.error("ERROR! Drastic Iozone error encountered !")
                self.assertEqual(ret, 0, "Iozone run failed!")

        # Check for crashes after iozone run
        g.log.info("Checking for Cluster Status after iozone run")
        ret, out, err = g.run(self.servers[0],
                              "/usr/libexec/ganesha/ganesha-ha.sh --status"
                              " /var/run/gluster/shared_storage/nfs-ganesha")

        if "HEALTHY" in out:
            g.log.info("Cluster is HEALTHY,Continuing..")

        else:
            g.log.error("ERROR! Cluster unhealthy,check for cores!")
            self.assertEqual(ret, 0, "Iozone run failed! Cluster Unhealthy")

        # Running kernel untar now,single loop for the sanity test
        g.log.info("Running kernel untars now")
        for mount_obj in self.mounts:
                cmd = ("cd %s ;mkdir $(hostname);cd $(hostname);"
                       "wget https://www.kernel.org/pub/linux/kernel/v2.6"
                       "/linux-2.6.1.tar.gz;"
                       "tar xvf linux-2.6.1.tar.gz" % (mount_obj.mountpoint))
                ret, out, err = g.run(mount_obj.client_system, cmd)
                if ret == 0:
                    g.log.info("Succesfully untared the tarball!")
                else:
                    g.log.error("ERROR ! Kernel untar errored out!")
                    self.assertEqual(ret, 0, "Kernel untar failed!")

        # Check for crashes after kernel untar
        g.log.info("Checking for Cluster Status after kernel untar")
        ret, out, err = g.run(self.servers[0],
                              "/usr/libexec/ganesha/ganesha-ha.sh --status"
                              " /var/run/gluster/shared_storage/nfs-ganesha")

        if "HEALTHY" in out:
            g.log.info("Cluster is HEALTHY,Continuing..")

        else:
            g.log.error("ERROR! Cluster unhealthy after I/O,check for cores!")
            self.assertEqual(ret, 0, "Cluster unhealthy after Kernel untar")

    @classmethod
    def tearDownClass(cls):
        (NfsGaneshaIOBaseClass.
         tearDownClass.
         im_func(cls,
                 teardown_nfsganesha_cluster=False))
