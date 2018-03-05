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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'], ['nfs']])
class TestNfsMountAndServerQuorumSettings(GlusterBaseClass):
    """
    Test Cases for performing NFS disable, enable and
    performing NFS mount and unmoount on all volumes,
    performing different types quorum settings
    """

    @classmethod
    def setUpClass(cls):
        GlusterBaseClass.setUpClass.im_func(cls)
        g.log.info("Starting %s ", cls.__name__)

        # checking for peer status from every node
        ret = cls.validate_peers_are_connected()
        if not ret:
            raise ExecutionError("Servers are not in peer probed state")

    def setUp(self):
        """
        setUp method for every test
        """
        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed: %s" % self.volname)
        g.log.info("Volme created successfully : %s", self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """
        # stopping the volume and Cleaning up the volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed Cleanup the Volume %s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_nfs_mount_quorum_settings(self):
        """
        Set nfs.disable off
        Mount it with nfs and unmount it
        set nfs.disable enable
        Mount it with nfs
        Set nfs.disable disable
        Enable server quorum
        Set the quorum ratio to numbers and percentage,
        negative- numbers should fail, negative percentage should fail,
        fraction should fail, negative fraction should fail
        """

        # Mounting a NFS volume
        ret = self.mount_volume(self.mounts)
        self.assertTrue(ret, "NFS volume mount failed for %s" % self.volname)
        g.log.info("Volume mounted sucessfully : %s", self.volname)

        # unmounting NFS Volume
        ret = self.unmount_volume(self.mounts)
        self.assertTrue(ret, "Volumes UnMount failed")
        g.log.info("Volumes UnMounted successfully")

        # performing nfs.disable enable
        self.nfs_options = {"nfs.disable": "enable"}
        ret = set_volume_options(self.mnode, self.volname, self.nfs_options)
        self.assertTrue(ret, "gluster volume set %s nfs.disable "
                             "enable failed" % self.volname)
        g.log.info("gluster volume set %s nfs.disable "
                   "enabled successfully", self.volname)

        # Mounting a NFS volume
        ret = self.mount_volume(self.mounts)
        self.assertFalse(ret, "Volume mount should fail for %s, but volume "
                              "mounted successfully after nfs.disable on"
                         % self.volname)
        g.log.info("Volume mount failed : %s", self.volname)

        # performing nfs.disable disable
        self.nfs_options['nfs.disable'] = 'disable'
        ret = set_volume_options(self.mnode, self.volname, self.nfs_options)
        self.assertTrue(ret, "gluster volume set %s nfs.disable "
                             "disable failed" % self.volname)
        g.log.info("gluster volume set %s nfs.disable "
                   "disabled successfully", self.volname)

        # Enabling server quorum
        self.quorum_options = {'cluster.server-quorum-type': 'server'}
        ret = set_volume_options(self.mnode, self.volname, self.quorum_options)
        self.assertTrue(ret, "gluster volume set %s cluster.server-quorum-type"
                             " server Failed" % self.volname)
        g.log.info("gluster volume set %s cluster.server-quorum-type server "
                   "enabled successfully", self.volname)

        # Setting Quorum ratio in percentage
        self.quorum_perecent = {'cluster.server-quorum-ratio': '51%'}
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertTrue(ret, "gluster volume set all cluster.server-quorum-rat"
                             "io percentage Failed :%s" % self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio 51 "
                   "percentage enabled successfully on :%s", self.servers)

        # Setting quorum ration in numbers
        self.quorum_perecent['cluster.server-quorum-ratio'] = "50"
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertTrue(ret, "gluster volume set all cluster.server-quorum-rat"
                             "io 50 Failed on :%s" % self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio 50 enab"
                   "led successfully 0n :%s", self.servers)

        # Setting quorum ration in negative numbers
        self.quorum_perecent['cluster.server-quorum-ratio'] = "-50"
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertFalse(ret, "gluster volume set all cluster.server-quorum-ra"
                              "tio should fail for negative numbers on :%s" %
                         self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio Failed "
                   "for negative number on :%s", self.servers)

        # Setting quorum ration in negative percentage
        self.quorum_perecent['cluster.server-quorum-ratio'] = "-51%"
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertFalse(ret, "gluster volume set all cluster.server-quorum-"
                              "ratio should fail for negative percentage on"
                              ":%s" % self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio Failed "
                   "for negtive percentage on :%s", self.servers)

        # Setting quorum ration in fraction numbers
        self.quorum_perecent['cluster.server-quorum-ratio'] = "1/2"
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertFalse(ret, "glustervolume set all cluster.server-quorum-"
                              "ratio should fail for fraction numbers :%s"
                         % self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio "
                   "Failed for fraction number :%s", self.servers)

        # Setting quorum ration in negative fraction numbers
        self.quorum_perecent['cluster.server-quorum-ratio'] = "-1/2"
        ret = set_volume_options(self.mnode, 'all', self.quorum_perecent)
        self.assertFalse(ret, "glustervolume set all cluster.server-quorum-"
                              "ratio should fail for negative fraction numbers"
                              " :%s" % self.servers)
        g.log.info("gluster volume set all cluster.server-quorum-ratio Failed "
                   "for negative fraction number :%s", self.servers)
