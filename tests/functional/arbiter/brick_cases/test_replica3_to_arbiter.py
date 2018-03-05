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

""" Test Arbiter Specific Cases"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status,
    expand_volume, wait_for_volume_process_to_be_online,
    verify_all_process_of_volume_are_online, shrink_volume)
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, rebalance_status, wait_for_rebalance_to_complete)
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class GlusterArbiterVolumeTypeClass(GlusterBaseClass):
    """Class for testing Volume Type Change from replicated to
        Arbitered volume
    """
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Overriding the volume type to specifically test the volume type

        if cls.volume_type == "replicated":
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'dist_count': 3,
                'transport': 'tcp'}

    def setUp(self):
        """
        - Setup Volume and Mount Volume
        """
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_replicated_to_arbiter_volume(self):
        """
        Description:-
        Reduce the replica count from replica 3 to arbiter
        """
        # pylint: disable=too-many-statements
        # Log Volume Info and Status
        g.log.info("Logging volume info and Status")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Remove brick to reduce the replica count from replica 3
        g.log.info("Removing bricks to form replica 2 volume")
        subvol_num = 1
        replica_num = 1
        reduce_count = {'replica_count': 2, 'distribute_count': 1}
        ret = shrink_volume(self.mnode, self.volname, subvol_num,
                            replica_num, force=True, **reduce_count)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Log Volume Info and Status after changin
        # replicate 3 to replica 2
        g.log.info("Logging volume info and Status after changing to "
                   "arbitered volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s process not online despite waiting "
                              "for 300 seconds", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verifying all bricks online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Adding the bricks to amke arbiter brick
        g.log.info("Adding bricks to convert to Arbiter Volume")
        replica_arbiter = {'replica_count': 3, 'arbiter_count': 1}
        ret = expand_volume(self.mnode, self.volname, self.servers[2:],
                            self.all_servers_info, add_to_hot_tier=False,
                            **replica_arbiter)
        self.assertTrue(ret, ("Failed to expand the volume  %s", self.volname))
        g.log.info("Changing volume to arbiter volume is successfull %s",
                   self.volname)

        # Log Volume Info and Status after expanding the volume
        g.log.info("Logging volume info and Status after expanding volume")
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

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

        # Log Rebalance status
        g.log.info("Log Rebalance status")
        _, _, _ = rebalance_status(self.mnode, self.volname)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalance is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)
