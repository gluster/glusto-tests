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

""" Test Arbiter Specific Cases"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.volume_libs import (
    expand_volume, wait_for_volume_process_to_be_online,
    verify_all_process_of_volume_are_online, shrink_volume, get_subvols)
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class GlusterArbiterVolumeTypeClass(GlusterBaseClass):
    """Class for testing Volume Type Change from replicated to
        Arbitered volume
    """
    def setUp(self):
        """
        Setup Volume
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        g.log.info("Starting to Setup Volume")
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup_Volume")
        g.log.info("Successful in Setup Volume")

        self.subvols = get_subvols(self.mnode, self.volname)['volume_subvols']

    def tearDown(self):
        # Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

        # Clearing bricks
        for subvol in self.subvols:
            for brick in subvol:
                g.log.info('Clearing brick %s', brick)
                node, brick_path = brick.split(':')
                ret, _, err = g.run(node, 'rm -rf %s' % brick_path)
                self.assertFalse(ret, err)
                g.log.info('Clearing brick %s is successful', brick)
        g.log.info('Clearing for all brick is successful')

    def test_replicated_to_arbiter_volume(self):
        """
        Description:-
        Reduce the replica count from replica 3 to arbiter
        """
        # pylint: disable=too-many-statements
        # Remove brick to reduce the replica count from replica 3
        g.log.info("Removing bricks to form replica 2 volume")
        ret = shrink_volume(self.mnode, self.volname, replica_num=0)
        self.assertTrue(ret, "Failed to remove brick on volume %s"
                        % self.volname)
        g.log.info("Successfully removed brick on volume %s", self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Volume %s process not online despite waiting "
                             "for 300 seconds" % self.volname)
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verifying all bricks online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, "Volume %s : All process are not online"
                        % self.volname)
        g.log.info("Volume %s : All process are online", self.volname)

        # Adding the bricks to make arbiter brick
        g.log.info("Adding bricks to convert to Arbiter Volume")
        replica_arbiter = {'replica_count': 1, 'arbiter_count': 1}
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info, add_to_hot_tier=False,
                            **replica_arbiter)
        self.assertTrue(ret, "Failed to expand the volume  %s" % self.volname)
        g.log.info("Changing volume to arbiter volume is successful %s",
                   self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Failed to wait for volume %s processes "
                             "to be online" % self.volname)
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, "Volume %s : All process are not online"
                        % self.volname)
        g.log.info("Volume %s : All process are online", self.volname)
