#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
      Test mount point ownership persistence post volume restart.
"""

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfile import (
    get_file_stat,
    set_file_permissions)
from glustolibs.gluster.volume_ops import (
    volume_stop,
    volume_start)
from glustolibs.gluster.volume_libs import wait_for_volume_process_to_be_online


@runs_on([['arbiter', 'distributed', 'replicated', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'distributed-arbiter'],
          ['glusterfs']])
class TestMountPointOwnershipPostVolumeRestart(GlusterBaseClass):
    """ Test mount point ownership persistence post volume restart """

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup and mount the volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume and mount it")
        self.client = self.mounts[0].client_system
        self.mountpoint = self.mounts[0].mountpoint

    def validate_mount_permissions(self):
        """
        Verify the mount permissions
        """
        stat_mountpoint_dict = get_file_stat(self.client,
                                             self.mounts[0].mountpoint)
        self.assertEqual(stat_mountpoint_dict['access'], '777', "Expected 777 "
                         " but found %s" % stat_mountpoint_dict['access'])
        g.log.info("Mountpoint permissions is 777, as expected.")

    def test_mountpoint_ownsership_post_volume_restart(self):
        """
        Test mountpoint ownership post volume restart
        1. Create a volume and mount it on client.
        2. set ownsership permissions and validate it.
        3. Restart volume.
        4. Ownership permissions should persist.
        """
        # Set full permissions on the mountpoint.
        ret = set_file_permissions(self.clients[0], self.mountpoint,
                                   "-R 777")
        self.assertTrue(ret, "Failed to set permissions on the mountpoint")
        g.log.info("Set full permissions on the mountpoint.")

        # Validate the permissions set.
        self.validate_mount_permissions()

        # Stop the volume.
        ret = volume_stop(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to stop volume %s" % self.volname))
        g.log.info("Successful in stopping volume.")

        # Start the volume.
        ret = volume_start(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to start volume %s" % self.volname))
        g.log.info("Successful in starting volume.")

        # Wait for all volume processes to be up and running.
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("All volume processes are not up"))
        g.log.info("All volume processes are up and running.")

        # Adding sleep for the mount to be recognized by client.
        sleep(3)

        # validate the mountpoint permissions.
        self.validate_mount_permissions()

    def tearDown(self):
        """tearDown callback"""
        # Unmount volume and cleanup.
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to unmount and cleanup volume")
        g.log.info("Successful in unmount and cleanup of volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
