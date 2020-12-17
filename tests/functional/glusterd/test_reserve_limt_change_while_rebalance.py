#  Copyright (C) 2020  Red Hat, Inc. <http://www.redhat.com>
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

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.rebalance_ops import (
    rebalance_start,
    rebalance_stop,
    wait_for_rebalance_to_complete
)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.io.utils import run_linux_untar


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestReserveLimitChangeWhileRebalance(GlusterBaseClass):

    def _set_vol_option(self, option):
        """Method for setting volume option"""
        ret = set_volume_options(
            self.mnode, self.volname, option)
        self.assertTrue(ret)

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Set I/O flag to false
        cls.is_io_running = False

        # Setup Volume and Mount Volume
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    def tearDown(self):
        if not wait_for_rebalance_to_complete(
                self.mnode, self.volname, timeout=300):
            raise ExecutionError(
                "Failed to complete rebalance on volume '{}'".format(
                    self.volname))

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume % s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_reserve_limt_change_while_rebalance(self):
        """
        1) Create a distributed-replicated volume and start it.
        2) Enable storage.reserve option on the volume using below command,
           gluster volume set storage.reserve 50
        3) Mount the volume on a client
        4) Add some data on the mount point (should be within reserve limits)
        5) Now, add-brick and trigger rebalance.
           While rebalance is in-progress change the reserve limit to a lower
           value say (30)
        6. Stop the rebalance
        7. Reset the storage reserve value to 50 as in step 2
        8. trigger rebalance
        9. while rebalance in-progress change the reserve limit to a higher
         value say (70)
        """

        # Setting storage.reserve 50
        self._set_vol_option({"storage.reserve": "50"})

        self.list_of_io_processes = []
        # Create a dir to start untar
        self.linux_untar_dir = "{}/{}".format(self.mounts[0].mountpoint,
                                              "linuxuntar")
        ret = mkdir(self.clients[0], self.linux_untar_dir)
        self.assertTrue(ret, "Failed to create dir linuxuntar for untar")

        # Start linux untar on dir linuxuntar
        ret = run_linux_untar(self.clients[0], self.mounts[0].mountpoint,
                              dirs=tuple(['linuxuntar']))
        self.list_of_io_processes += ret
        self.is_io_running = True

        # Add bricks to the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick with rsync on volume %s"
                        % self.volname)

        # Trigger rebalance on the volume
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Setting storage.reserve 30
        self._set_vol_option({"storage.reserve": "30"})

        # Stopping Rebalance
        ret, _, _ = rebalance_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to stop rebalance on the volume %s"
                         % self.volname)

        # Setting storage.reserve 500
        self._set_vol_option({"storage.reserve": "500"})

        # Trigger rebalance on the volume
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to start rebalance on the volume %s"
                         % self.volname)

        # Setting storage.reserve 70
        self._set_vol_option({"storage.reserve": "70"})
