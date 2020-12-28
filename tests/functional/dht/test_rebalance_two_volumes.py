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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.io.utils import collect_mounts_arequal
from glustolibs.gluster.mount_ops import mount_volume
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           volume_stop, volume_delete)
from glustolibs.gluster.lib_utils import form_bricks_list


@runs_on([['distributed', 'distributed-replicated'], ['glusterfs']])
class TestRebalanceTwoVolumes(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Setup Volume
        if not self.setup_volume_and_mount_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Setup and mount volume")

        self.first_client = self.mounts[0].client_system

        self.second_vol_name = "second_volume"
        self.second_mountpoint = "/mnt/{}".format(self.second_vol_name)
        self.is_second_volume_created = False

    def tearDown(self):

        # Unmount and clean volume
        if not self.unmount_volume_and_cleanup_volume([self.mounts[0]]):
            raise ExecutionError("Failed to Cleanup Volume")

        if self.is_second_volume_created:
            # Stop the 2nd volume
            ret, _, _ = volume_stop(self.mnode, self.second_vol_name)
            self.assertEqual(ret, 0, ("volume stop failed for %s"
                                      % self.second_vol_name))
            g.log.info("Volume %s stopped", self.second_vol_name)

            # Delete the 2nd volume
            ret = volume_delete(self.mnode, self.second_vol_name)
            self.assertTrue(ret, ("Failed to cleanup the Volume "
                                  "%s", self.second_vol_name))
            g.log.info("Volume deleted successfully : %s",
                       self.second_vol_name)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_rebalance_two_volumes(self):
        """
        Test case:
        1. Create a volume, start it and mount it
        2. Create a 2nd volume, start it and mount it
        3. Create files on mount points
        4. Collect arequal checksum on mount point pre-rebalance
        5. Expand the volumes
        6. Start rebalance simultaneously on the 2 volumes
        7. Wait for rebalance to complete
        8. Collect arequal checksum on mount point post-rebalance
           and compare with value from step 4
        """

        # Get brick list
        bricks_list = form_bricks_list(self.mnode, self.volname, 3,
                                       self.servers, self.all_servers_info)
        self.assertIsNotNone(bricks_list, "Bricks list is None")

        # Create 2nd volume
        ret, _, _ = volume_create(self.mnode, self.second_vol_name,
                                  bricks_list)
        self.assertEqual(ret, 0, ("Failed to create volume %s") % (
            self.second_vol_name))
        g.log.info("Volume %s created successfully", self.second_vol_name)

        # Start 2nd volume
        ret, _, _ = volume_start(self.mnode, self.second_vol_name)
        self.assertEqual(ret, 0, ("Failed to start volume %s") % (
            self.second_vol_name))
        g.log.info("Started volume %s", self.second_vol_name)

        self.is_second_volume_created = True

        # Mount 2nd volume
        for mount_obj in self.mounts:
            ret, _, _ = mount_volume(self.second_vol_name,
                                     mtype=self.mount_type,
                                     mpoint=self.second_mountpoint,
                                     mserver=self.mnode,
                                     mclient=mount_obj.client_system)
            self.assertEqual(ret, 0, ("Failed to mount volume %s") % (
                self.second_vol_name))
            g.log.info("Volume mounted successfully : %s",
                       self.second_vol_name)

        # Start I/O from mount point for volume 1 and wait for it to complete
        cmd = ("cd %s; for i in {1..1000} ; do "
               "dd if=/dev/urandom of=file$i bs=10M count=1; done"
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertEqual(ret, 0, "IO failed on volume %s"
                         % self.volname)

        # Start I/O from mount point for volume 2 and wait for it to complete
        cmd = ("cd %s; for i in {1..1000} ; do "
               "dd if=/dev/urandom of=file$i bs=10M count=1; done"
               % self.second_mountpoint)
        ret, _, _ = g.run(self.first_client, cmd)
        self.assertEqual(ret, 0, "IO failed on volume %s"
                         % self.second_vol_name)

        # Collect arequal checksum before rebalance
        arequal_checksum_before = collect_mounts_arequal(self.mounts[0])

        # Add bricks to volumes
        for volume in (self.volname, self.second_vol_name):
            ret = expand_volume(self.mnode, volume, self.servers,
                                self.all_servers_info)
            self.assertTrue(ret, "Failed to add brick on volume %s"
                            % volume)

        # Trigger rebalance
        for volume in (self.volname, self.second_vol_name):
            ret, _, _ = rebalance_start(self.mnode, volume,
                                        force=True)
            self.assertEqual(ret, 0, "Failed to start rebalance on the"
                             " volume %s" % volume)

        # Wait for rebalance to complete
        for volume in (self.volname, self.second_vol_name):
            ret = wait_for_rebalance_to_complete(self.mnode, volume,
                                                 timeout=1200)
            self.assertTrue(ret, "Rebalance is not yet complete on the volume"
                            " %s" % volume)
            g.log.info("Rebalance successfully completed")

        # Collect arequal checksum after rebalance
        arequal_checksum_after = collect_mounts_arequal(self.mounts[0])

        # Check for data loss by comparing arequal before and after rebalance
        self.assertEqual(arequal_checksum_before, arequal_checksum_after,
                         "arequal checksum is NOT MATCHNG")
        g.log.info("arequal checksum is SAME")
