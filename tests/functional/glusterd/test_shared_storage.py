#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

"""
    Description:
    Test cases in this module related glusterd enabling and
    disabling shared storage
"""

from random import choice
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.shared_storage_ops import (enable_shared_storage,
                                                   is_shared_volume_mounted,
                                                   disable_shared_storage,
                                                   check_gluster_shared_volume)
from glustolibs.gluster.volume_ops import (volume_create,
                                           volume_delete, get_volume_list)
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.misc.misc_libs import reboot_nodes_and_wait_to_come_online


@runs_on([['distributed'], ['glusterfs', 'nfs']])
class SharedStorage(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        # Creating Volume
        if not self.setup_volume():
            raise ExecutionError("Volume creation failed")

    def tearDown(self):
        # Stopping and cleaning up the volume
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get volume list")

        for volume in vol_list:
            if not cleanup_volume(self.mnode, volume):
                raise ExecutionError("Failed Cleanup the Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _enable_and_check_shared_storage(self):
        """Enable and check shared storage is present"""

        ret = enable_shared_storage(self.mnode)
        self.assertTrue(ret, ("Failed to enable a shared storage"))
        g.log.info("Successfully enabled: enable-shared-storage option")

        # Check volume list to confirm gluster_shared_storage is created
        ret = check_gluster_shared_volume(self.mnode)
        self.assertTrue(ret, ("gluster_shared_storage volume not"
                              " created even after enabling it"))
        g.log.info("gluster_shared_storage volume created"
                   " successfully")

    def _disable_and_check_shared_storage(self):
        """Disable a shared storage without specifying the domain and check"""

        ret = disable_shared_storage(self.mnode)
        self.assertTrue(ret, ("Failed to disable a shared storage"))
        g.log.info("Successfully disabled: disable-shared-storage")

        # Check volume list to confirm gluster_shared_storage is deleted
        ret = check_gluster_shared_volume(self.mnode, present=False)
        self.assertTrue(ret, ("gluster_shared_storage volume not"
                              " deleted even after disabling it"))
        g.log.info("gluster_shared_storage volume deleted"
                   " successfully")

    def _is_shared_storage_mounted_on_the_nodes(self, brick_details, mounted):
        """
        Checks if the shared storage is mounted on the nodes where it is
        created.
        """
        for node in brick_details:
            ret = is_shared_volume_mounted(node.split(":")[0])
            if mounted:
                self.assertTrue(ret, ("Shared volume not mounted even after"
                                      " enabling it"))
                g.log.info("Shared volume mounted successfully")
            else:
                self.assertFalse(ret, ("Shared volume not unmounted even"
                                       " after disabling it"))
                g.log.info("Shared volume unmounted successfully")

    def _get_all_bricks(self):
        """Get all bricks where the shared storage is mounted"""

        brick_list = get_all_bricks(self.mnode, "gluster_shared_storage")
        self.assertIsNotNone(brick_list, "Unable to fetch brick list of shared"
                                         " storage")
        return brick_list

    def _shared_storage_test_without_node_reboot(self):
        """Shared storge testcase till the node reboot scenario"""

        # Enable shared storage and check it is present on the cluster
        self._enable_and_check_shared_storage()

        # Get all the bricks where shared storage is mounted
        brick_list = self._get_all_bricks()

        # Check the shared volume is mounted on the nodes where it is created
        self._is_shared_storage_mounted_on_the_nodes(brick_details=brick_list,
                                                     mounted=True)
        # Disable shared storage and check it is not present on the cluster
        self._disable_and_check_shared_storage()

        # Check the shared volume is unmounted on the nodes where it is created
        self._is_shared_storage_mounted_on_the_nodes(brick_details=brick_list,
                                                     mounted=False)

        # Create a volume with name gluster_shared_storage
        volume = "gluster_shared_storage"
        bricks_list = form_bricks_list(self.mnode, volume, 2, self.servers,
                                       self.all_servers_info)
        count = 0
        while count < 20:
            ret, _, _ = volume_create(self.mnode, volume, bricks_list, True)
            if not ret:
                break
            sleep(2)
            count += 1
        self.assertEqual(ret, 0, "Failed to create volume")
        g.log.info("Volume create is success")

        # Disable the shared storage should fail
        ret = disable_shared_storage(self.mnode)
        self.assertFalse(ret, ("Unexpected: Successfully disabled"
                               " shared-storage"))
        g.log.info("Volume set: failed as expected")

        # Check volume list to confirm gluster_shared_storage
        # is not deleted which was created before
        vol_list = get_volume_list(self.mnode)
        _rc = False
        for vol in vol_list:
            if vol == "gluster_shared_storage":
                _rc = True
                break
        self.assertTrue(_rc, ("gluster_shared_storage volume got"
                              " deleted after disabling it"))
        g.log.info("gluster_shared_storage volume not deleted as "
                   " expected after disabling enable-shared-storage")

        # Delete the volume created
        ret = volume_delete(self.mnode, volume)
        self.assertTrue(ret, ("Failed to cleanup the Volume "
                              "%s", volume))
        g.log.info("Volume deleted successfully : %s", volume)

        # Enable shared storage and check it is present on the cluster
        self._enable_and_check_shared_storage()

        # Check the shared volume is mounted on the nodes where it is created
        self._is_shared_storage_mounted_on_the_nodes(brick_details=brick_list,
                                                     mounted=True)

        # Disable shared storage and check it is not present on the cluster
        self._disable_and_check_shared_storage()

        # Check the shared volume is unmounted on the nodes where it is created
        self._is_shared_storage_mounted_on_the_nodes(brick_details=brick_list,
                                                     mounted=False)

    def test_shared_storage(self):
        """
        This test case includes:
        -> Enable a shared storage
        -> Disable a shared storage
        -> Create volume of any type with
           name gluster_shared_storage
        -> Disable the shared storage
        -> Check, volume created in step-3 is
           not deleted
        -> Delete the volume
        -> Enable the shared storage
        -> Check volume with name gluster_shared_storage
           is created
        -> Disable the shared storage
        -> Enable shared storage and validate whether it is mounted
        -> Perform node reboot
        -> Post reboot validate the bricks are mounted back or not
        """
        # pylint: disable=too-many-statements, too-many-branches
        self._shared_storage_test_without_node_reboot()

        # Enable shared storage and check it is present on the cluster
        self._enable_and_check_shared_storage()

        # Get all the bricks where shared storage is mounted
        brick_list = self._get_all_bricks()

        # Check the shared volume is mounted on the nodes where it is created
        self._is_shared_storage_mounted_on_the_nodes(brick_details=brick_list,
                                                     mounted=True)

        # Perform node reboot on any of the nodes where the shared storage is
        # mounted
        node_to_reboot = choice(brick_list)
        node_to_reboot = node_to_reboot.split(":")[0]
        ret = reboot_nodes_and_wait_to_come_online(node_to_reboot)
        self.assertTrue(ret, "Reboot Failed on node: "
                             "{}".format(node_to_reboot))
        g.log.info("Node: %s rebooted successfully", node_to_reboot)

        # Post reboot checking peers are connected
        count = 0
        while count < 10:
            ret = self.validate_peers_are_connected()
            if ret:
                break
            sleep(3)
            count += 1
        self.assertTrue(ret, "Peers are not in connected state.")

        # Check the shared volume is mounted on the nodes where it is created
        self._is_shared_storage_mounted_on_the_nodes(brick_details=brick_list,
                                                     mounted=True)

        # Disable shared storage and check it is not present on the cluster
        self._disable_and_check_shared_storage()

        # Check the shared volume is unmounted on the nodes where it is created
        self._is_shared_storage_mounted_on_the_nodes(brick_details=brick_list,
                                                     mounted=False)
