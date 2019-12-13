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

"""
    Description:
    Test cases in this module related glusterd enabling and
    disabling shared storage
"""

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import (volume_create,
                                           volume_delete, get_volume_list)
from glustolibs.gluster.volume_libs import cleanup_volume
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.shared_storage_ops import (enable_shared_storage,
                                                   is_shared_volume_mounted,
                                                   disable_shared_storage,
                                                   check_gluster_shared_volume)
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['distributed'], ['glusterfs']])
class SharedStorage(GlusterBaseClass):

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        # Creating Volume
        g.log.info("Started creating volume")
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Volume creation failed")
        g.log.info("Volume created successfully : %s", self.volname)

    def tearDown(self):
        # Stopping and cleaning up the volume
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Failed Cleanup the Volume")
            g.log.info("Volume deleted successfully : %s", volume)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_shared_storage(self):
        """This test case includes:
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
        """
        # pylint: disable=too-many-statements, too-many-branches
        # Enable a shared storage without specifying the domain
        ret = enable_shared_storage(self.mnode)
        self.assertTrue(ret, ("Failed to enable a shared storage"))
        g.log.info("Successfully enabled: enable-shared-storage option")

        # Check volume list to confirm gluster_shared_storage is created
        ret = check_gluster_shared_volume(self.mnode)
        self.assertTrue(ret, ("gluster_shared_storage volume not"
                              " created even after enabling it"))
        g.log.info("gluster_shared_storage volume created"
                   " successfully")

        # Check the shared volume got mounted
        ret = is_shared_volume_mounted(self.mnode)
        self.assertTrue(ret, ("Shared volume not mounted even"
                              " after enabling it"))
        g.log.info("Shared volume mounted successfully")

        # Disable a shared storage without specifying the domain
        ret = disable_shared_storage(self.mnode)
        self.assertTrue(ret, ("Failed to disable a shared storage"))
        g.log.info("Successfully disabled: disable-shared-storage")

        # Check volume list to confirm gluster_shared_storage is deleted
        ret = check_gluster_shared_volume(self.mnode, present=False)
        self.assertTrue(ret, ("gluster_shared_storage volume not"
                              " deleted even after disabling it"))
        g.log.info("gluster_shared_storage volume deleted"
                   " successfully")

        # Check the shared volume unmounted
        ret = is_shared_volume_mounted(self.mnode)
        self.assertFalse(ret, ("Shared volume not unmounted even"
                               " after disabling it"))
        g.log.info("Shared volume unmounted successfully")

        # Create a volume with name gluster_shared_storage
        g.log.info("creation of volume should succeed")
        volume = "gluster_shared_storage"
        bricks_list = form_bricks_list(self.mnode, volume,
                                       2, self.servers,
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

        # Enable the shared storage
        ret = enable_shared_storage(self.mnode)
        self.assertTrue(ret, ("Failed to enable a shared storage"))
        g.log.info("Successfully enabled: enable-shared-storage option")

        # Check volume list to confirm gluster_shared_storage is created
        ret = check_gluster_shared_volume(self.mnode)
        self.assertTrue(ret, ("gluster_shared_storage volume not"
                              " created even after enabling it"))
        g.log.info("gluster_shared_storage volume created"
                   " successfully")

        # Check the shared volume got mounted
        ret = is_shared_volume_mounted(self.mnode)
        self.assertTrue(ret, ("Shared volume not mounted even"
                              " after enabling it"))
        g.log.info("Shared volume mounted successfully")

        # Disable a shared storage
        ret = disable_shared_storage(self.mnode)
        self.assertTrue(ret, ("Failed to disable a shared storage"))
        g.log.info("Successfully disabled: disable-shared-storage")

        # Check volume list to confirm gluster_shared_storage is deleted
        ret = check_gluster_shared_volume(self.mnode, present=False)
        self.assertTrue(ret, ("gluster_shared_storage volume not"
                              " deleted even after disabling it"))
        g.log.info("gluster_shared_storage volume deleted"
                   " successfully")

        # Check the shared volume unmounted
        ret = is_shared_volume_mounted(self.mnode)
        self.assertFalse(ret, ("Shared volume not unmounted even"
                               " after disabling it"))
        g.log.info("Shared volume unmounted successfully")
