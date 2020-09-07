#  Copyright (C) 2017-2020 Red Hat, Inc. <http://www.redhat.com>
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
        The purpose of this test case is to validate Snapshot
        activation and deactivation.
        Pre-Activate, Post-Activate and Post-Deactivate
"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_delete_all,
                                         snap_activate,
                                         snap_deactivate,
                                         get_snap_info_by_snapname,
                                         get_snap_status_by_snapname)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestActivateDeactivate(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        """
        setup volume and initialize necessary variables
        """

        cls.get_super_method(cls, 'setUpClass')()
        g.log.info("Starting %s:", cls.__name__)
        # Setup volume and mount
        g.log.info("Starting to Setup Volume")
        ret = cls.setup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup_Volume")
        g.log.info("Successful in Setup Volume")

    def tearDown(self):
        """
        tearDown for every test
        """
        ret, _, _ = snap_delete_all(self.mnode)
        if ret != 0:
            raise ExecutionError("Snapshot Delete Failed")
        g.log.info("Successfully deleted all snapshots")

        # Cleanup-volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_activate_deactivate(self):
        # pylint: disable=too-many-branches, too-many-statements
        """
        Verifying Snapshot activation/deactivation functionality.

        * Create Snapshot
        * Validate snapshot info before activation
        * Validate snapshot status before activation
        * Activate snapshot
        * Validate snapshot info after activation
        * Validate snapshot status after activation
        * Deactivate snapshot
        * Validate snapshot info after deactivation
        * Validate snapshot status after deactivation
        """

        # Create Snapshot
        snap_name = 'snap_%s' % self.volname
        g.log.info("Starting to Create Snapshot %s", snap_name)
        ret, _, _ = snap_create(self.mnode, self.volname, snap_name)
        self.assertEqual(ret, 0, ("Snapshot Creation failed for %s",
                                  snap_name))
        g.log.info("Successfully created Snapshot %s for volume %s",
                   snap_name, self.volname)

        # Validate Snapshot Info Before Activation
        g.log.info("Validating 'snapshot info' in 'stopped' state before "
                   "activating the snapshot")
        ret = get_snap_info_by_snapname(self.mnode, snap_name)
        self.assertIsNotNone(ret, ("Failed to Fetch Snapshot info for %s",
                                   snap_name))
        g.log.info("Snapshot info Success for %s", ret['snapVolume']['status'])
        self.assertEqual(ret['snapVolume']['status'], 'Stopped',
                         ("Unexpected: Snapshot %s Status is in Started state",
                          snap_name))
        g.log.info("Expected: Snapshot is in Stopped state as it is "
                   "not Activated")

        # Validate Snapshot Status Before Activation
        g.log.info("Validating 'snapshot status' in 'stopped' state before "
                   "activating the snapshot")
        ret = get_snap_status_by_snapname(self.mnode, snap_name)
        self.assertIsNotNone(ret, ("Failed to Fetch Snapshot status for %s",
                                   snap_name))
        g.log.info("Snapshot Status Success for %s", snap_name)
        for brick in ret['volume']['brick']:
            self.assertEqual(brick['pid'], 'N/A',
                             ("Unexpected: Brick Pid '%s' is available for %s",
                              brick['pid'], brick['path']))
        g.log.info("Expected: Deactivated Snapshot Brick PID is 'N/A'")

        # Activate Snapshot
        g.log.info("Starting to Activate %s", snap_name)
        ret, _, _ = snap_activate(self.mnode, snap_name)
        self.assertEqual(ret, 0, ("Snapshot Activation Failed for %s",
                                  snap_name))
        g.log.info("Snapshot %s Activated Successfully", snap_name)

        # Validate Snapshot Info After Activation
        g.log.info("Validating 'snapshot info' in 'started' state after"
                   " activating the snapshot")
        snap_info = get_snap_info_by_snapname(self.mnode, snap_name)
        self.assertEqual(snap_info['snapVolume']['status'], "Started",
                         ("Failed to Fetch Snapshot info after activate "
                          "for %s", snap_name))
        g.log.info("Success: Snapshot info in 'started' state")

        # Validate Snaphot Status After Activation
        g.log.info("Validating 'snapshot status' in started state after "
                   "activating the snapshot")
        ret = get_snap_status_by_snapname(self.mnode, snap_name)
        for brick in ret['volume']['brick']:
            self.assertNotEqual(brick['pid'], 'N/A',
                                ("Brick Path %s  Not Available for Activated "
                                 "Snapshot %s", (brick['path'], snap_name)))
        g.log.info("Sucessfully validated Activated Snapshot Brick Path "
                   "Available")

        # Deactivate Snapshot
        g.log.info("Starting to Deactivate %s", snap_name)
        ret, _, _ = snap_deactivate(self.mnode, snap_name)
        self.assertEqual(ret, 0, ("Snapshot Deactivation Failed for %s",
                                  snap_name))
        g.log.info("Successfully Deactivated Snapshot %s", snap_name)

        # Validate Snapshot Info After Deactivation
        g.log.info("Validating 'snapshot info' in stopped state after "
                   "deactivating the snapshot")
        ret = get_snap_info_by_snapname(self.mnode, snap_name)
        self.assertEqual(ret['snapVolume']['status'], 'Stopped',
                         ("Snapshot Status is not in 'Stopped' State"))
        g.log.info("Expected: Snapshot is in Stopped state after Deactivation")

        # Validate Snaphot Status After Deactivation
        g.log.info("Validating 'snapshot status' in started state after "
                   "deactivating the snapshot")
        ret = get_snap_status_by_snapname(self.mnode, snap_name)
        for brick in ret['volume']['brick']:
            self.assertEqual(brick['pid'], 'N/A',
                             ("Deactivated Snapshot Brick Pid %s available "
                              "for %s", brick['pid'], brick['path']))
        g.log.info("Expected: Deactivated Snapshot Brick PID is 'N/A'")
