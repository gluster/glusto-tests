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

""" Description:
       Enable snapshot activate-on-create and
       validate with multiple snapshot creation
"""
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.snap_ops import (snap_create,
                                         snap_delete_all,
                                         get_snap_info_by_snapname,
                                         get_snap_status_by_snapname,
                                         get_snap_config, set_snap_config)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestActivateOnCreate(GlusterBaseClass):
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
        cls.option_enable = {'activate-on-create': 'enable'}
        cls.option_disable = {'activate-on-create': 'disable'}

    def tearDown(self):
        """
        tearDown for every test
        """
        ret, _, _ = snap_delete_all(self.mnode)
        if ret != 0:
            raise ExecutionError("Snapshot Delete Failed")
        ret, _, _ = set_snap_config(self.mnode, self.option_disable)
        if ret != 0:
            raise ExecutionError("Failed to execute set_snap_config")
        ret = get_snap_config(self.mnode, self.volname)
        if ret is None:
            raise ExecutionError("Failed to execute get_snap_config")
        if 'disable' not in ret['systemConfig']['activateOnCreate']:
            raise ExecutionError("Failed to disable activate-on-create")
        g.log.info("set_snap_config Success to disable "
                   "activate-on-create")

        # Cleanup-volume
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_activate_on_create(self):
        # pylint: disable=too-many-branches, too-many-statements
        """
        Verifying Snapshot activate on create

        * Create a default snapshot
        * Enable activate-on-create snapshot config option
        * Create more snapshots
            * Validate snapshot info after snapshot create. It should be in
              started state.
            * Validate snapshot status after snapshot create. It should be in
              started state.
        * Validate the default snapshot info and status. It should be in
          stopped state
        """

        # Create Default Snapshot
        g.log.info("Starting to Create Snapshot one")
        snap_default = "snap_%s" % self.volname
        ret, _, _ = snap_create(self.mnode, self.volname, snap_default)
        self.assertEqual(ret, 0, ("Snapshot Creation failed for %s",
                                  snap_default))
        g.log.info("Successfully created Snapshot %s of volume %s",
                   snap_default, self.volname)

        # Enable activate_on_create snapshot
        g.log.info("Enabling snapshot activate-on-create config option")
        ret, _, _ = set_snap_config(self.mnode, self.option_enable)
        self.assertEqual(ret, 0, "Failed to execute set_snap_config")
        g.log.info("Validating the value of activate-on-create")
        ret = get_snap_config(self.mnode, self.volname)
        self.assertIsNotNone(ret, ("Failed to execute get_snap_config"))
        self.assertIn('enable', (ret['systemConfig']['activateOnCreate']),
                      ("Failed to validate activate-on-create value as "
                       "'enabled'"))
        g.log.info("Successfully enabled activate-on-create")

        # Create Snapshots after enabling activate-on-create
        g.log.info("Starting to Create Snapshots")
        for snap_count in range(1, 5):
            snap_name = "snap_%s" % snap_count
            ret, _, _ = snap_create(self.mnode, self.volname, snap_name)
            self.assertEqual(ret, 0, ("Snapshot Creation failed for %s",
                                      snap_name))
            g.log.info("Successfully created Snapshot %s of volume %s",
                       snap_name, self.volname)

            # Validate Snapshot Info After Snapshot Create
            g.log.info("Validating 'snapshot info' after enabling "
                       "activate-on-create")
            ret = get_snap_info_by_snapname(self.mnode, snap_name)
            self.assertIsNotNone(ret, ("Failed to Fetch Snapshot info after "
                                       "activation for %s", snap_name))
            g.log.info("Snapshot info Success for %s",
                       ret['snapVolume']['status'])
            self.assertEqual(ret['snapVolume']['status'], 'Started',
                             ("Activated Snapshot is in Stopped State %s",
                              (ret['snapVolume']['status'])))
            g.log.info("Snapshot %s is Activated By Default - %s state",
                       snap_name, (ret['snapVolume']['status']))

            # Validate Snaphot Status After Snapshot Create
            g.log.info("Validating 'snapshot status' after enabling "
                       "activate-on-create")
            ret = get_snap_status_by_snapname(self.mnode, snap_name)
            self.assertIsNotNone("Failed to Fetch Snapshot status for %s",
                                 snap_name)
            g.log.info("Snapshot Status Success for %s", snap_name)
            for brick in ret['volume']['brick']:
                self.assertNotEqual(brick['pid'], 'N/A',
                                    ("Brick Path %s  Not Available for "
                                     "Activated Snapshot %s",
                                     (brick['path'], snap_name)))
            g.log.info("Success: Snapshot Brick Path Available")

        # Validate Snapshot Info for the 'default' snapshot
        # Expected to be Stopped
        g.log.info("Validating 'snapshot info' of the 'default' snapshot")
        ret = get_snap_info_by_snapname(self.mnode, snap_default)
        self.assertIsNotNone(ret, ("Failed to Fetch Snapshot info for %s",
                                   snap_default))
        g.log.info("Snapshot info Success for %s", ret['snapVolume']['status'])
        self.assertEqual(ret['snapVolume']['status'], 'Stopped',
                         ("Snapshot Status is not in Stopped State"))
        g.log.info("Snapshot %s is in Stopped state as it "
                   "is not Activated", snap_default)

        # Validate Snapshot Status for the 'default' snapshot
        # Expected to be N/A
        g.log.info("Validating 'snapshot status' of the 'default' snapshot")
        ret = get_snap_status_by_snapname(self.mnode, snap_default)
        self.assertIsNotNone(ret, ("Failed to Fetch Snapshot status for %s",
                                   snap_default))
        g.log.info("Snapshot Status Success for %s", snap_default)
        for brick in ret['volume']['brick']:
            self.assertEqual(brick['pid'], 'N/A',
                             ("Brick Pid available for %s", brick['path']))
        g.log.info("Success: Snapshot %s Brick PID is 'N/A'", snap_default)
