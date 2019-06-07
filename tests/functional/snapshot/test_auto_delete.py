#  Copyright (C) 2016-2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.snap_ops import (snap_create,
                                         set_snap_config,
                                         get_snap_config,
                                         get_snap_list)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'distributed'],
          ['glusterfs', 'nfs', 'cifs']])
class TestSnapAutoDelete(GlusterBaseClass):
    """
    TestSnapAutoDelete contains tests which verifies the deletion of
    snapshots along with the snapshot config option 'auto-delete'
    """

    @classmethod
    def setUpClass(cls):
        """
        setup volume and initialize necessary variables which is used in tests
        """

        # calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()
        # Setup Volume
        ret = cls.setup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup_Volume %s" % cls.volname)
        g.log.info("Successful in Setup Volume %s", cls.volname)
        cls.autodel_enable = {'auto-delete': 'enable'}

    def setUp(self):
        """
        Initialize necessary variables.
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Gather the default snapshot config option values
        self.snap_conf = get_snap_config(self.mnode)
        self.assertIsNotNone(
            self.snap_conf, "Failed to get the snapshot config options")
        softlim = self.snap_conf['systemConfig']['softLimit']
        self.def_conf_softlim = {'snap-max-soft-limit': softlim[:-1]}
        autodel = self.snap_conf['systemConfig']['autoDelete']
        self.def_conf_autodel = {'auto-delete': autodel}
        g.log.info("Successfully gathered the default snapshot config options")

        self.snapshots = [('snap-test-snap-auto-delete-%s-%s'
                           % (self.volname, i))for i in range(0, 20)]

    def tearDown(self):
        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

        # setting back default max-soft-limit to 90%
        ret, _, _ = set_snap_config(self.mnode, self.def_conf_softlim)
        if ret:
            raise ExecutionError("Failed to set the default config options "
                                 "for snap-max-soft-limit")
        g.log.info("Successfully set the snapshot config options to default")

        # setting back default value for auto-delete config option
        ret, _, _ = set_snap_config(self.mnode, self.def_conf_autodel)
        if ret:
            raise ExecutionError("Failed to set the default config option for "
                                 "auto-delete")
        g.log.info("Successfully set the snapshot config options to default")

    @classmethod
    def tearDownClass(cls):
        # calling GlusterBaseClass tearDownClass
        cls.get_super_method(cls, 'tearDownClass')()

        # Clean up the volume
        ret = cls.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

    def test_snap_auto_delete(self):
        """
        Verifying snapshot auto-delete config option

        * Enable auto-delete snapshot
        * Set snap-max-hard limit and snap-max-soft-limit
        * Validate snap-max-hard-limit and snap-max-soft-limit
        * Verify the limits by creating another 20 snapshots
        * Oldest of newly created snapshots will be deleted
        * Retaining the latest 8 (softlimit) snapshots
        * Cleanup snapshots and volumes
        """

        # pylint: disable=too-many-statements
        # Enable auto-delete snapshot config option
        ret, _, _ = set_snap_config(self.mnode, self.autodel_enable)
        self.assertEqual(ret, 0, ("Failed to enable auto-delete snapshot "
                                  "config option on volume %s", self.volname))
        g.log.info("Successfully enabled snapshot auto-delete")

        # Set snap-max-hard-limit snapshot config option for volume
        max_hard_limit = {'snap-max-hard-limit': '10'}
        ret, _, _ = set_snap_config(self.mnode, max_hard_limit, self.volname)
        self.assertEqual(ret, 0, ("Failed to set snap-max-hard-limit"
                                  "config option for volume %s", self.volname))
        g.log.info("Successfully set snap-max-hard-limit config option for"
                   "volume %s", self.volname)

        # Validate snap-max-hard-limit snapshot config option
        hard_limit_val = get_snap_config(self.mnode)
        self.assertEqual(hard_limit_val['volumeConfig'][0]['hardLimit'], '10',
                         ("Failed to Validate snap-max-hard-limit"))
        g.log.info("Successfully validated snap-max-hard-limit")

        # Set snap-max-soft-limit snapshot config option
        max_soft_limit = {'snap-max-soft-limit': '80'}
        ret, _, _ = set_snap_config(self.mnode, max_soft_limit)
        self.assertEqual(ret, 0, ("Failed to set snap-max-soft-limit"
                                  "config option"))
        g.log.info("Successfully set snap-max-soft-limit config option")

        # Validate snap-max-soft-limit snapshot config option
        soft_limit_val = get_snap_config(self.mnode)
        self.assertEqual(soft_limit_val['volumeConfig'][0]['softLimit'], '8',
                         ("Failed to Validate max-soft-limit"))
        g.log.info("Successfully validated snap-max-soft-limit")

        # Create 20 snapshots. As the count of snapshots crosses the
        # soft-limit the oldest of newly created snapshot should
        # be deleted and only the latest 8 snapshots must remain.
        for snapname in self.snapshots:
            ret, _, _ = snap_create(self.mnode, self.volname, snapname,
                                    description="This is the Description wit#"
                                    " ($p3c1al) ch@r@cters!")
            self.assertEqual(ret, 0, ("Failed to create snapshot %s for "
                                      "volume %s", snapname, self.volname))
            g.log.info("Snapshot snap%s of volume %s created successfully",
                       snapname, self.volname)

        # Perform snapshot list to get total number of snaps after auto-delete
        # Validate the existence of the snapshots using the snapname
        snaplist = get_snap_list(self.mnode)
        self.assertEqual(len(snaplist), 8,
                         ("Failed: The snapshot count is not as expected"))
        for snapname in self.snapshots[-8:]:
            self.assertIn(snapname, snaplist, "Failed to validate snapshot "
                          "existence for the snapshot %s" % snapname)
        g.log.info("Successful in validating the Snapshot count and existence "
                   "by snapname")
