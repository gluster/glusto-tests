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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import get_volume_options
from glustolibs.gluster.snap_scheduler import (scheduler_init,
                                               scheduler_enable,
                                               scheduler_status,
                                               scheduler_disable)
from glustolibs.gluster.shared_storage_ops import (enable_shared_storage,
                                                   is_shared_volume_mounted,
                                                   disable_shared_storage)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class SnapshotSchedulerStatus(GlusterBaseClass):
    """
    SnapshotSchedulerStatus includes tests which verify the snap_scheduler
    functionality WRT the status and shared storage
    """

    def setUp(self):
        """
        setup volume for the test
        """

        # SettingUp volume and Mounting the volume
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):
        """
        tearDown for every test
        """

        # Check if shared storage is enabled
        # Disable if true
        g.log.info("Checking if shared storage is mounted")
        ret = is_shared_volume_mounted(self.mnode)
        if ret:
            g.log.info("Disabling shared storage")
            ret = disable_shared_storage(self.mnode)
            if not ret:
                raise ExecutionError("Failed to disable shared storage")
            g.log.info("Successfully disabled shared storage")

        # Unmount and cleanup-volume
        g.log.info("Starting to cleanup-volume")
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume %s" % self.volname)
        g.log.info("Successful in Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_snap_scheduler_status(self):
        # pylint: disable=too-many-statements
        """
        Validating the snapshot scheduler behavior when shared storage
        volume is mounted/not mounted.

        * Initialise snap_scheduler without enabling shared storage
        * Enable shared storage
        * Initialise snap_scheduler on all nodes
        * Check snap_scheduler status
        """

        # Validate shared storage is disabled
        g.log.info("Validating shared storage is disabled")
        volinfo = get_volume_options(self.mnode, self.volname,
                                     option=("cluster.enable-shared-storage"))
        if volinfo["cluster.enable-shared-storage"] == "disable":
            # Initialise snapshot scheduler
            g.log.info("Initialising snapshot scheduler on all nodes")
            ret = scheduler_init(self.servers)
            self.assertFalse(ret, "Unexpected: Successfully initialized "
                             "scheduler on all nodes")
            g.log.info("Expected: Failed to initialize snap_scheduler on "
                       "all nodes")
        self.assertEqual(volinfo["cluster.enable-shared-storage"],
                         "disable", "Unexpected: Shared storage "
                         "is enabled on cluster")

        # Enable shared storage
        g.log.info("enabling shared storage")
        ret = enable_shared_storage(self.mnode)
        self.assertTrue(ret, "Failed to enable shared storage")
        g.log.info("Successfully enabled shared storage")

        # Validate shared storage volume is mounted
        g.log.info("Validating if shared storage volume is mounted")
        count = 0
        while count < 5:
            ret = is_shared_volume_mounted(self.mnode)
            if ret:
                break
            sleep(2)
            count += 1
        self.assertTrue(ret, "Failed to validate if shared volume is mounted")
        g.log.info("Successfully validated shared volume is mounted")

        # Validate shared storage volume is enabled
        g.log.info("Validate shared storage is enabled")
        volinfo = get_volume_options(self.mnode, self.volname,
                                     option=("cluster.enable-shared-storage"))
        self.assertIsNotNone(volinfo, "Failed to validate volume option")
        self.assertEqual(volinfo["cluster.enable-shared-storage"], "enable",
                         "Failed to validate if shared storage is enabled")
        g.log.info("Successfully validated shared storage is enabled")

        # Initialise snap_scheduler on all nodes
        g.log.info("Initialising snapshot scheduler on all nodes")
        count = 0
        while count < 40:
            ret = scheduler_init(self.servers)
            if ret:
                break
            sleep(2)
            count += 1
        self.assertTrue(ret, "Failed to initialize scheduler on all nodes")
        g.log.info("Successfully initialized scheduler on all nodes")

        # Enable snap_scheduler
        g.log.info("Enabling snap_scheduler")
        ret, _, _ = scheduler_enable(self.mnode)
        self.assertEqual(ret, 0, "Failed to enable scheduler on %s node" %
                         self.mnode)
        g.log.info("Successfully enabled scheduler on %s node", self.mnode)

        # Check snapshot scheduler status
        g.log.info("checking status of snapshot scheduler")
        for server in self.servers:
            count = 0
            while count < 40:
                ret, status, _ = scheduler_status(server)
                if ret == 0:
                    self.assertEqual(status.strip().split(":")[2], ' Enabled',
                                     "Failed to check status of scheduler")
                    break
                sleep(2)
                count += 1
            self.assertEqual(ret, 0, "Failed to check status of scheduler"
                             " on node %s" % server)
            g.log.info("Successfully checked scheduler status on %s nodes",
                       server)

        # disable snap scheduler
        g.log.info("disabling snap scheduler")
        ret, _, _ = scheduler_disable(self.mnode)
        self.assertEqual(ret, 0, "Unexpected: Failed to disable "
                         "snapshot scheduler")
        g.log.info("Successfully disabled snapshot scheduler")

        # Check snapshot scheduler status
        g.log.info("checking status of snapshot scheduler")
        for server in self.servers:
            count = 0
            while count < 40:
                ret, status, _ = scheduler_status(server)
                if not ret:
                    self.assertEqual(status.strip().split(":")[2], ' Disabled',
                                     "Failed to check status of scheduler")
                    break
                sleep(2)
                count += 1
            self.assertEqual(ret, 0, "Failed to check status of scheduler"
                             " on node %s" % server)
            g.log.info("Successfully checked scheduler status on %s nodes",
                       server)
