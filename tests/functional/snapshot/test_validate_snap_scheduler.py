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

This test cases will validate snapshot scheduler behaviour
when we enable/disable scheduler.

"""
import time
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.volume_ops import get_volume_info
from glustolibs.gluster.shared_storage_ops import (enable_shared_storage,
                                                   is_shared_volume_mounted,
                                                   disable_shared_storage)
from glustolibs.gluster.snap_scheduler import (scheduler_init,
                                               scheduler_enable,
                                               scheduler_status,
                                               scheduler_disable)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class SnapshotSchedulerBehaviour(GlusterBaseClass):

    def setUp(self):

        # SettingUp volume and Mounting the volume
        GlusterBaseClass.setUpClass.im_func(self)
        g.log.info("Starting to SetUp and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):

        # Disable shared storage
        g.log.info("Disabling shared storage")
        ret = disable_shared_storage(self.mnode)
        self.assertTrue(ret, "Failed to disable shared storage")
        g.log.info("Successfully disabled shared storage")

        # Unmount and cleanup-volume
        g.log.info("Starting to Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

    def test_snap_scheduler_behaviour(self):

        """
        Steps:
        1. Create volumes
        2. Enable shared storage
        3. Validate shared storage mounted
        4. Validate shared storage is enabled
        5. Initialise snapshot scheduler on all node
        6. Enable snapshot scheduler
        7. Validate snapshot scheduler status
        8. Disable snapshot scheduler
        9. Validate snapshot scheduler status
        """

        # Enable shared storage
        g.log.info("Enable shared storage")
        ret = enable_shared_storage(self.mnode)
        self.assertTrue(ret, "Failed to enable shared storage")
        g.log.info("Successfully enabled shared storage")

        # Validate shared storage mounted
        g.log.info("Starting to validate shared storage mounted")
        for server in self.servers:
            ret = is_shared_volume_mounted(server)
            self.assertTrue(ret, "Failed to mount shared volume")
        g.log.info("Successfully mounted shared volume")

        # Validate shared storage is enabled
        g.log.info("Starting to validate shared storage volume")
        self.shared = "gluster_shared_storage"
        volinfo = get_volume_info(self.mnode, self.shared)
        self.assertEqual(volinfo['gluster_shared_storage']['options']
                         ['cluster.enable-shared-storage'], 'enable',
                         "shared storage is disabled")
        g.log.info("Shared storage enabled successfully")

        # Initialise snap scheduler
        g.log.info("Initialising snapshot scheduler on all nodes")
        ret = scheduler_init(self.servers)
        self.assertTrue(ret, "Failed to initialize scheduler on all nodes")
        g.log.info("Successfully initialized scheduler on all nodes")

        # Enable snap scheduler
        g.log.info("Starting to enable snapshot scheduler on all nodes")
        ret, _, _ = scheduler_enable(self.mnode)
        self.assertEqual(ret, 0, "Failed to enable scheduler on all servers")
        g.log.info("Successfully enabled scheduler on all nodes")

        # Check snapshot scheduler status
        g.log.info("checking status of snapshot scheduler")
        for server in self.servers:
            count = 0
            while count < 40:
                ret, status, _ = scheduler_status(server)
                if status.strip().split(":")[2] == ' Enabled':
                    break
                time.sleep(2)
                count += 2
        self.assertEqual(status.strip().split(":")[2], ' Enabled',
                         "Failed to check status of scheduler")
        g.log.info("Successfully checked scheduler status")

        # Disable snap scheduler
        g.log.info("Starting to disable snapshot scheduler on all nodes")
        ret, _, _ = scheduler_disable(self.mnode)
        self.assertEqual(ret, 0, "Failed to disable scheduler on node"
                         " %s" % self.mnode)
        g.log.info("Successfully disabled scheduler on all nodes")

        # Check snapshot scheduler status
        g.log.info("checking status of snapshot scheduler")
        for server in self.servers:
            count = 0
            while count < 40:
                ret, status, _ = scheduler_status(server)
                if status.strip().split(":")[2] == ' Disabled':
                    break
                time.sleep(2)
                count += 2
        self.assertEqual(status.strip().split(":")[2], ' Disabled',
                         "Failed to check status of scheduler")
        g.log.info("Successfully checked scheduler status")
