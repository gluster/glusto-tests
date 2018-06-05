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
    Test Cases in this module tests the
    snapshot scheduler behavior when shared volume is mounted/not
    mounted. scheduler command such as initialise scheduler,
    enable scheduler, status of scheduler.
"""
import time
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.volume_ops import get_volume_options
from glustolibs.gluster.snap_scheduler import (scheduler_init,
                                               scheduler_enable,
                                               scheduler_status,
                                               scheduler_disable)
from glustolibs.gluster.shared_storage_ops import (enable_shared_storage,
                                                   is_shared_volume_mounted,
                                                   is_shared_volume_unmounted,
                                                   disable_shared_storage)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class SnapshotSchedulerStatus(GlusterBaseClass):

    def setUp(self):

        # SettingUp volume and Mounting the volume
        GlusterBaseClass.setUp.im_func(self)
        g.log.info("Starting to SetUp and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):

        # Unmount and cleanup-volume
        g.log.info("Starting to Unmount and cleanup-volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

    def test_snap_scheduler_status(self):
        # pylint: disable=too-many-statements
        """
        Steps:
        1. create volumes
        2. initialise snap scheduler without
           enabling shared storage should fail
        3. enable shared storage
        4. initialise snap scheduler
        5. check snapshot scheduler status
        """
        # Validate shared storage is enabled
        g.log.info("Starting to validate shared storage volume")
        volinfo = get_volume_options(self.mnode, self.volname,
                                     option=("cluster.enable"
                                             "-shared-storage"))
        if volinfo["cluster.enable-shared-storage"] == "disable":
            # Initialise snapshot scheduler
            g.log.info("Initialising snapshot scheduler on all nodes")
            ret = scheduler_init(self.servers)
            self.assertFalse(ret, "Unexpected: Successfully initialized "
                             "scheduler on all nodes")
            g.log.info("As Expected, Failed to initialize scheduler on "
                       "all nodes")
        self.assertEqual(volinfo["cluster.enable-shared-storage"],
                         "disable", "Unexpected: Shared storage "
                         "is enabled on cluster")

        # Enable Shared storage
        g.log.info("enabling shared storage")
        ret = enable_shared_storage(self.mnode)
        self.assertTrue(ret, "Failed to enable shared storage")
        g.log.info("Successfully enabled shared storage")

        # Validate shared storage mounted
        g.log.info("validate shared storage mounted")
        ret = is_shared_volume_mounted(self.mnode)
        self.assertTrue(ret, "Failed to mount shared volume")
        g.log.info("Successfully mounted shared volume")

        # Validate shared storage volume is enabled
        g.log.info("validate shared storage volume")
        volinfo = get_volume_options(self.mnode, self.volname,
                                     option=("cluster.enable"
                                             "-shared-storage"))
        self.assertIsNotNone(volinfo, "Failed to validate volume option")
        self.assertEqual(volinfo["cluster.enable-shared-storage"], "enable",
                         "Failed to enable shared storage volume")
        g.log.info("Shared storage enabled successfully")

        # Initialise snap scheduler
        g.log.info("Initialising snapshot scheduler on all nodes")
        count = 0
        while count < 40:
            ret = scheduler_init(self.servers)
            if ret:
                break
            time.sleep(2)
            count += 1
        self.assertTrue(ret, "Failed to initialize scheduler on all nodes")
        g.log.info("Successfully initialized scheduler on all nodes")

        # Enable snap scheduler
        g.log.info("Enabling snap scheduler")
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
                time.sleep(2)
                count += 1
            self.assertEqual(ret, 0, "Failed to check status of scheduler"
                             " on nodes %s" % server)
            g.log.info("Successfully checked scheduler status on %s nodes",
                       server)

        # disable snap scheduler
        g.log.info("disabling snap scheduler")
        ret, _, _ = scheduler_disable(self.mnode)
        self.assertEqual(ret, 0, "Unexpected: Failed to disable "
                         "snapshot scheduler")
        g.log.info("Successfully disabled snapshot scheduler")

        # disable shared storage
        g.log.info("starting to disable shared storage")
        ret = disable_shared_storage(self.mnode)
        self.assertTrue(ret, "Failed to disable shared storage")
        g.log.info("Successfully disabled shared storage")

        # Validate shared volume unmounted
        g.log.info("Validate shared volume unmounted")
        ret = is_shared_volume_unmounted(self.mnode)
        self.assertTrue(ret, "Failed to unmount shared storage")
        g.log.info("Successfully unmounted shared storage")
