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
snapshot listing before and after
glusterd restart.

"""
import time
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.gluster_init import (restart_glusterd,
                                             is_glusterd_running)
from glustolibs.gluster.snap_ops import (snap_create, snap_delete,
                                         get_snap_list)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class SnapshotGlusterddown(GlusterBaseClass):

    def setUp(self):
        # SettingUp volume and Mounting the volume
        GlusterBaseClass.setUp.im_func(self)
        g.log.info("Starting to SetUp Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to setup volume %s" % self.volname)
        g.log.info("Volume %s has been setup successfully", self.volname)

    def tearDown(self):

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

    def test_snap_delete_and_list_glusterd_down(self):
        # pylint: disable=too-many-statements

        """
        Steps:

        1. create a volume
        2. mount volume
        3. create 3 snapshot of that volume
        4. delete snapshot snap1
        5. list all snapshots created
        6. restart glusterd
        7. list all snapshots created
           except snap1
        """

        # Creating snapshot:
        g.log.info("Starting to Create snapshot")
        for snap_count in range(0, 3):
            self.snap = "snap%s" % snap_count
            ret, _, _ = snap_create(self.mnode, self.volname, self.snap)
            self.assertEqual(ret, 0, ("Failed to create snapshot for "
                                      "volume %s" % self.volname))
            g.log.info("Snapshot %s created successfully "
                       "for volume %s", self.snap, self.volname)

        # delete snap1 snapshot
        g.log.info("Starting to Delete snapshot snap1")
        ret, _, _ = snap_delete(self.mnode, "snap1")
        self.assertEqual(ret, 0, "Failed to delete"
                         "snapshot snap1")
        g.log.info("Snapshots snap1 deleted Successfully")

        # snapshot list
        g.log.info("Starting to list all snapshots")
        out = get_snap_list(self.mnode)
        self.assertIsNotNone(out, "Failed to list all snapshots")
        self.assertEqual(len(out), 2, "Failed to validate snap list")
        g.log.info("Successfully validated snap list")

        # restart Glusterd
        g.log.info("Restarting Glusterd on all nodes")
        ret = restart_glusterd(self.servers)
        self.assertTrue(ret, "Failed to restart glusterd on nodes"
                        "%s" % self.servers)
        g.log.info("Successfully restarted glusterd on nodes"
                   " %s", self.servers)

        # check glusterd running
        g.log.info("Checking glusterd is running or not")
        count = 0
        while count < 80:
            ret = is_glusterd_running(self.servers)
            if ret == 0:
                break
            time.sleep(2)
            count += 1

        self.assertEqual(ret, 0, "Failed to validate glusterd "
                         "running on nodes %s" % self.servers)
        g.log.info("glusterd is running on "
                   "nodes %s", self.servers)

        # snapshot list
        g.log.info("Starting to list all snapshots")
        for server in self.servers[0:]:
            out = get_snap_list(server)
            self.assertIsNotNone(out, "Falied to list snap in node"
                                 "%s" % server)
            self.assertEqual(len(out), 2, "Failed to validate snap list"
                             "on node %s" % server)
            g.log.info("Successfully validated snap list on node %s", server)
