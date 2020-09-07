#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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

from multiprocessing.pool import ThreadPool
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              rebalance_status,
                                              wait_for_fix_layout_to_complete)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import get_fattr


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestRemoveBrickScenarios(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Override Volumes
        cls.volume['voltype'] = {
            'type': 'distributed-replicated',
            'dist_count': 2,
            'replica_count': 3,
            'transport': 'tcp'}

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Creating Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)
        g.log.info("Volme created and mounted successfully : %s",
                   self.volname)

    def tearDown(self):

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Unable to delete volume % s" % self.volname)
        g.log.info("Volume deleted successfully : %s", self.volname)

        self.get_super_method(self, 'tearDown')()

    def test_remove_brick_scenarios(self):
        # pylint: disable=too-many-statements
        """
        Test case:
        1. Create a cluster by peer probing and create a volume.
        2. Mount it and write some IO like 100000 files.
        3. Initiate the remove-brick operation on pair of bricks.
        4. Stop the remove-brick operation using other pairs of bricks.
        5. Get the remove-brick status using other pair of bricks in
           the volume.
        6. stop the rebalance process using non-existing brick.
        7. Check for the remove-brick status using non-existent bricks.
        8. Stop the remove-brick operation where remove-brick start have been
            initiated.
        9. Perform fix-layout on the volume.
        10. Get the rebalance fix-layout.
        11. Create a directory from mountpoint.
        12. check for 'trusted.glusterfs.dht' extended attribute in the
            newly created directory in the bricks where remove brick stopped
            (which was tried to be removed in step 8).
        13. Umount, stop and delete the volume.
        """

        # Getting a list of all the bricks.
        g.log.info("Get all the bricks of the volume")
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        # Running IO.
        pool = ThreadPool(5)
        # Build a command per each thread
        # e.g. "seq 1 20000 ... touch" , "seq 20001 40000 ... touch" etc
        cmds = ["seq {} {} | sed 's|^|{}/test_file|' | xargs touch".
                format(i, i + 19999, self.mounts[0].mountpoint)
                for i in range(1, 100000, 20000)]
        # Run all commands in parallel (each thread returns a tuple from g.run)
        ret = pool.map(
            lambda command: g.run(self.mounts[0].client_system, command), cmds)
        # ret -> list of tuples [(return_code, stdout, stderr),...]
        pool.close()
        pool.join()
        # Verify all commands' exit code is 0 (first element of each tuple)
        for thread_return in ret:
            self.assertEqual(thread_return[0], 0, "File creation failed.")
        g.log.info("Files create on mount point.")

        # Removing bricks from volume.
        remove_brick_list_original = bricks_list[3:6]
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list_original, 'start')
        self.assertEqual(ret, 0, "Failed to start remove brick operation.")
        g.log.info("Remove bricks operation started successfully.")

        # Stopping brick remove operation for other pair of bricks.
        remove_brick_list_other_pair = bricks_list[0:3]
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list_other_pair, 'stop')
        self.assertEqual(ret, 1, "Successfully stopped remove brick operation "
                                 "on other pair of bricks.")
        g.log.info("Failed to stop remove brick operation on"
                   " other pair of bricks.")

        # Checking status of brick remove operation for other pair of bricks.
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list_other_pair, 'status')
        self.assertEqual(ret, 1, "Error: Got status on other pair of bricks.")
        g.log.info("EXPECTED: Failed to get status on other pair of bricks.")

        # Stopping remove operation for non-existent bricks.
        remove_brick_list_non_existent = [bricks_list[0] + 'non-existent',
                                          bricks_list[1] + 'non-existent',
                                          bricks_list[2] + 'non-existent']
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list_non_existent, 'stop')
        self.assertEqual(ret, 1, "Error: Successfully stopped remove brick"
                                 " operation on non-existent bricks.")
        g.log.info("EXPECTED: Failed to stop remove brick operation"
                   " on non existent bricks.")

        # Checking status of brick remove opeation for non-existent bricks.
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list_non_existent, 'status')
        self.assertEqual(ret, 1,
                         "Error: Status on non-existent bricks successful.")
        g.log.info("EXPECTED: Failed to get status on non existent bricks.")

        # Stopping the initial brick remove opeation.
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list_original, 'stop')
        self.assertEqual(ret, 0, "Failed to stop remove brick operation")
        g.log.info("Remove bricks operation stop successfully")

        # Start rebalance fix layout for volume.
        g.log.info("Starting Fix-layout on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname, fix_layout=True)
        self.assertEqual(ret, 0, ("Failed to start rebalance for fix-layout"
                                  "on the volume %s", self.volname))
        g.log.info("Successfully started fix-layout on the volume %s",
                   self.volname)

        # Checking status of rebalance fix layout for the volume.
        ret, _, _ = rebalance_status(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to check status of rebalance"
                                  "on the volume %s", self.volname))
        g.log.info("Successfully checked status on the volume %s",
                   self.volname)
        ret = wait_for_fix_layout_to_complete(self.mnode,
                                              self.volname, timeout=30000)
        self.assertTrue(ret, ("Failed to check for rebalance."))
        g.log.info("Rebalance completed.")

        # Creating directory.
        dir_name = ''
        for counter in range(0, 10):
            ret = mkdir(self.mounts[0].client_system,
                        self.mounts[0].mountpoint + "/dir1" + str(counter),
                        parents=True)
            if ret:
                dir_name = "/dir1" + str(counter)
                break
        self.assertTrue(ret, ("Failed to create directory dir1."))
        g.log.info("Directory dir1 created successfully.")

        # Checking value of attribute for dht.
        brick_server, brick_dir = bricks_list[0].split(':')
        dir_name = brick_dir + dir_name
        g.log.info("Check trusted.glusterfs.dht on host  %s for directory %s",
                   brick_server, dir_name)
        ret = get_fattr(brick_server, dir_name, 'trusted.glusterfs.dht')
        self.assertTrue(ret, ("Failed to get trusted.glusterfs.dht for %s"
                              % dir_name))
        g.log.info("Get trusted.glusterfs.dht xattr for %s successfully",
                   dir_name)
