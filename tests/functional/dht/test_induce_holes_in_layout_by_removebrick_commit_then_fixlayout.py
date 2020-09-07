#  Copyright (C) 2018 Red Hat, Inc. http://www.redhat.com>
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

from glustolibs.gluster.brick_ops import remove_brick
from glustolibs.gluster.constants import \
    TEST_LAYOUT_IS_COMPLETE as LAYOUT_IS_COMPLETE
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, wait_for_fix_layout_to_complete)
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status, form_bricks_list_to_remove_brick)
from glustolibs.gluster.dht_test_utils import is_layout_complete


@runs_on([['distributed', 'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs']])
class RebalanceValidation(GlusterBaseClass):
    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        if self.volume_type == "distributed-replicated":
            self.volume_configs = []
            # Redefine distributed-replicated volume
            self.volume['voltype'] = {
                'type': 'distributed-replicated',
                'replica_count': 3,
                'dist_count': 4,
                'transport': 'tcp'}

        if self.volume_type == "distributed-dispersed":
            self.volume_configs = []
            # Redefine distributed-dispersed volume
            self.volume['voltype'] = {
                'type': 'distributed-dispersed',
                'dist_count': 3,
                'disperse_count': 6,
                'redundancy_count': 2,
                'transport': 'tcp'
                }

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        # Form bricks list for Shrinking volume
        self.remove_brick_list = form_bricks_list_to_remove_brick(self.mnode,
                                                                  self.volname,
                                                                  subvol_num=1)
        if not self.remove_brick_list:
            g.log.error("Volume %s: Failed to form bricks list "
                        "for volume shrink", self.volname)
            raise ExecutionError("Volume %s: Failed to form bricks list "
                                 "for volume shrink" % self.volname)
        g.log.info("Volume %s: Formed bricks list for volume shrink",
                   self.volname)

    def test_induce_holes_thenfixlayout(self):

        # pylint: disable=too-many-statements
        m_point = self.mounts[0].mountpoint
        command = 'mkdir -p ' + m_point + '/testdir'
        ret, _, _ = g.run(self.clients[0], command)
        self.assertEqual(ret, 0, "mkdir failed")
        g.log.info("mkdir is successful")

        # DHT Layout validation
        g.log.debug("Verifying hash layout values %s:%s",
                    self.clients[0], self.mounts[0].mountpoint)
        ret = validate_files_in_dir(self.clients[0], self.mounts[0].mountpoint,
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "LAYOUT_IS_COMPLETE: FAILED")
        g.log.info("LAYOUT_IS_COMPLETE: PASS")

        # Log Volume Info and Status before shrinking the volume.
        g.log.info("Logging volume info and Status before shrinking volume")
        log_volume_info_and_status(self.mnode, self.volname)

        # Shrinking volume by removing bricks
        g.log.info("Start removing bricks from volume")
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 self.remove_brick_list, "force")
        self.assertFalse(ret, "Remove-brick with force: FAIL")
        g.log.info("Remove-brick with force: PASS")

        # Check the layout
        ret = is_layout_complete(self.mnode, self.volname, dirpath='/testdir')
        self.assertFalse(ret, "Volume %s: Layout is complete")
        g.log.info("Volume %s: Layout has some holes")

        # Start Rebalance fix-layout
        g.log.info("Volume %s: Start fix-layout", self.volname)
        ret, _, _ = rebalance_start(self.mnode, self.volname, fix_layout=True)
        self.assertEqual(ret, 0, ("Volume %s: fix-layout start failed"
                                  "%s", self.volname))
        g.log.info("Volume %s: fix-layout start success", self.volname)

        # Wait for fix-layout to complete
        g.log.info("Waiting for fix-layout to complete")
        ret = wait_for_fix_layout_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s: Fix-layout is either failed or "
                              "in-progress", self.volname))
        g.log.info("Volume %s: Fix-layout completed successfully",
                   self.volname)

        # DHT Layout validation
        g.log.debug("Verifying hash layout values %s:%s",
                    self.clients[0], self.mounts[0].mountpoint)
        ret = validate_files_in_dir(self.clients[0], self.mounts[0].mountpoint,
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "LAYOUT_IS_COMPLETE: FAILED")
        g.log.info("LAYOUT_IS_COMPLETE: PASS")

    def tearDown(self):

        # Cleaning the removed bricks
        for brick in self.remove_brick_list:
            brick_node, brick_path = brick.split(":")
            cmd = "rm -rf " + brick_path
            ret, _, _ = g.run(brick_node, cmd)
            if ret:
                raise ExecutionError("Failed to delete removed brick dir "
                                     "%s:%s" % (brick_node, brick_path))
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")
