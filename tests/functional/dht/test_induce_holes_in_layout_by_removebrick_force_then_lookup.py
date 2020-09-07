#  Copyright (C) 2018-2020 Red Hat, Inc. http://www.redhat.com>
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
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status, form_bricks_list_to_remove_brick)
from glustolibs.gluster.dht_test_utils import is_layout_complete
from glustolibs.gluster.mount_ops import mount_volume


@runs_on([['distributed', 'distributed-replicated', 'distributed-arbiter',
           'distributed-dispersed'],
          ['glusterfs']])
class RebalanceValidation(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Check for the default dist_count value and override it if required
        if cls.default_volume_type_config['distributed']['dist_count'] <= 2:
            cls.default_volume_type_config['distributed']['dist_count'] = 4
        if (cls.default_volume_type_config['distributed-replicated']
                ['dist_count']) <= 2:
            (cls.default_volume_type_config['distributed-replicated']
             ['dist_count']) = 4
        if (cls.default_volume_type_config['distributed-dispersed']
                ['dist_count']) <= 2:
            (cls.default_volume_type_config['distributed-dispersed']
             ['dist_count']) = 4
        if (cls.default_volume_type_config['distributed-arbiter']
                ['dist_count']) <= 2:
            (cls.default_volume_type_config['distributed-arbiter']
             ['dist_count']) = 4

        # Setup Volume and Mount Volume

        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def test_induce_holes_then_lookup(self):

        """
        Test Script to induce holes in layout by using remove-brick force
        and then performing lookup in order to fix the layout.

        Steps :
        1) Create a volume and mount it using FUSE.
        2) Create a directory "testdir" on mount point.
        3) Check if the layout is complete.
        4) Log volume info and status before remove-brick operation.
        5) Form a list of bricks to be removed.
        6) Start remove-brick operation using 'force'.
        7) Let remove-brick complete and check layout.
        8) Mount the volume on a new mount.
        9) Send a lookup on mount point.
        10) Check if the layout is complete.

        """
        # pylint: disable=too-many-statements
        # Create a directory on mount point
        m_point = self.mounts[0].mountpoint
        dirpath = '/testdir'
        command = 'mkdir -p ' + m_point + dirpath
        ret, _, _ = g.run(self.clients[0], command)
        self.assertEqual(ret, 0, "mkdir failed")
        g.log.info("mkdir is successful")

        # DHT Layout validation
        g.log.debug("Verifying hash layout values %s:%s",
                    self.clients[0], m_point)
        ret = validate_files_in_dir(self.clients[0], m_point,
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "LAYOUT_IS_COMPLETE: FAILED")
        g.log.info("LAYOUT_IS_COMPLETE: PASS")

        # Log Volume Info and Status before shrinking the volume.
        g.log.info("Logging volume info and Status before shrinking volume")
        log_volume_info_and_status(self.mnode, self.volname)

        # Form bricks list for Shrinking volume
        self.remove_brick_list = form_bricks_list_to_remove_brick(self.mnode,
                                                                  self.volname,
                                                                  subvol_num=1)
        self.assertNotEqual(self.remove_brick_list, None,
                            ("Volume %s: Failed to form bricks list for volume"
                             " shrink", self.volname))
        g.log.info("Volume %s: Formed bricks list for volume shrink",
                   self.volname)

        # Shrinking volume by removing bricks
        g.log.info("Start removing bricks from volume")
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 self.remove_brick_list, "force")
        self.assertFalse(ret, "Remove-brick with force: FAIL")
        g.log.info("Remove-brick with force: PASS")

        # Check the layout
        ret = is_layout_complete(self.mnode, self.volname, dirpath)
        self.assertFalse(ret, ("Volume %s: Layout is complete", self.volname))
        g.log.info("Volume %s: Layout has some holes", self.volname)

        # Mount the volume on a new mount point
        ret, _, _ = mount_volume(self.volname, mtype='glusterfs',
                                 mpoint=m_point,
                                 mserver=self.mnode,
                                 mclient=self.clients[1])
        self.assertEqual(ret, 0, ("Failed to do gluster mount of volume %s"
                                  " on client node %s",
                                  self.volname, self.clients[1]))
        g.log.info("Volume %s mounted successfullly on %s", self.volname,
                   self.clients[1])

        # Send a look up on the directory
        cmd = 'ls %s%s' % (m_point, dirpath)
        ret, _, err = g.run(self.clients[1], cmd)
        self.assertEqual(ret, 0, ("Lookup failed on %s with error %s",
                                  (dirpath, err)))
        g.log.info("Lookup sent successfully on %s", m_point + dirpath)

        # DHT Layout validation
        g.log.info("Checking layout after new mount")
        g.log.debug("Verifying hash layout values %s:%s",
                    self.clients[1], m_point + dirpath)
        ret = validate_files_in_dir(self.clients[1], m_point + dirpath,
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

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
