#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           reset_volume_option)
from glustolibs.gluster.volume_libs import expand_volume
from glustolibs.gluster.rebalance_ops import (rebalance_start,
                                              wait_for_fix_layout_to_complete)
from glustolibs.gluster.glusterfile import move_file


@runs_on([['distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'replicated',
           'arbiter', 'distributed-arbiter'],
          ['glusterfs']])
class TestStackOverflow(GlusterBaseClass):
    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError("Failed to Setup and Mount Volume")

    def tearDown(self):
        # Reset the volume options set inside the test
        vol_options = ['performance.parallel-readdir',
                       'performance.readdir-ahead']
        for opt in vol_options:
            ret, _, _ = reset_volume_option(self.mnode, self.volname, opt)
            if ret:
                raise ExecutionError("Failed to reset the volume option %s"
                                     % opt)
        g.log.info("Successfully reset the volume options")

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_stack_overflow(self):
        """
        Description: Tests to check that there is no stack overflow
                     in readdirp with parallel-readdir enabled.
        Steps :
        1) Create a volume.
        2) Mount the volume using FUSE.
        3) Enable performance.parallel-readdir and
           performance.readdir-ahead on the volume.
        4) Create 10000 files on the mount point.
        5) Add-brick to the volume.
        6) Perform fix-layout on the volume (not rebalance).
        7) From client node, rename all the files, this will result in creation
           of linkto files on the newly added brick.
        8) Do ls -l (lookup) on the mount-point.
        """
        # pylint: disable=too-many-statements
        # Enable performance.parallel-readdir and
        # performance.readdir-ahead on the volume
        options = {"performance.parallel-readdir": "enable",
                   "performance.readdir-ahead": "enable"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, "Failed to set volume options")
        g.log.info("Successfully set volume options")

        # Creating 10000 files on volume root
        m_point = self.mounts[0].mountpoint
        command = 'touch ' + m_point + '/file{1..10000}_0'
        ret, _, _ = g.run(self.clients[0], command)
        self.assertEqual(ret, 0, "File creation failed on %s"
                         % m_point)
        g.log.info("Files successfully created on the mount point")

        # Add bricks to the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume %s",
                              self.volname))
        g.log.info("Expanding volume is successful on "
                   "volume %s", self.volname)

        # Perform fix-layout on the volume
        ret, _, _ = rebalance_start(self.mnode, self.volname, fix_layout=True)
        self.assertEqual(ret, 0, 'Failed to start rebalance')
        g.log.info('Rebalance is started')

        # Wait for fix-layout to complete
        ret = wait_for_fix_layout_to_complete(self.mnode, self.volname,
                                              timeout=3000)
        self.assertTrue(ret, ("Fix-layout failed on volume %s",
                              self.volname))
        g.log.info("Fix-layout is successful on "
                   "volume %s", self.volname)

        # Rename all files from client node
        for i in range(1, 10000):
            ret = move_file(self.clients[0],
                            '{}/file{}_0'.format(m_point, i),
                            '{}/file{}_1'.format(m_point, i))
            self.assertTrue(ret, "Failed to rename files")
        g.log.info("Files renamed successfully")

        # Perform lookup from the mount-point
        cmd = "ls -lR " + m_point
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to lookup")
        g.log.info("Lookup successful")
