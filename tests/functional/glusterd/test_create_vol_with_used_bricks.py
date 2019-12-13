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
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

""" Description:
      Create volume using bricks of deleted volume
"""

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.volume_ops import (volume_stop, volume_create,
                                           get_volume_list)
from glustolibs.gluster.volume_libs import (cleanup_volume, setup_volume)
from glustolibs.gluster.mount_ops import mount_volume, umount_volume


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestCreateVolWithUsedBricks(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.counter = 1
        cls.get_super_method(cls, 'setUpClass')()

        # Uploading file_dir script in all client direcotries
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to "
                                 "clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def tearDown(self):
        """
        tearDown for every test
        """
        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume % s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        # Cleaning the deleted volume bricks
        for brick in self.brick_list:
            node, brick_path = brick.split(r':')
            cmd = "rm -rf " + brick_path
            ret, _, _ = g.run(node, cmd)
            if ret:
                raise ExecutionError("Failed to delete the brick "
                                     "dir's of deleted volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_create_vol_used_bricks(self):
        '''
        -> Create distributed-replica Volume
        -> Add 6 bricks to the volume
        -> Mount the volume
        -> Perform some I/O's on mount point
        -> unmount the volume
        -> Stop and delete the volume
        -> Create another volume using bricks of deleted volume
        '''

        # Create and start a volume
        self.volume['name'] = "test_create_vol_with_fresh_bricks"
        self.volname = "test_create_vol_with_fresh_bricks"
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")

        # Forming brick list
        brick_list = form_bricks_list(self.mnode, self.volname, 6,
                                      self.servers, self.all_servers_info)
        # Adding bricks to the volume
        ret, _, _ = add_brick(self.mnode, self.volname, brick_list)
        self.assertEqual(ret, 0, "Failed to add bricks to the volume %s"
                         % self.volname)
        g.log.info("Bricks added successfully to the volume %s", self.volname)

        # Mounting the volume.
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume %s is not mounted") % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)

        # run IOs
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 10 %s" % (sys.version_info.major,
                                             self.script_upload_path,
                                             self.counter,
                                             mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Unmouting the volume.
        ret, _, _ = umount_volume(mclient=self.mounts[0].client_system,
                                  mpoint=self.mounts[0].mountpoint)
        self.assertEqual(ret, 0, ("Volume %s is not unmounted") % self.volname)
        g.log.info("Volume unmounted successfully : %s", self.volname)

        # Getting brick list
        self.brick_list = get_all_bricks(self.mnode, self.volname)
        if not self.brick_list:
            raise ExecutionError("Failed to get the brick list of %s"
                                 % self.volname)

        # Stop volume
        ret, _, _ = volume_stop(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Failed to stop the volume %s"
                         % self.volname)
        g.log.info("Volume %s stopped successfully", self.volname)

        # Delete Volume
        ret, _, _ = g.run(self.mnode, "gluster volume delete %s --mode=script"
                          % self.volname)
        self.assertEqual(ret, 0, "Failed to delete volume %s" % self.volname)
        g.log.info("Volume deleted successfully %s", self.volname)

        # Create another volume by using bricks of deleted volume
        self.volname = "test_create_vol_used_bricks"
        ret, _, err = volume_create(self.mnode, self.volname, brick_list[0:6],
                                    replica_count=3)
        self.assertNotEqual(ret, 0, "Volume creation should fail with used "
                                    "bricks but volume creation success")
        g.log.info("Failed to create volume with used bricks")

        # Checking failed message of volume creation
        msg = ' '.join(['volume create: test_create_vol_used_bricks: failed:',
                        brick_list[0].split(':')[1],
                        'is already part of a volume'])
        self.assertIn(msg, err, "Incorrect error message for volume creation "
                                "with used bricks")
        g.log.info("correct error message for volume creation with "
                   "used bricks")
