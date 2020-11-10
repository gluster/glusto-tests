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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from random import choice
from unittest import skip, SkipTest

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.volume_libs import expand_volume, shrink_volume
from glustolibs.gluster.brickmux_ops import enable_brick_mux, disable_brick_mux
from glustolibs.misc.misc_libs import upload_scripts, kill_process
from glustolibs.io.utils import (run_linux_untar, validate_io_procs,
                                 wait_for_io_to_complete)


@runs_on([['distributed-replicated', 'distributed-dispersed'], ['glusterfs']])
class TestAddBrickRemoveBrickWithlookupsAndKernaluntar(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Check for availability of atleast 4 clients
        if len(cls.clients) < 4:
            raise SkipTest("This test requires atleast 4 clients")

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        # Enable brickmux on cluster
        if not enable_brick_mux(self.mnode):
            raise ExecutionError("Failed to enable brickmux on cluster")

        # Changing dist_count to 3
        self.volume['voltype']['dist_count'] = 3

        # Creating Volume and mounting the volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Volume creation or mount failed: %s"
                                 % self.volname)

        self.list_of_io_processes = []
        self.is_io_running = False

    def tearDown(self):

        # Disable brickmux on cluster
        if not disable_brick_mux(self.mnode):
            raise ExecutionError("Failed to disable brickmux on cluster")

        # If I/O processes are running wait from them to complete
        if self.is_io_running:
            if not wait_for_io_to_complete(self.list_of_io_processes,
                                           self.mounts):
                raise ExecutionError("Failed to wait for I/O to complete")

        # Unmounting and cleaning volume
        ret = self.unmount_volume_and_cleanup_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Unable to delete volume %s" % self.volname)

        self.get_super_method(self, 'tearDown')()

    @skip('Skipping due to Bug 1571317')
    def test_add_brick_remove_brick_with_lookups_and_kernal_untar(self):
        """
        Test case:
        1. Enable brickmux on cluster, create a volume, start it and mount it.
        2. Start the below I/O from 4 clients:
           From client-1 : run script to create folders and files continuously
           From client-2 : start linux kernel untar
           From client-3 : while true;do find;done
           From client-4 : while true;do ls -lRt;done
        3. Kill brick process on one of the nodes.
        4. Add brick to the volume.
        5. Remove bricks from the volume.
        6. Validate if I/O was successful or not.
        """
        # Fill few bricks till it is full
        bricks = get_all_bricks(self.mnode, self.volname)

        # Create a dir to start untar
        self.linux_untar_dir = "{}/{}".format(self.mounts[0].mountpoint,
                                              "linuxuntar")
        ret = mkdir(self.clients[0], self.linux_untar_dir)
        self.assertTrue(ret, "Failed to create dir linuxuntar for untar")

        # Start linux untar on dir linuxuntar
        ret = run_linux_untar(self.clients[0], self.mounts[0].mountpoint,
                              dirs=tuple(['linuxuntar']))
        self.list_of_io_processes += ret
        self.is_io_running = True

        # Run script to create folders and files continuously
        cmd = ("/usr/bin/env python {} create_deep_dirs_with_files "
               "--dirname-start-num 758 --dir-depth 2 "
               "--dir-length 100 --max-num-of-dirs 10 --num-of-files 105 {}"
               .format(self.script_upload_path, self.mounts[1].mountpoint))
        ret = g.run_async(self.mounts[1].client_system, cmd)
        self.list_of_io_processes += [ret]

        # Run lookup operations from 2 clients
        cmd = ("cd {}; for i in `seq 1 1000000`;do find .; done"
               .format(self.mounts[2].mountpoint))
        ret = g.run_async(self.mounts[2].client_system, cmd)
        self.list_of_io_processes += [ret]

        cmd = ("cd {}; for i in `seq 1 1000000`;do ls -lRt; done"
               .format(self.mounts[3].mountpoint))
        ret = g.run_async(self.mounts[3].client_system, cmd)
        self.list_of_io_processes += [ret]

        # Kill brick process of one of the nodes.
        brick = choice(bricks)
        node, _ = brick.split(":")
        ret = kill_process(node, process_names="glusterfsd")
        self.assertTrue(ret, "Failed to kill brick process of brick %s"
                        % brick)

        # Add brick to volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, "Failed to add brick on volume %s"
                        % self.volname)
        g.log.info("Add brick to volume successful")

        # Remove bricks from the volume
        ret = shrink_volume(self.mnode, self.volname, rebalance_timeout=2400)
        self.assertTrue(ret, "Failed to remove-brick from volume")
        g.log.info("Remove-brick rebalance successful")

        # Validate if I/O was successful or not.
        ret = validate_io_procs(self.list_of_io_processes, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.is_io_running = False
