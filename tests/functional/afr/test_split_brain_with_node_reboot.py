#  Copyright (C) 2021  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable=too-many-statements, too-many-locals
from unittest import SkipTest
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete)
from glustolibs.io.utils import (run_linux_untar, run_crefi,
                                 wait_for_io_to_complete)


@runs_on([['replicated', 'distributed-replicated'], ['glusterfs']])
class TestSelfHeal(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Check for availability of atleast 3 clients
        if len(cls.clients) < 3:
            raise SkipTest("This test requires atleast 3 clients")

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        # Setup Volume and Mount Volume
        ret = cls.setup_volume_and_mount_volume(cls.mounts, True)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        cls.list_of_io_processes = []
        cls.is_io_running = False

    def tearDown(self):

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

    def test_afr_node_reboot_self_heal(self):
        """
        Steps:
        1. Create *3 replica volume
        2. Mount the volume on 3 clients
        3. Run following workload from clients
        Client 1: Linux Untars
        Client 2: Lookups ls
        Client 3: Lookups du
        4. Create a directory on mount point
        5. Create deep dirs and file in the directory created at step 4
        6. Perform node reboot
        7. Check for heal status
        8. Reboot another node
        9. Check for heal status
        """

        # Create a dir to start untar
        self.linux_untar_dir = "{}/{}".format(self.mounts[0].mountpoint,
                                              "linuxuntar")
        ret = mkdir(self.clients[0], self.linux_untar_dir)
        self.assertTrue(ret, "Failed to create dir linuxuntar for untar")

        # Start linux untar on dir linuxuntar from client 1
        ret = run_linux_untar(self.clients[0], self.mounts[0].mountpoint,
                              dirs=tuple(['linuxuntar']))
        self.list_of_io_processes += ret
        self.is_io_running = True

        # Run lookup operation ls from client 2
        cmd = ("cd {}; for i in `seq 1 1000000`;do du -sh; done"
               .format(self.mounts[1].mountpoint))
        ret = g.run_async(self.mounts[1].client_system, cmd)
        self.list_of_io_processes += [ret]

        # Run lookup operation du from client 3
        cmd = ("cd {}; for i in `seq 1 1000000`;do ls -laRt; done"
               .format(self.mounts[2].mountpoint))
        ret = g.run_async(self.mounts[2].client_system, cmd)
        self.list_of_io_processes += [ret]

        # Create a dir to start crefi tool
        self.linux_untar_dir = "{}/{}".format(self.mounts[3].mountpoint,
                                              "crefi")
        ret = mkdir(self.clients[3], self.linux_untar_dir)
        self.assertTrue(ret, "Failed to create dir for crefi")

        # Create deep dirs and files on mount point from client 4
        list_of_fops = ("create", "rename", "chmod", "chown", "chgrp",
                        "hardlink", "truncate", "setxattr")
        for fops in list_of_fops:
            ret = run_crefi(self.clients[3],
                            self.linux_untar_dir, 10, 3, 3, thread=4,
                            random_size=True, fop=fops, minfs=0,
                            maxfs=102400, multi=True, random_filename=True)
            self.assertTrue(ret, "crefi failed during {}".format(fops))
            g.log.info("crefi PASSED FOR fop %s", fops)
        g.log.info("IOs were successful using crefi")

        for server_num in (1, 2):
            # Perform node reboot for servers
            g.log.info("Rebooting %s", self.servers[server_num])
            ret = g.run_async(self.servers[server_num], "reboot")
            self.assertTrue(ret, 'Failed to reboot node')

            # Monitor heal completion
            ret = monitor_heal_completion(self.mnode, self.volname)
            self.assertTrue(ret, 'Heal has not yet completed')

            # Check if heal is completed
            ret = is_heal_complete(self.mnode, self.volname)
            self.assertTrue(ret, 'Heal is not complete')
            g.log.info('Heal is completed successfully')
