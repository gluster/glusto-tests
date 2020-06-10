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

from random import choice
import os

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.mount_ops import create_mount_objs
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs', 'nfs']])
class TestEcLookupAndMoveOperations(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        # pylint: disable=unsubscriptable-object
        cls.get_super_method(cls, 'setUpClass')()

        # As the test requires three clients using one of the
        # server as third client and choosing it randomly
        cls.third_client = choice(cls.servers[1:])
        cls.clients.extend([cls.third_client])
        newmount = {
            'protocol': cls.mount_type,
            'server': cls.mnode,
            'volname': cls.volume['name'],
            'client': {'host': cls.third_client},
            'mountpoint': (os.path.join(
                "/mnt", '_'.join([cls.volume['name'],
                                  cls.mount_type]))),
            'options': '',
        }
        new_mount = create_mount_objs([newmount])
        cls.mounts.extend(new_mount)

        # Upload IO scripts for running IO on mounts
        cls.script_upload_path = (
            "/usr/share/glustolibs/io/scripts/file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s" %
                                 cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup volume and mount it on three clients.
        if not self.setup_volume_and_mount_volume(self.mounts):
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

        # Unmount and cleanup the volume
        if not self.unmount_volume_and_cleanup_volume(self.mounts):
            raise ExecutionError("Unable to unmount and cleanup volume")
        g.log.info("Unmount and volume cleanup is successful")

    def _run_create_files(self, file_count, base_name, mpoint, client):
        """Run create files using file_dir_op.py"""
        cmd = ("/usr/bin/env python {} create_files -f {} --fixed-file-size"
               " 1k --base-file-name {} {}".format(self.script_upload_path,
                                                   file_count, base_name,
                                                   mpoint))
        proc = g.run_async(client, cmd)
        self.mount_procs.append(proc)

    def test_ec_lookup_and_move_operations_all_bricks_online(self):
        """
        Test Steps:
        1. Create volume and mount the volume on 3 clients, c1(client1),
           c2(client2), and, c3(client3)
        2. On c1, mkdir /c1/dir
        3. On c2, Create 4000 files on mount point i.e. "/"
        4. After step 3, Create next 4000 files on c2 on mount point i.e. "/"
        5. On c1 Create 10000 files on /dir/
        6. On c3 start moving 4000 files created on step 3 from mount point
            to /dir/
        7. On c3, start ls in a loop for 20 iterations
        """
        # Create directory on client1
        dir_on_mount = self.mounts[0].mountpoint + '/dir'
        ret = mkdir(self.mounts[0].client_system, dir_on_mount)
        self.assertTrue(ret, "unable to create directory on client"
                             "1 {}".format(self.mounts[0].client_system))
        g.log.info("Directory created on %s successfully",
                   self.mounts[0].client_system)

        # Create 4000 files on the mountpoint of client2
        cmd = ("/usr/bin/env python {} create_files -f 4000"
               " --fixed-file-size 10k --base-file-name file_from_client2_"
               " {}".format(self.script_upload_path,
                            self.mounts[1].mountpoint))
        ret, _, err = g.run(self.mounts[1].client_system, cmd)
        self.assertEqual(ret, 0, "File creation on {} failed with {}".
                         format(self.mounts[1].client_system, err))
        g.log.info("File creation successful on %s",
                   self.mounts[1].client_system)

        # Next IO to be ran in the background so using mount_procs list
        self.mount_procs = []
        # Create next 4000 files on the mountpoint of client2
        self._run_create_files(file_count=4000,
                               base_name="files_on_client2_background_",
                               mpoint=self.mounts[1].mountpoint,
                               client=self.mounts[1].client_system)

        # Create 10000 files from client 1 on dir1
        self._run_create_files(file_count=10000,
                               base_name="files_on_client1_background_",
                               mpoint=dir_on_mount,
                               client=self.mounts[0].client_system)

        # Move the files created on client2 to dir from client3
        cmd = ("for i in `seq 0 3999`; do mv {}/file_from_client2_$i.txt {}; "
               "done".format(self.mounts[2].mountpoint, dir_on_mount))
        proc = g.run_async(self.mounts[2].client_system, cmd)
        self.mount_procs.append(proc)

        # Perform a lookup in loop from client3 for 20 iterations
        cmd = ("ls -R {}".format(self.mounts[2].mountpoint))
        counter = 20
        while counter > 0:
            ret, _, err = g.run(self.mounts[2].client_system, cmd)
            self.assertEqual(ret, 0, "ls while mv operation being carried"
                                     " failed with {}".format(err))
            g.log.debug("ls successful for the %s time", counter)
            counter -= 1

        self.assertTrue(validate_io_procs(self.mount_procs, self.mounts),
                        "IO failed on the clients")
