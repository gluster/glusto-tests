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

from random import choice, sample
import os

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           are_bricks_offline,
                                           are_bricks_online)
from glustolibs.gluster.heal_libs import monitor_heal_completion
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.mount_ops import create_mount_objs
from glustolibs.gluster.volume_ops import volume_start
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs, wait_for_io_to_complete)


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

        if self.mount_procs:
            ret = wait_for_io_to_complete(self.mount_procs, self.mounts)
            if ret:
                raise ExecutionError(
                    "Wait for IO completion failed on some of the clients")

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

    def test_ec_lookup_and_move_operations_few_bricks_are_offline(self):
        """
        Test Steps:
        1. Mount this volume on 3 mount point, c1, c2, and c3
        2. Bring down two bricks offline in each subvol.
        3. On client1: under dir1 create files f{1..10000} run in background
        4. On client2: under root dir of mountpoint touch x{1..1000}
        5. On client3: after step 4 action completed, start creating
           x{1001..10000}
        6. Bring bricks online which were offline(brought up all the bricks
           which were down (2 in each of the two subvols)
        7. While IO on Client1 and Client3 were happening, On client2 move all
           the x* files into dir1
        8. Perform lookup from client 3
        """
        # List two bricks in each subvol
        all_subvols_dict = get_subvols(self.mnode, self.volname)
        subvols = all_subvols_dict['volume_subvols']
        bricks_to_bring_offline = []
        for subvol in subvols:
            self.assertTrue(subvol, "List is empty")
            bricks_to_bring_offline.extend(sample(subvol, 2))

        # Bring two bricks of each subvol offline
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, "Bricks are still online")
        g.log.info("Bricks are offline %s", bricks_to_bring_offline)

        # Validating the bricks are offline or not
        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, "Few of the bricks are still online in"
                             " {} in".format(bricks_to_bring_offline))
        g.log.info("%s bricks are offline as expected",
                   bricks_to_bring_offline)

        # Create directory on client1
        dir_on_mount = self.mounts[0].mountpoint + '/dir1'
        ret = mkdir(self.mounts[0].client_system, dir_on_mount)
        self.assertTrue(ret, "unable to create directory on client"
                             " 1 {}".format(self.mounts[0].client_system))
        g.log.info("Dir1 created on %s successfully",
                   self.mounts[0].client_system)

        # Next IO to be ran in the background so using mount_procs
        # and run_async.
        self.mount_procs = []

        # On client1: under dir1 create files f{1..10000} run in background
        self._run_create_files(file_count=10000, base_name="f_",
                               mpoint=dir_on_mount,
                               client=self.mounts[0].client_system)

        # On client2: under root dir of the mountpoint touch x{1..1000}
        cmd = ("/usr/bin/env python {} create_files -f 1000 --fixed-file-size"
               " 10k --base-file-name x {}".format(self.script_upload_path,
                                                   self.mounts[1].mountpoint))
        ret, _, err = g.run(self.mounts[1].client_system, cmd)
        self.assertEqual(ret, 0, "File creation failed on {} with {}".
                         format(self.mounts[1].client_system, err))
        g.log.info("File creation successful on %s",
                   self.mounts[1].client_system)

        # On client3: start creating x{1001..10000}
        cmd = ("cd {}; for i in `seq 1000 10000`; do touch x$i; done; "
               "cd -".format(self.mounts[2].mountpoint))
        proc = g.run_async(self.mounts[2].client_system, cmd)
        self.mount_procs.append(proc)

        # Bring bricks online with volume start force
        ret, _, err = volume_start(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, err)
        g.log.info("Volume: %s started successfully", self.volname)

        # Check whether bricks are online or not
        ret = are_bricks_online(self.mnode, self.volname,
                                bricks_to_bring_offline)
        self.assertTrue(ret, "Bricks {} are still offline".
                        format(bricks_to_bring_offline))
        g.log.info("Bricks %s are online now", bricks_to_bring_offline)

        # From client2 move all the files with name starting with x into dir1
        cmd = ("for i in `seq 0 999`; do mv {}/x$i.txt {}; "
               "done".format(self.mounts[1].mountpoint, dir_on_mount))
        proc = g.run_async(self.mounts[1].client_system, cmd)
        self.mount_procs.append(proc)

        # Perform a lookup in loop from client3 for 20 iterations
        cmd = ("ls -R {}".format(self.mounts[2].mountpoint))
        counter = 20
        while counter:
            ret, _, err = g.run(self.mounts[2].client_system, cmd)
            self.assertEqual(ret, 0, "ls while mv operation being carried"
                                     " failed with {}".format(err))
            g.log.debug("ls successful for the %s time", 21-counter)
            counter -= 1

        self.assertTrue(validate_io_procs(self.mount_procs, self.mounts),
                        "IO failed on the clients")
        # Emptying mount_procs for not validating IO in tearDown
        self.mount_procs *= 0

        # Wait for heal to complete
        ret = monitor_heal_completion(self.mnode, self.volname,)
        self.assertTrue(ret, "Heal didn't completed in the expected time")
        g.log.info("Heal completed successfully on %s volume", self.volname)
