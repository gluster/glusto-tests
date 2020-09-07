#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

import time

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import log_volume_info_and_status
from glustolibs.gluster.brick_libs import (
    bring_bricks_offline, bring_bricks_online,
    are_bricks_offline, get_all_bricks,
    are_bricks_online)
from glustolibs.gluster.heal_libs import (
    monitor_heal_completion, are_all_self_heal_daemons_are_online)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs']])
class ListMount(GlusterBaseClass):
    """
    Tetstcase involves killing brick in cyclic order and
    listing the directories after healing from mount point
    """
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "fd_writes.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        cls.counter = 1
        # int: Value of counter is used for dirname-start-num argument for
        # file_dir_ops.py create_deep_dirs_with_files.

        # The --dir-length argument value for file_dir_ops.py
        # create_deep_dirs_with_files is set to 10 (refer to the cmd in setUp
        # method). This means every mount will create
        # 10 top level dirs. For every mountpoint/testcase to create new set of
        # dirs, we are incrementing the counter by --dir-length value i.e 10
        # in this test suite.

        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Unmount Volume and Cleanup Volume
        """
        # Wait for IO to complete if io validation is not executed in the
        # test method
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

            # List all files and dirs created
            g.log.info("List all files and directories:")
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_files_on_mount(self):
        """""
        Description:-
        - I/O on the mounts
        - kill brick in cyclic order
        - list the files after healing
        """""

        # IO on the mount point
        # Each client will write 2 files each of 1 GB and keep
        # modifying the same file
        g.log.info("Starting IO on all mounts...")
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s "
                   "--file-sizes-list 1G "
                   "--chunk-sizes-list 128 "
                   "--write-time 900 "
                   "--num-of-files 2 "
                   "--base-file-name test_brick_down_from_client_%s.txt "
                   "--dir %s " % (self.script_upload_path,
                                  mount_obj.client_system,
                                  mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10
        self.io_validation_complete = False

        # Killing bricks in cyclic order
        bricks_list = get_all_bricks(self.mnode, self.volname)

        # Total number of cyclic brick-down cycles to be executed
        number_of_cycles = 0
        while number_of_cycles < 3:
            number_of_cycles += 1
            for brick in bricks_list:
                # Bring brick offline
                g.log.info('Bringing bricks %s offline', brick)
                ret = bring_bricks_offline(self.volname, [brick])
                self.assertTrue(ret, ("Failed to bring bricks %s offline"
                                      % brick))

                ret = are_bricks_offline(self.mnode, self.volname, [brick])
                self.assertTrue(ret, 'Bricks %s are not offline' % brick)
                g.log.info('Bringing bricks %s offline is successful', brick)

                # Introducing 30 second sleep when brick is down
                g.log.info("Waiting for 30 seconds, with ongoing IO while "
                           "brick %s is offline", brick)
                ret = time.sleep(30)

                # Bring brick online
                g.log.info('Bringing bricks %s online', brick)
                ret = bring_bricks_online(self.mnode, self.volname, [brick])
                self.assertTrue(ret, ("Failed to bring bricks %s online "
                                      % brick))
                g.log.info('Bricks %s are online', brick)

                # Check if bricks are online
                ret = are_bricks_online(self.mnode, self.volname, bricks_list)
                self.assertTrue(ret, 'Bricks %s are not online' % bricks_list)
                g.log.info('Bricks %s are online', bricks_list)

                # Check daemons
                g.log.info('Checking daemons...')
                ret = are_all_self_heal_daemons_are_online(self.mnode,
                                                           self.volname)
                self.assertTrue(ret, ("Some of the self-heal Daemons are "
                                      "offline"))
                g.log.info('All self-heal Daemons are online')

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Checking volume status
        g.log.info("Logging volume info and Status after bringing bricks "
                   "offline from the volume %s", self.volname)
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s" % self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Monitoring heals on the volume
        g.log.info("Wait for self-heal to completeon the volume")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, ("Self heal didn't complete even after waiting "
                              "for 20 minutes."))
        g.log.info("self-heal is successful after changing the volume type "
                   "from replicated to arbitered volume")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")
