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

"""

Description:
    Renaming of directories and files while rebalance is running
"""

from unittest import skip
from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.rebalance_ops import (get_rebalance_status,
                                              rebalance_start,
                                              wait_for_rebalance_to_complete)
from glustolibs.gluster.volume_libs import (
    expand_volume,
    log_volume_info_and_status)
from glustolibs.io.utils import (
    collect_mounts_arequal,
    wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['arbiter', 'distributed-arbiter', 'dispersed', 'replicated',
           'distributed-dispersed', 'distributed-replicated', 'distributed'],
          ['glusterfs']])
class TestRenameDuringRebalance(GlusterBaseClass):
    """Renaming Files during rebalance"""

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()
        self.all_mounts_procs = []

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and mount it")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup Volume and Mount it")

        # Upload io script for running IO on mounts
        self.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                   "file_dir_ops.py")
        ret = upload_scripts(self.mounts[0].client_system,
                             self.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    @skip('Skipping due to Bug 1755834')
    def test_rename_file_rebalance(self):
        """
        Test file renames during rebalance
        - Create a volume
        - Create directories or files
        - Calculate the checksum using arequal
        - Add brick and start rebalance
        - While rebalance is running, rename files or directories.
        - After rebalancing calculate checksum.
        """
        # Taking the instance of mount point.
        mount_point = self.mounts[0].mountpoint

        # Creating main directory.
        ret = mkdir(self.mounts[0].client_system,
                    "{}/main".format(mount_point))
        self.assertTrue(ret, "mkdir of dir main failed")

        # Creating Files.
        self.all_mounts_procs = []
        command = ("/usr/bin/env python {} create_files"
                   " {}/main/ -f 4000"
                   " --fixed-file-size 1k".format(self.script_upload_path,
                                                  mount_point))
        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)
        g.log.info("IO on %s:%s is started successfully",
                   self.mounts[0].client_system, mount_point)

        # Wait for IO completion.
        self.assertTrue(wait_for_io_to_complete(self.all_mounts_procs,
                                                self.mounts[0]),
                        "IO failed on some of the clients")
        g.log.info("IO completed on the clients")

        # Getting the arequal checksum.
        arequal_checksum_before_rebalance = collect_mounts_arequal(self.mounts)

        # Log Volume Info and Status before expanding the volume.
        log_volume_info_and_status(self.mnode, self.volname)

        # Expanding volume by adding bricks to the volume
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume %s", self.volname))
        g.log.info("Expanding volume is successful on "
                   "volume %s", self.volname)

        # Log Volume Info and Status after expanding the volume.
        log_volume_info_and_status(self.mnode, self.volname)

        # Start Rebalance
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Check that rebalance status is "in progress"
        rebalance_status = get_rebalance_status(self.mnode, self.volname)
        ret = rebalance_status['aggregate']['statusStr']
        self.assertEqual(ret, "in progress", ("Rebalance is not in "
                                              "'in progress' state, either "
                                              "rebalance is in completed state"
                                              " or failed to get rebalance "
                                              " status"))
        g.log.info("Rebalance is in 'in progress' state")

        # Renaming the files during rebalance.
        self.all_mounts_procs = []
        command = ("/usr/bin/env python {} mv"
                   " {}/main/ --postfix re ".format(
                       self.script_upload_path,
                       mount_point))
        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        g.log.info("IO on %s:%s is started successfully",
                   self.mounts[0].client_system, mount_point)
        self.all_mounts_procs.append(proc)

        # Wait for rebalance to complete
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname)
        self.assertTrue(ret, ("Rebalace is not yet complete on the volume "
                              "%s", self.volname))
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        # Wait for IO completion.
        self.assertTrue(wait_for_io_to_complete(self.all_mounts_procs,
                                                self.mounts[0]),
                        "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Getting arequal checksum after rebalance
        arequal_checksum_after_rebalance = collect_mounts_arequal(self.mounts)

        # Comparing arequals checksum before and after rebalance.
        self.assertEqual(arequal_checksum_before_rebalance,
                         arequal_checksum_after_rebalance,
                         "arequal checksum is NOT MATCHING")
        g.log.info("arequal checksum is SAME")

    def tearDown(self):
        """tear Down Callback"""
        # Unmount Volume and Cleanup volume.
        g.log.info("Starting to Umount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
