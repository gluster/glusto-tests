#  Copyright (C) 2015-2020  Red Hat, Inc. <http://www.redhat.com>
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

from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain)
from glustolibs.gluster.heal_ops import (trigger_heal_full,
                                         disable_heal, enable_heal)
from glustolibs.misc.misc_libs import (upload_scripts,
                                       are_nodes_online,
                                       reboot_nodes)
from glustolibs.io.utils import (collect_mounts_arequal,
                                 validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs']])
class TestHealFullNodeReboot(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [cls.script_upload_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volumes
        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Cleanup and umount volume
        """
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

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_heal_full_node_reboot(self):
        """
        - Create IO from mountpoint.
        - Calculate arequal from mount.
        - Delete data from backend from the EC volume.
        - Trigger heal full.
        - Disable Heal.
        - Again Enable and do Heal full.
        - Reboot a Node.
        - Calculate arequal checksum and compare it.
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

            # Create dirs with file
            g.log.info('Creating dirs with file...')
            command = ("/usr/bin/env python %s create_deep_dirs_with_files "
                       "-d 2 -l 2 -n 2 -f 20 %s" % (
                           self.script_upload_path,
                           mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Get arequal before deleting the files from brick
        g.log.info('Getting arequal before getting bricks offline...')
        ret, result_before_killing_procs = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks offline '
                   'is successful')

        subvols = get_subvols(self.mnode, self.volname)['volume_subvols']

        # Delete data from backend from the erasure node
        for subvol in subvols:
            erasure = subvol[-1]
            g.log.info('Clearing ec brick %s', erasure)
            node, brick_path = erasure.split(':')
            ret, _, err = g.run(node, 'cd %s/ ; rm -rf *' % brick_path)
            self.assertFalse(ret, err)
            g.log.info('Clearing ec brick %s is successful', erasure)
        g.log.info('Clearing data from brick is unsuccessful')

        # Trigger heal full
        ret = trigger_heal_full(self.mnode, self.volname)
        self.assertTrue(ret, 'Unable to trigger full heal.')

        # Disable Heal and Enable Heal Full Again
        g.log.info("Disabling Healon the Servers")
        ret = disable_heal(self.mnode, self.volname)
        self.assertTrue(ret, "Disabling Failed")
        g.log.info("Healing is Now Disabled")

        g.log.info("Enbaling Heal Now")
        ret = enable_heal(self.mnode, self.volname)
        self.assertTrue(ret, "Enabling Heal failed")
        g.log.info("Healing is now enabled")
        ret = trigger_heal_full(self.mnode, self.volname)
        self.assertTrue(ret, 'Unable to trigger full heal.')

        # Reboot A Node
        g.log.info("Rebooting Node from the Cluster")
        subvols_dict = get_subvols(self.mnode, self.volname)
        nodes_to_reboot = []
        for subvol in subvols_dict['volume_subvols']:
            # Define nodes to reboot
            brick_list = subvol[1:2]
            for brick in brick_list:
                node, brick_path = brick.split(':')
                if node not in nodes_to_reboot:
                    nodes_to_reboot.append(node)

        # Reboot nodes on subvol and wait while rebooting
        g.log.info("Rebooting the nodes %s", nodes_to_reboot)
        ret = reboot_nodes(nodes_to_reboot)
        self.assertTrue(ret, 'Failed to reboot nodes %s '
                        % nodes_to_reboot)

        # Check if nodes are online
        counter = 0
        timeout = 700
        _rc = False
        while counter < timeout:
            ret, reboot_results = are_nodes_online(nodes_to_reboot)
            if not ret:
                g.log.info("Nodes are offline, Retry after 5 seconds ... ")
                sleep(5)
                counter = counter + 5
            _rc = True
            break

        if not _rc:
            for node in reboot_results:
                if not reboot_results[node]:
                    g.log.error("Node %s is offline even after "
                                "%d minutes", node, timeout / 60.0)
        g.log.info("All nodes %s are up and running", nodes_to_reboot)

        # Trigger Heal Full
        ret = trigger_heal_full(self.mnode, self.volname)
        if not ret:
            sleep(10)
            ret = trigger_heal_full(self.mnode, self.volname)
        self.assertTrue(ret, 'Unable to trigger full heal.')

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')

        # Get arequal after healing
        g.log.info('Getting arequal after getting bricks online...')
        ret, result_after_healing = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after getting bricks online '
                   'is successful')

        # Comparing arequals
        self.assertEqual(result_before_killing_procs, result_after_healing,
                         'Arequals before killing arbiter '
                         'processes and after healing are not equal')
        g.log.info('Arequals before killing arbiter '
                   'processes and after healing are equal')
