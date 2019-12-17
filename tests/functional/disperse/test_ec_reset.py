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

"""
Test Description:
    Tests brick reset on a EC volume.
    For brick reset we can start it to kill a brick with source defined
    or commit to reset a brick with source and destination defined
"""
from os import getcwd
from random import choice
import sys
from time import sleep

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.heal_libs import (monitor_heal_completion)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal,
                                 list_all_files_and_dirs_mounts,
                                 validate_io_procs)
from glustolibs.gluster.brick_libs import (
    get_all_bricks, bring_bricks_online,
    bring_bricks_offline, get_offline_bricks_list,
    wait_for_bricks_to_be_online, are_bricks_offline,
    are_bricks_online)
from glustolibs.gluster.brick_ops import (reset_brick)


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestBrickReset(GlusterBaseClass):

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
        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_brickreset_ec_volume(self):
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals

        """
        - Start resource consumption tool
        - Create IO on dir2 of volume mountpoint
        - Reset brick start
        - Check if brick is offline
        - Reset brick with destination same as source with force running IO's
        - Validating IO's and waiting for it to complete on dir2
        - Remove dir2
        - Create 5 directory and 5 files in dir of mountpoint
        - Rename all files inside dir1 at mountpoint
        - Create softlink and hardlink of files in dir1 of mountpoint
        - Delete op for deleting all file in one of the dirs inside dir1
        - Change chmod, chown, chgrp
        - Create tiny, small, medium and large file
        - Create IO's
        - Validating IO's and waiting for it to complete
        - Calculate arequal before kiiling brick
        - Get brick from Volume
        - Reset brick
        - Check if brick is offline
        - Reset brick by giving a different source and dst node
        - Reset brick by giving dst and source same without force
        - Obtain hostname
        - Reset brick with dst-source same force using hostname - Successful
        - Monitor heal completion
        - Bring down other bricks to max redundancy
        - Get arequal after bringing down bricks
        - Bring bricks online
        - Reset brick by giving a same source and dst brick
        - Kill brick manually
        - Check if brick is offline
        - Reset brick by giving a same source and dst brick
        - Wait for brick to come online
        - Bring down other bricks to max redundancy
        - Get arequal after bringing down bricks
        - Bring bricks online
        - Remove brick from backend
        - Check if brick is offline
        - Reset brick by giving dst and source same without force - Successful
        - Monitor heal completion
        - Compare the arequal's calculated
        """
        # Starting resource consumption using top
        log_file_mem_monitor = getcwd() + '/mem_usage.log'
        cmd = 'for i in {1..100};do top -n 1 -b|egrep \
                "RES|gluster" & free -h 2>&1 >> ' + \
            log_file_mem_monitor + ' ;sleep 10;done'
        g.log.info(cmd)
        for mount_obj in self.mounts:
            g.run_async(mount_obj.client_system, cmd)
        bricks_list = []

        # Get the bricks from the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # Creating directory2
        cmd = ('mkdir %s/dir2' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create directory2")
        g.log.info("Directory 2 on %s created successfully", self.mounts[0])

        # Creating files on client side for dir2
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

            # Create dirs with file
            g.log.info('Creating dirs with file...')
            command = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                       "-d 2 -l 2 -n 2 -f 20 %s/dir2" % (
                           sys.version_info.major, self.script_upload_path,
                           mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Reset a brick
        g.log.info('Reset of brick using start')
        brick_reset = choice(bricks_list)
        ret, _, _ = reset_brick(self.mnode, self.volname, brick_reset,
                                "start")

        # Check if the brick is offline
        g.log.info("Check the brick status if it is offline")
        offline_bricks = get_offline_bricks_list(self.mnode, self.volname)
        self.assertEqual(offline_bricks[0], brick_reset, "Brick not offline")
        g.log.info("Expected : Brick is offline")

        # Reset brick with dest same as source with force while running IO's
        g.log.info('Reset of brick with same src and dst brick')
        ret, _, _ = reset_brick(self.mnode, self.volname, brick_reset,
                                "commit", brick_reset, force="true")
        self.assertEqual(ret, 0, "Not Expected: Reset brick failed")
        g.log.info("Expected : Reset brick is successful")

        # Validating IO's and waiting to complete
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
            )
        self.io_validation_complete = True

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

        # Deleting dir2
        cmd = ('rm -rf %s/dir2' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to delete directory2")
        g.log.info("Directory 2 deleted successfully for %s", self.mounts[0])

        del self.all_mounts_procs[:]

        # Creating dir1
        cmd = ('mkdir  %s/dir1' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create directory1")
        g.log.info("Directory 1 created successfully for %s", self.mounts[0])

        # Create 5 dir and 5 files in each dir at mountpoint on dir1
        start, end = 1, 5
        for mount_obj in self.mounts:
            # Number of dir and files to be created.
            dir_range = str(start) + ".." + str(end)
            file_range = str(start) + ".." + str(end)
            # Create dir 1-5 at mountpoint.
            cmd = ('mkdir %s/dir1/dir{%s};' %
                   (mount_obj.mountpoint, dir_range))
            g.run(mount_obj.client_system, cmd)

            # Create files inside each dir.
            cmd = ('touch %s/dir1/dir{%s}/file{%s};'
                   % (mount_obj.mountpoint, dir_range, file_range))
            g.run(mount_obj.client_system, cmd)

            # Increment counter so that at next client dir and files are made
            # with diff offset. Like at next client dir will be named
            # dir6, dir7...dir10. Same with files.
            start += 5
            end += 5

        # Rename all files inside dir1 at mountpoint on dir1
        clients = []
        for mount_obj in self.mounts:
            clients.append(mount_obj.client_system)
            cmd = ('cd %s/dir1/dir1/; '
                   'for FILENAME in *;'
                   'do mv $FILENAME Unix_$FILENAME; '
                   'done;'
                   % mount_obj.mountpoint)
            g.run_parallel(clients, cmd)

        # Truncate at any dir in mountpoint inside dir1
        # start is an offset to be added to dirname to act on
        # diff files at diff clients.
        start = 1
        for mount_obj in self.mounts:
            cmd = ('cd %s/dir1/dir%s/; '
                   'for FILENAME in *;'
                   'do echo > $FILENAME; '
                   'done;'
                   % (mount_obj.mountpoint, str(start)))
            g.run(mount_obj.client_system, cmd)

        # Create softlink and hardlink of files in mountpoint. Start is an
        # offset to be added to dirname to act on diff files at diff clients.
        start = 1
        for mount_obj in self.mounts:
            cmd = ('cd %s/dir1/dir%s; '
                   'for FILENAME in *; '
                   'do ln -s $FILENAME softlink_$FILENAME; '
                   'done;'
                   % (mount_obj.mountpoint, str(start)))
            g.run(mount_obj.client_system, cmd)
            cmd = ('cd %s/dir1/dir%s; '
                   'for FILENAME in *; '
                   'do ln $FILENAME hardlink_$FILENAME; '
                   'done;'
                   % (mount_obj.mountpoint, str(start + 1)))
            g.run(mount_obj.client_system, cmd)
            start += 5

        # Delete op for deleting all file in one of the dirs. start is being
        # used as offset like in previous testcase in dir1
        start = 1
        for mount_obj in self.mounts:
            cmd = ('cd %s/dir1/dir%s; '
                   'for FILENAME in *; '
                   'do rm -f $FILENAME; '
                   'done;'
                   % (mount_obj.mountpoint, str(start)))
            g.run(mount_obj.client_system, cmd)
            start += 5

        # chmod, chown, chgrp inside dir1
        # start and end used as offset to access diff files
        # at diff clients.
        start, end = 2, 5
        for mount_obj in self.mounts:
            dir_file_range = '%s..%s' % (str(start), str(end))
            cmd = ('chmod 777 %s/dir1/dir{%s}/file{%s}'
                   % (mount_obj.mountpoint, dir_file_range, dir_file_range))
            g.run(mount_obj.client_system, cmd)

            cmd = ('chown root %s/dir1/dir{%s}/file{%s}'
                   % (mount_obj.mountpoint, dir_file_range, dir_file_range))
            g.run(mount_obj.client_system, cmd)

            cmd = ('chgrp root %s/dir1/dir{%s}/file{%s}'
                   % (mount_obj.mountpoint, dir_file_range, dir_file_range))
            g.run(mount_obj.client_system, cmd)

            start += 5
            end += 5

        # Create tiny, small, medium nd large file
        # at mountpoint. Offset to differ filenames
        # at diff clients.
        offset = 1
        for mount_obj in self.mounts:
            cmd = 'fallocate -l 100 tiny_file%s.txt' % str(offset)
            g.run(mount_obj.client_system, cmd)
            cmd = 'fallocate -l 20M small_file%s.txt' % str(offset)
            g.run(mount_obj.client_system, cmd)
            cmd = 'fallocate -l 200M medium_file%s.txt' % str(offset)
            g.run(mount_obj.client_system, cmd)
            cmd = 'fallocate -l 1G large_file%s.txt' % str(offset)
            g.run(mount_obj.client_system, cmd)
            offset += 1

        # Creating files on client side for dir1
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create dirs with file
            g.log.info('Creating dirs with file...')
            command = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                       "-d 2 -l 2 -n 2 -f 20 %s/dir1" % (
                           sys.version_info.major, self.script_upload_path,
                           mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validating IO's and waiting to complete
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
            )
        self.io_validation_complete = True

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")

        # Get areequal before killing the brick
        g.log.info('Getting areequal before killing of brick...')
        ret, result_before_killing_brick = (
            collect_mounts_arequal(self.mounts[0]))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before killing of brick '
                   'is successful')

        # Reset a brick
        g.log.info('Reset of brick using start')
        ret, _, _ = reset_brick(self.mnode, self.volname, bricks_list[0],
                                "start")

        # Check if the brick is offline
        g.log.info("Check the brick status if it is offline")
        ret = are_bricks_offline(self.mnode, self.volname, [bricks_list[0]])
        self.assertTrue(ret, "Brick is not offline")
        g.log.info("Expected : Brick is offline")

        # Reset brick by giving a different source and dst brick
        g.log.info('Reset of brick by giving different src and dst brick')
        ret, _, _ = reset_brick(self.mnode, self.volname, bricks_list[0],
                                "commit", bricks_list[1])
        self.assertNotEqual(ret, 0, "Not Expected: Reset brick is successfull")
        g.log.info("Expected : Source and Destination brick must be same for"
                   " reset")

        # Reset brick with destination same as source
        g.log.info('Reset of brick with same src and dst brick')
        ret, _, _ = reset_brick(self.mnode, self.volname, bricks_list[0],
                                "commit", bricks_list[0])
        self.assertNotEqual(ret, 0, "Not Expected : Reset brick is successful")
        g.log.info("Expected : Reset brick failed,Vol id is same use force")

        # Obtain hostname of node
        ret, hostname_node1, _ = g.run(self.mnode, "hostname")
        self.assertEqual(ret, 0, ("Failed to obtain hostname of node %s",
                                  self.mnode))
        g.log.info("Obtained hostname of client. IP- %s, hostname- %s",
                   self.mnode, hostname_node1.strip())

        # Reset brick with destination same as source with force using hostname
        g.log.info('Reset of brick with same src and dst brick')
        ret, _, _ = reset_brick(hostname_node1.strip(), self.volname,
                                bricks_list[0], "commit", bricks_list[0],
                                force="true")
        self.assertEqual(ret, 0, "Not Expected: Reset brick failed")
        g.log.info("Expected : Reset brick is successful")

        # Wait for brick to come online
        g.log.info("Waiting for brick to come online")
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Bricks are not online")
        g.log.info("Expected : Bricks are online")

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')
        g.log.info('Heal has completed successfully')

        # Check if bricks are online
        all_bricks = get_all_bricks(self.mnode, self.volname)
        ret = are_bricks_online(self.mnode, self.volname, all_bricks)
        self.assertTrue(ret, 'All bricks are not online')
        g.log.info('All bricks are online')

        # Bring down other bricks to max redundancy
        # Get List of bricks to bring offline

        # Bringing bricks offline
        ret = bring_bricks_offline(self.volname,
                                   bricks_list[1:3])
        self.assertTrue(ret, 'Bricks not offline')
        g.log.info('Bricks are offline successfully')
        sleep(2)

        # Check if 4 bricks are online
        all_bricks = []
        all_bricks = [bricks_list[0], bricks_list[3], bricks_list[4],
                      bricks_list[5]]
        ret = are_bricks_online(self.mnode, self.volname, all_bricks)
        self.assertTrue(ret, 'All bricks are not online')
        g.log.info('All bricks are online')

        # Check mount point
        cmd = 'ls -lrt /mnt'
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        g.log.info("Client mount point details ")

        # Get arequal after bringing down bricks
        g.log.info('Getting arequal after bringing down bricks...')
        ret, result_offline_redundant_brick1 = (
            collect_mounts_arequal(self.mounts[0]))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks offline '
                   'is successful')

        # Bring bricks online
        list_of_bricks_to_bring_online = bricks_list[1:3]
        ret = bring_bricks_online(self.mnode, self.volname,
                                  list_of_bricks_to_bring_online)
        self.assertTrue(ret, 'Bricks not brought online')
        g.log.info('Bricks are online successfully')

        # Wait for brick to come online
        g.log.info("Waiting for brick to come online")
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Bricks are not online")
        g.log.info("Expected : Bricks are online")

        # Check if bricks are online
        all_bricks = get_all_bricks(self.mnode, self.volname)
        ret = are_bricks_online(self.mnode, self.volname, all_bricks)
        self.assertTrue(ret, 'All bricks are not online')
        g.log.info('All bricks are online')

        # Reset brick without bringing down brick
        g.log.info('Reset of brick by giving different src and dst brick')
        ret, _, _ = reset_brick(self.mnode, self.volname, bricks_list[1],
                                "commit", bricks_list[1])
        self.assertNotEqual(ret, 0, "Not Expected: Reset brick passed")
        g.log.info("Expected : Brick reset failed as source brick must be"
                   " stopped")

        # Kill the brick manually
        ret = bring_bricks_offline(self.volname,
                                   [bricks_list[1]])
        self.assertTrue(ret, 'Brick not offline')
        g.log.info('Brick is offline successfully')

        # Check if the brick is offline
        g.log.info("Check the brick status if it is offline")
        ret = are_bricks_offline(self.mnode, self.volname, [bricks_list[1]])
        self.assertTrue(ret, "Brick is not offline")
        g.log.info("Expected : Brick is offline")

        # Reset brick with dest same as source after killing brick manually
        g.log.info('Reset of brick by giving different src and dst brick')
        ret, _, _ = reset_brick(self.mnode, self.volname, bricks_list[1],
                                "commit", bricks_list[1], force="true")
        self.assertEqual(ret, 0, "Not Expected: Reset brick failed")
        g.log.info("Expected : Reset brick is successful")

        # Wait for brick to come online
        g.log.info("Waiting for brick to come online")
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, "Bricks are not online")
        g.log.info("Expected : Bricks are online")

        # Check if bricks are online
        all_bricks = get_all_bricks(self.mnode, self.volname)
        ret = are_bricks_online(self.mnode, self.volname, all_bricks)
        self.assertTrue(ret, 'All bricks are not online')
        g.log.info('All bricks are online')

        # Bring down other bricks to max redundancy
        # Bringing bricks offline
        ret = bring_bricks_offline(self.volname,
                                   bricks_list[2:4])
        self.assertTrue(ret, 'Bricks not offline')
        g.log.info('Bricks are offline successfully')

        # Check mount point
        cmd = 'ls -lrt /mnt'
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        g.log.info("Client mount point details")

        # Get arequal after bringing down bricks
        g.log.info('Getting arequal after bringing down redundant bricks...')
        ret, result_offline_redundant_brick2 = (
            collect_mounts_arequal(self.mounts[0]))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal before getting bricks offline '
                   'is successful')

        # Bring bricks online
        list_of_bricks_to_bring_online = bricks_list[2:4]
        ret = bring_bricks_online(self.mnode, self.volname,
                                  list_of_bricks_to_bring_online)
        self.assertTrue(ret, 'Bricks not brought online')
        g.log.info('Bricks are online successfully')

        # Removing brick from backend
        brick = bricks_list[0].strip().split(":")
        cmd = "rm -rf %s" % brick[1]
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 0, "Failed to delete brick %s"
                         % bricks_list[0])
        g.log.info("Removed brick %s sucessfully", bricks_list[0])

        # Check if the brick is offline
        count = 0
        while count <= 20:
            g.log.info("Check the brick status if it is offline")
            ret = are_bricks_offline(self.mnode, self.volname,
                                     [bricks_list[0]])
            if ret:
                break
            sleep(2)
            count = + 1
        self.assertTrue(ret, "Brick is not offline")
        g.log.info("Expected : Brick is offline")

        # Reset brick with destination same as source
        g.log.info('Reset of brick with same src and dst brick')
        ret, _, _ = reset_brick(hostname_node1.strip(), self.volname,
                                bricks_list[0], "commit", bricks_list[0])
        self.assertEqual(ret, 0, "Not Expected: Reset brick failed")
        g.log.info("Expected : Reset brick is successful")

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')
        g.log.info('Heal has completed successfully')

        # Comparing arequals
        self.assertEqual(result_before_killing_brick,
                         result_offline_redundant_brick1,
                         'Arequals are not equals before killing brick'
                         'processes and after offlining redundant bricks')
        g.log.info('Arequals are equals before killing brick'
                   'processes and after offlining redundant bricks')

        # Comparing arequals
        self.assertEqual(result_offline_redundant_brick2,
                         result_offline_redundant_brick1,
                         'Arequals are not equals for offlining redundant'
                         ' bricks')
        g.log.info('Arequals are equals for offlining redundant bricks')

        # Deleting dir1
        cmd = ('rm -rf %s/dir1' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to delete directory1")
        g.log.info("Directory 1 deleted successfully for %s", self.mounts[0])
