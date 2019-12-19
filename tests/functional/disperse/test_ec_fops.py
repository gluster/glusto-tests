#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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
    Tests File Operations on a healthy EC volume
"""
from os import getcwd
import sys

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (collect_mounts_arequal,
                                 validate_io_procs)
from glustolibs.gluster.brick_libs import (
    get_all_bricks, bring_bricks_offline)


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestFops(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
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

    def test_fops_ec_volume(self):
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        """
        - 1.Start resource consumption tool
        - 2.Create directory dir1
        - 3.Create 5 dir and 5 files in each dir in directory 1
        - 4.Rename all file inside dir1
        - 5.Truncate at any dir in mountpoint inside dir1
        - 6.Create softlink and hardlink of files in mountpoint
        - 7.Delete op for deleting all file in one of the dirs
        - 8.chmod, chown, chgrp inside dir1
        - 9.Create tiny, small, medium nd large file
        - 10.Creating files on client side for dir1
        - 11.Validating IO's and waiting to complete
        - 12.Get areequal before killing the brick
        - 13.Killing 1st brick manually
        - 14.Get areequal after killing 1st brick
        - 15.Killing 2nd brick manually
        - 16.Get areequal after killing 2nd brick
        - 17.Getting arequal and comparing the arequals
        - 18.Deleting dir1
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

        # get the bricks from the volume
        g.log.info("Fetching bricks for the volume : %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick List : %s", bricks_list)

        # Creating dir1
        cmd = ('mkdir  %s/dir1' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create directory1")
        g.log.info("Directory 1 created successfully for %s", self.mounts[0])

        # Create 5 dir and 5 files in each dir at mountpoint on dir1
        start = 1
        end = 5
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

        # chmod, chown, chgrp inside dir1
        # start and end used as offset to access diff files
        # at diff clients.
        start = 2
        end = 5
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

        # Creating 2TB file if volume is greater
        # than equal to 3TB
        list1 = []
        command = ("df %s" % mount_obj.mountpoint)
        rcode, rout, rerr = g.run(mount_obj.client_system[0], command)
        if rcode == 0:
            list1 = rout.split("\n")[1].split()
            avail = list1[3]
            if int(avail) >= 3000000000:
                cmd = 'fallocate -l 2TB tiny_file_large.txt'
                g.run(mount_obj.client_system[0], cmd)
        g.log.error("Get mountpoint failed: %s", rerr)

        # Creating files on client side for dir1
        # Write IO
        all_mounts_procs = []
        count = 1
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 10 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (
                       sys.version_info.major, self.script_upload_path, count,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
            count = count + 10

        # Validating IO's and waiting to complete
        g.log.info("Validating IO's")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("Successfully validated all io's")

        # Get areequal before killing the brick
        g.log.info('Getting areequal before killing of brick...')
        ret, result_before_killing_brick = (
            collect_mounts_arequal(self.mounts[0]))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before killing of brick '
                   'is successful')

        # Kill 1st brick manually
        ret = bring_bricks_offline(self.volname,
                                   [bricks_list[1]])
        self.assertTrue(ret, 'Brick not offline')
        g.log.info('Brick is offline successfully')

        # Get areequal after killing 1st brick
        g.log.info('Getting areequal after killing of brick...')
        ret, result_after_killing_brick = (
            collect_mounts_arequal(self.mounts[0]))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before killing of brick '
                   'is successful')

        # Kill 2nd brick manually
        ret = bring_bricks_offline(self.volname,
                                   [bricks_list[3]])
        self.assertTrue(ret, 'Brick not offline')
        g.log.info('Brick is offline successfully')

        # Get areequal after killing 2nd brick
        g.log.info('Getting areequal after killing of brick...')
        ret, result_after_killing_brick_2 = (
            collect_mounts_arequal(self.mounts[0]))
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting areequal before killing of brick '
                   'is successful')

        # Comparing areequals
        self.assertEqual(result_before_killing_brick,
                         result_after_killing_brick,
                         'Areequals are not equals before killing brick'
                         'processes and after offlining 1 redundant bricks')
        g.log.info('Areequals are equals before killing brick'
                   'processes and after offlining 1 redundant bricks')

        # Comparing areequals
        self.assertEqual(result_after_killing_brick,
                         result_after_killing_brick_2,
                         'Areequals are not equals after killing 2'
                         ' bricks')
        g.log.info('Areequals are equals after offlining 2 redundant bricks')

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

        # Deleting dir1
        cmd = ('rm -rf %s/dir1' % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to delete directory1")
        g.log.info("Directory 1 deleted successfully for %s", self.mounts[0])
