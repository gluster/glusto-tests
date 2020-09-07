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
    Tests bricks EC version on a EC vol
    Bring down a brick and wait then bring down
    another brick and bring the first brick up healing
    should complete and EC version should be updated
"""
from time import sleep
from copy import deepcopy
from random import choice
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs)
from glustolibs.gluster.brick_libs import (
    bring_bricks_online,
    bring_bricks_offline)
from glustolibs.gluster.glusterdir import (mkdir)
from glustolibs.gluster.volume_libs import (get_subvols)


@runs_on([['dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class TestEcVersionBrickdown(GlusterBaseClass):

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
            raise ExecutionError("Failed to upload script on client")
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

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

    def get_xattr(self, xattr):
        """
        Function will get xattr and return true or false
        """

        _rc = True
        time_counter = 250
        g.log.info("The heal monitoring timeout is : %d minutes",
                   (time_counter / 60))
        while time_counter > 0:
            list1 = []
            for brick in self.bricks_list1:
                brick_node, brick_path = brick.split(":")
                cmd = ("getfattr -d -e hex -m. %s/dir1/|grep %s" %
                       (brick_path, xattr))
                _, out, _ = g.run(brick_node, cmd)
                list1.append(out)
            if len(self.bricks_list1) == list1.count(out):
                _rc = True
                return _rc
            else:
                sleep(120)
                time_counter = time_counter - 120
                _rc = False
        return _rc

    def test_ec_version(self):
        """
        Create a directory on the mountpoint
        Create files on the mountpoint
        Bring down a brick say b1
        Create more files on the mountpoint
        Bring down another brick b2
        Bring up brick b1
        Wait for healing to complete
        Check if EC version is updated
        Check is EC size is updated
        """
        # pylint: disable=too-many-statements,too-many-branches,too-many-locals

        # Creating dir1 on the mountpoint
        ret = mkdir(self.mounts[0].client_system, "%s/dir1"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, "Failed to create dir1")
        g.log.info("Directory dir1 on %s created successfully", self.mounts[0])

        # Creating files on client side for dir1
        g.log.info("Generating data for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)

        # Create dirs with file
        command = ("cd %s/dir1; for i in {1..10};do"
                   " dd if=/dev/urandom of=file.$i "
                   "bs=1024 count=10000; done" % self.mounts[0].mountpoint)

        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validating IO's and waiting to complete
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts[0]),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Bringing brick b1 offline
        sub_vols = get_subvols(self.mnode, self.volname)
        self.bricks_list1 = list(choice(sub_vols['volume_subvols']))
        brick_b1_down = choice(self.bricks_list1)
        ret = bring_bricks_offline(self.volname,
                                   brick_b1_down)
        self.assertTrue(ret, 'Brick %s is not offline' % brick_b1_down)
        g.log.info('Brick %s is offline successfully', brick_b1_down)

        del self.all_mounts_procs[:]
        # Creating files on client side for dir1
        g.log.info("Generating data for %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)

        # Create dirs with file
        command = ("cd %s/dir1; for i in {11..20};do"
                   " dd if=/dev/urandom of=file.$i "
                   "bs=1024 count=10000; done" % self.mounts[0].mountpoint)

        proc = g.run_async(self.mounts[0].client_system, command,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validating IO's and waiting to complete
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts[0]),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Changing mode owner and group of files
        dir_file_range = '2..5'
        cmd = ('chmod 777 %s/dir1/file.{%s}'
               % (self.mounts[0].mountpoint, dir_file_range))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "Changing mode of files has failed")

        g.log.info("Mode of files have been changed successfully")

        cmd = ('chown root %s/dir1/file.{%s}'
               % (self.mounts[0].mountpoint, dir_file_range))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "Changing owner of files has failed")
        g.log.info("Owner of files have been changed successfully")

        cmd = ('chgrp root %s/dir1/file.{%s}'
               % (self.mounts[0].mountpoint, dir_file_range))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "Changing group of files has failed")
        g.log.info("Group of files have been changed successfully")

        # Create softlink and hardlink of files in mountpoint.
        cmd = ('cd %s/dir1/; '
               'for FILENAME in *; '
               'do ln -s $FILENAME softlink_$FILENAME; '
               'done;'
               % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "Creating Softlinks have failed")
        g.log.info("Softlink of files have been changed successfully")

        cmd = ('cd %s/dir1/; '
               'for FILENAME in *; '
               'do ln $FILENAME hardlink_$FILENAME; '
               'done;'
               % (self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, "Creating Hardlinks have failed")
        g.log.info("Hardlink of files have been changed successfully")

        # Bringing brick b2 offline
        bricks_list2 = deepcopy(self.bricks_list1)
        bricks_list2.remove(brick_b1_down)
        brick_b2_down = choice(bricks_list2)
        ret = bring_bricks_offline(self.volname,
                                   brick_b2_down)
        self.assertTrue(ret, 'Brick %s is not offline' % brick_b2_down)
        g.log.info('Brick %s is offline successfully', brick_b2_down)

        # Bring brick b1 online
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [brick_b1_down],
                                  'glusterd_restart')
        self.assertTrue(ret, 'Brick %s is not brought'
                             'online' % brick_b1_down)
        g.log.info('Brick %s is online successfully', brick_b1_down)

        # Delete brick2 from brick list as we are not checking for heal
        # completion in brick 2 as it is offline

        self.bricks_list1.remove(brick_b2_down)

        # Check if EC version is same on all bricks which are up
        ret = self.get_xattr("ec.version")
        self.assertTrue(ret, "Healing not completed and EC version is"
                        "not updated")
        g.log.info("Healing is completed and EC version is updated")

        # Check if EC size is same on all bricks which are up
        ret = self.get_xattr("ec.size")
        self.assertTrue(ret, "Healing not completed and EC size is"
                        "not updated")
        g.log.info("Healing is completed and EC size is updated")
