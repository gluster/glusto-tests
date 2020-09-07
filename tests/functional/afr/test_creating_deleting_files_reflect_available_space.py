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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import wait_for_io_to_complete


@runs_on([['replicated', 'distributed-replicated'], ['glusterfs']])
class VerifyAvaliableSpaceBeforeAfterDelete(GlusterBaseClass):
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
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
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

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_creating_deleting_files_reflect_available_space(self):
        """
        - note the current available space on the mount
        - create 1M file on the mount
        - note the current available space on the mountpoint and compare
          with space before creation
        - remove the file
        - note the current available space on the mountpoint and compare
          with space before creation
        """

        # Create 1M file on client side
        g.log.info('Creating file on %s', self.mounts[0].mountpoint)
        cmd = ("/usr/bin/env python %s create_files -f 1"
               " --fixed-file-size 1M %s" % (self.script_upload_path,
                                             self.mounts[0].mountpoint))
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)

        # Get the current available space on the mount
        g.log.info('Getting the current available space on the mount...')
        cmd = ("df --output=avail %s | grep '[0-9]'"
               % self.mounts[0].mountpoint)
        ret, out, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)
        space_before_file_creation = int(out)

        # Create 1M file on client side
        g.log.info('Creating file on %s', self.mounts[0].mountpoint)
        cmd = ("/usr/bin/env python %s create_files -f 1 "
               "--fixed-file-size 1M --base-file-name newfile %s/newdir"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)

        # Get the current available space on the mount
        g.log.info('Getting the current available space on the mount...')
        cmd = ("df --output=avail %s | grep '[0-9]'"
               % self.mounts[0].mountpoint)
        ret, out, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)
        space_after_file_creation = int(out)

        # Compare available size before creation and after creation file
        g.log.info('Comparing available size before creation '
                   'and after creation file...')
        space_diff = space_before_file_creation - space_after_file_creation
        space_diff = round(space_diff / 1024)
        g.log.info('Space difference is %d', space_diff)
        self.assertEqual(space_diff, 1.0,
                         'Available size before creation and '
                         'after creation file is not valid')
        g.log.info('Available size before creation and '
                   'after creation file is valid')

        # Delete file on client side
        g.log.info('Deleting file on %s', self.mounts[0].mountpoint)
        cmd = ("/usr/bin/env python %s delete %s/newdir"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        ret, _, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)

        # Get the current available space on the mount
        cmd = ("df --output=avail %s | grep '[0-9]'"
               % self.mounts[0].mountpoint)
        ret, out, err = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, err)
        space_after_file_deletion = int(out)

        # Compare available size before creation and after deletion file
        g.log.info('Comparing available size before creation '
                   'and after deletion file...')
        space_diff = space_before_file_creation - space_after_file_deletion
        space_diff_comp = space_diff < 200
        self.assertTrue(space_diff_comp,
                        'Available size before creation is not proportional '
                        'to the size after deletion file')
        g.log.info('Available size before creation is proportional '
                   'to the size after deletion file')
