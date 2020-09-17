#  Copyright (C) 2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_ops import (get_volume_options,
                                           set_volume_options)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.glusterfile import get_file_stat


@runs_on([['replicated', 'distributed', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'arbiter', 'distributed-arbiter'],
          ['glusterfs']])
class ConsistentValuesAcrossTimeStamps(GlusterBaseClass):
    """
    This testcase tests for atime, ctime and mtime to be same when a
    file or directory is created
    """

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
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
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def validate_timestamp(self, objectpath, objectname):
        ret = get_file_stat(self.mounts[0].client_system, objectpath)
        self.assertTrue(bool(ret["atime"] == ret["ctime"] == ret["mtime"]),
                        "a|m|c timestamps on {} are not equal"
                        .format(objectname))
        g.log.info("a|m|c timestamps on %s are same", objectname)

    def test_time_stamps_on_create(self):
        '''
        1. Create a volume , enable features.ctime, mount volume
        2. Create a directory "dir1" and check the a|m|c times
        3. Create a file "file1"  and check the a|m|c times
        4. Again create a new file "file2" as below
            command>>> touch file2;stat file2;stat file2
        5. Check the a|m|c times of "file2"
        6. The atime,ctime,mtime must be same within each object
        '''
        # pylint: disable=too-many-statements

        # Check if ctime feature is disabled by default
        ret = get_volume_options(self.mnode, self.volname, "features.ctime")
        self.assertEqual(ret['features.ctime'], 'off',
                         'features_ctime is not disabled by default')
        g.log.info("ctime feature is disabled by default as expected")

        # Enable features.ctime
        ret = set_volume_options(self.mnode, self.volname,
                                 {'features.ctime': 'on'})
        self.assertTrue(ret, 'failed to enable features_ctime feature on %s'
                        % self.volume)
        g.log.info("Successfully enabled ctime feature on %s", self.volume)

        # Create a directory and check if ctime, mtime, atime is same
        objectname = 'dir1'
        objectpath = ('%s/%s' % (self.mounts[0].mountpoint, objectname))
        ret = mkdir(self.mounts[0].client_system, objectpath)
        self.assertTrue(ret, "{} creation failed".format(objectname))
        g.log.info("%s was successfully created on %s", objectname,
                   self.mounts[0])
        self.validate_timestamp(objectpath, objectname)

        # Create a file and check if ctime, mtime, atime is same
        objectname = 'file1'
        objectpath = ('%s/%s' % (self.mounts[0].mountpoint, objectname))
        cmd = ('touch  %s' % objectpath)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "touch command to create {} has "
                         "failed".format(objectname))
        g.log.info("%s was successfully created on %s", objectname,
                   self.mounts[0])
        self.validate_timestamp(objectpath, objectname)

        # Create a file and issue stat immediately. This step helps in
        # testing a corner case where issuing stat immediately was changing
        # ctime before the touch was effected on the disk
        objectname = 'file2'
        objectpath = ('%s/%s' % (self.mounts[0].mountpoint, objectname))
        cmd = ("touch {obj};stat {obj};stat {obj}".format(obj=objectpath))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "touch command to create {}  has "
                                 "failed".format(objectname))
        g.log.info("%s was successfully created on %s", objectname,
                   self.mounts[0])
        self.validate_timestamp(objectpath, objectname)
