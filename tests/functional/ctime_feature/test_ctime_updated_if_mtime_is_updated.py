#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.glusterfile import get_file_stat
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['distributed', 'replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'arbiter', 'distributed-arbiter'],
          ['glusterfs']])
class TestCtimeGetUpdated(GlusterBaseClass):
    """ Whenever atime or mtime gets updated ctime too must get updated"""

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=[self.mounts[0]],
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """tearDown"""
        self.get_super_method(self, 'tearDown')()
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

    def test_ctime_updated_if_mtime_is_updated(self):
        """
        whenever atime or mtime gets updated ctime too must get updated
        1. test with features.ctime enabled
        2. touch /mnt/file1
        3. stat /mnt/file1
        4. sleep 1;
        5. touch -m -d "2020-01-01 12:00:00" /mnt/file1
        6. stat /mnt/file1
        """
        # Enable features.ctime
        ret = set_volume_options(
            self.mnode, self.volname, {'features.ctime': 'on'})
        self.assertTrue(ret, 'failed to enable ctime feature on %s'
                        % self.volume)

        # Create a file on the mountpoint
        objectname = 'file_zyx1'
        objectpath = ('%s/%s' % (self.mounts[0].mountpoint, objectname))
        create_file_cmd = "touch {}".format(objectpath)
        modify_mtimr_cmd = (
            'touch -m -d "2020-01-01 12:00:00" {}'.format(objectpath))
        ret, _, _ = g.run(self.mounts[0].client_system, create_file_cmd)
        self.assertFalse(ret, "File creation failed on the mountpoint")

        # Get stat of the file
        stat_data = get_file_stat(self.mounts[0].client_system, objectpath)
        self.assertFalse(
            ret, "Failed to get stat of the file {}".format(objectname))
        ret, _, _ = g.run(self.mounts[0].client_system, modify_mtimr_cmd)
        self.assertFalse(ret, "Failed to run {}".format(modify_mtimr_cmd))

        sleep(3)
        stat_data1 = get_file_stat(self.mounts[0].client_system, objectpath)

        # Check if mtime and ctime are changed
        for key in ('mtime', 'ctime'):
            self.assertNotEqual(
                stat_data[key], stat_data1[key], "Before and after not same")
