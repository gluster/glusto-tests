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

from re import sub
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.io.utils import run_crefi
from glustolibs.gluster.brick_libs import get_subvols
from glustolibs.gluster.glusterdir import (rmdir, get_dir_contents)
from glustolibs.gluster.lib_utils import get_extended_attributes_info
from glustolibs.gluster.volume_libs import get_volume_type_info
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['distributed', 'replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed', 'arbiter', 'distributed-arbiter'],
          ['glusterfs']])
class ValidateCtimeFeatures(GlusterBaseClass):
    """
    This testcase validates ctime(consistent times) feature
    """

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

    # Need to get list of host and resp brickpaths
    # Get list of entries under any one path
    # Get xattr values on each brick of same path
    # Compare them to see if mdata exists and value is same
    # For arbiter the value may not be same on arbiter brick

    def validate_xattr_values(self, dirname, ctime=True):
        """Validate existence and consistency of a specific
           xattr value across replica set

        Args:
            dirname (str): parent directory name
        Kwargs:
            ctime(bool): ctime feature enablement
        """
        # pylint: disable=too-many-branches
        # Fetch all replica sets(subvols) in the volume
        ret = get_subvols(self.mnode, self.volname)
        # Iterating through each subvol(replicaset)
        for subvol in ret['volume_subvols']:
            brick_host_list = {}  # Dict for storing host,brickpath pairs
            for each in subvol:  # Fetching each replica in replica set
                # Splitting to brick,hostname pairs
                host, brick_path = each.split(':')
                brick_host_list[host] = brick_path
            # Fetch Complete parent directory path
            directory = brick_path + '/' + dirname
            # Fetching all entries recursively in a replicaset
            entry_list = get_dir_contents(host, directory, recursive=True)
            for each in entry_list:
                xattr_value = []  # list to store xattr value
                # Logic to get xattr values
                for host, brickpath in brick_host_list.items():
                    # Remove the prefix brick_path from entry-name
                    each = sub(brick_path, '', each)
                    # Adding the right brickpath name for fetching xattrval
                    brick_entry_path = brickpath + each
                    ret = get_extended_attributes_info(host,
                                                       [brick_entry_path],
                                                       encoding='hex',
                                                       attr_name='trusted'
                                                                 '.glusterfs.'
                                                                 'mdata')
                    if ret:
                        ret = ret[brick_entry_path]['trusted.glusterfs.mdata']
                        g.log.info("mdata xattr value of %s is %s",
                                   brick_entry_path, ret)
                    else:
                        pass
                    if ctime:
                        self.assertIsNotNone(ret, "glusterfs.mdata not set on"
                                                  " {}"
                                             .format(brick_entry_path))
                        g.log.info("mdata xattr %s is set on the back-end"
                                   " bricks", ret)
                    else:
                        self.assertIsNone(ret, "trusted.glusterfs.mdata seen "
                                               " on {}"
                                          .format(brick_entry_path))
                        g.log.info("mdata xattr %s is not set on the back-end"
                                   " bricks", ret)
                    xattr_value.append(ret)
                voltype = get_volume_type_info(self.mnode, self.volname)
                if voltype['volume_type_info']['arbiterCount'] == '0':
                    ret = bool(xattr_value.count(xattr_value[0]) ==
                               len(xattr_value))
                elif voltype['volume_type_info']['arbiterCount'] == '1':
                    ret = bool(((xattr_value.count(xattr_value[0])) or
                                (xattr_value.count(xattr_value[1])) > 1))
                else:
                    g.log.error("Arbiter value is neither 0 nor 1")
                if ctime:
                    self.assertTrue(ret, 'trusted.glusterfs.mdata' +
                                    ' value not same across bricks for '
                                    'entry ' + each)
                else:
                    self.assertTrue(ret, 'trusted.glusterfs.mdata' +
                                    ' seems to be set on some bricks for ' +
                                    each)

    def data_create(self, dirname):
        """Create different files and directories"""
        dirname = self.mounts[0].mountpoint + '/' + dirname
        list_of_fops = ["create", "rename", "chmod", "chown", "chgrp",
                        "hardlink", "truncate", "setxattr"]
        for fops in list_of_fops:
            ret = run_crefi(self.mounts[0].client_system,
                            dirname, 10, 3, 3, thread=4,
                            random_size=True, fop=fops, minfs=0,
                            maxfs=102400, multi=True, random_filename=True)
            self.assertTrue(ret, "crefi failed during {}".format(fops))
            g.log.info("crefi PASSED FOR fop %s", fops)
        g.log.info("IOs were successful using crefi")

    def data_delete(self, dirname):
        """Delete created data"""
        dirname = self.mounts[0].mountpoint + '/' + dirname
        ret = rmdir(self.mounts[0].client_system, dirname, force=True)
        self.assertTrue(ret, 'deletion of data failed')

    def test_consistent_timestamps_feature(self):
        '''
        Test Steps:
        1. Create a volume, enable features.ctime, mount volume
        2. Create different files and directories
        3. For each entry trusted.glusterfs.mdata  must be set
        4. For every entry, above xattr must match on each brick of replicaset
        5. Delete all data created
        6. turn off features.ctime
        7. Again create different files and directories
        8. "glusterfs.mdata xattr" must not be present for any entry
        9. Delete created data
        '''
        # pylint: disable=too-many-statements

        # Enable features.ctime
        ret = set_volume_options(self.mnode, self.volname,
                                 {'features.ctime': 'on'})
        self.assertTrue(ret, 'failed to enable ctime feature on %s'
                        % self.volume)
        g.log.info("Successfully enabled ctime feature on %s", self.volume)

        # Create different files and directories
        self.data_create('ctime-on')

        # Check if mdata xattr has been set for all entries
        # Check if the values are same across all replica copies
        self.validate_xattr_values('ctime-on')

        # Delete all the existing data
        self.data_delete('ctime-on')

        # Disable features.ctime
        ret = set_volume_options(self.mnode, self.volname,
                                 {'features.ctime': 'off'})
        self.assertTrue(ret, 'failed to disable features_ctime feature on %s'
                        % self.volume)
        g.log.info("Successfully disabled ctime feature on %s", self.volume)

        # Create different files and directories
        self.data_create('ctime-off')

        # Check that mdata xattr has not been set for any entries
        self.validate_xattr_values('ctime-off', ctime=False)

        # Delete all the existing data
        self.data_delete('ctime-off')
