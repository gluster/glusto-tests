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
Test Description:
    Verify Eagerlock and other-eagerlock behavior
"""
from unittest import SkipTest
from random import choice
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.dht_test_utils import find_hashed_subvol
from glustolibs.gluster.glusterdir import rmdir
from glustolibs.gluster.lib_utils import (append_string_to_file,
                                          get_extended_attributes_info)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           get_volume_options)
from glustolibs.misc.misc_libs import (yum_install_packages,
                                       upload_scripts)


@runs_on([['dispersed', 'distributed-dispersed'], ['glusterfs']])
class DisperseEagerlockTest(GlusterBaseClass):
    # Method to setup the environment for test case

    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()
        # Check for availability of atleast 4 clients
        if len(cls.clients) < 4:
            raise SkipTest("This test requires atleast 4 clients")
        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients {}"
                                 .format(cls.clients))
        # Install time package on all clients needed for measurement of ls

        ret = yum_install_packages(cls.clients, 'time')
        if not ret:
            raise ExecutionError("Failed to install TIME package on all nodes")

    def setUp(self):
        """
        setUp method
        """
        # Setup_Volume
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to setup and mount volume")
        g.log.info("Volume %s has been setup successfully", self.volname)

    def _filecreate_and_hashcheck(self, timeoutval):
        """Create a file and check on which subvol it is hashed to"""
        # Create and write to a file to test the eagerlock timeout behavior
        objectname = 'EagerLockTimeoutCheck-file-' + timeoutval
        objectpath = ("{}/{}".format(self.mounts[0].mountpoint, objectname))
        ret = append_string_to_file(self.mounts[0].client_system,
                                    objectpath, 'EagerLockTest')
        self.assertTrue(ret, 'create and append of %s failed' % objectname)
        ret = get_subvols(self.mnode, self.volname)
        # Find the hashed subvol of the file created
        if len(ret['volume_subvols']) > 1:
            _, hashed_subvol = find_hashed_subvol(ret['volume_subvols'],
                                                  '', objectname)
            if hashed_subvol is None:
                g.log.error("Error in finding hash value of %s", objectname)
                return None
            return (objectname, ret['volume_subvols'], hashed_subvol)
        # Set subvol to 0 for plain(non-distributed) disperse volume
        hashed_subvol = 0
        return (objectname, ret['volume_subvols'], hashed_subvol)

    @staticmethod
    def _get_dirty_xattr_value(ret, hashed_subvol, objectname):
        """Get trusted.ec.dirty xattr value to validate eagerlock behavior"""
        # Collect ec.dirty xattr value from each brick
        hashvals = []
        for subvol in ret[hashed_subvol]:
            host, brickpath = subvol.split(':')
            brickpath = brickpath + '/' + objectname
            ret = get_extended_attributes_info(host, [brickpath],
                                               encoding='hex',
                                               attr_name='trusted.ec.dirty')
            ret = ret[brickpath]['trusted.ec.dirty']
            hashvals.append(ret)
        # Check if xattr values are same across all bricks
        if hashvals.count(hashvals[0]) == len(hashvals):
            del hashvals
            return ret
        g.log.error("trusted.ec.dirty value is not consistent across the "
                    "disperse set %s", hashvals)
        return None

    def _change_eagerlock_timeouts(self, timeoutval):
        """Change eagerlock and other-eagerlock timeout values as per input"""
        ret = set_volume_options(self.mnode, self.volname,
                                 {'disperse.eager-lock-timeout': timeoutval,
                                  'disperse.other-eager-lock-timeout':
                                      timeoutval})
        self.assertTrue(ret, 'failed to change eager-lock timeout values to '
                             '%s sec on %s' % (timeoutval, self.volname))
        g.log.info("SUCCESS:Changed eager-lock timeout vals to %s sec on %s",
                   timeoutval, self.volname)

    def _file_dir_create(self, clients, mountpoint):
        """Create Directories and files which will be used for
        checking response time of lookups"""
        client = choice(clients)
        cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
               "--dirname-start-num 0 "
               "--dir-depth 2 "
               "--dir-length 4 "
               "--max-num-of-dirs 4 "
               "--num-of-files 100 %s" % (self.script_upload_path, mountpoint))
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "FAILED to create data needed for lookups")

    @staticmethod
    def _lookup_response_time(clients, mountpoint):
        """ Check lookup response time which should be around 2-3 sec """
        # Sleeping to allow some cache timeout
        sleep(60)
        cmd = '/usr/bin/time -f "%e" ls -lRt ' + mountpoint + ' >>/dev/null'
        results = g.run_parallel(clients, cmd)
        # Checking the actual time taken for lookup
        for ret_values in results.values():
            _, _, ret = ret_values
            calc = float(ret.strip())
            if calc > 2:
                g.log.error("lookups taking more than 2 seconds."
                            " Actual time: %s", calc)

    def _rmdir_on_mountpoint(self, clients, mountpoint):
        """ Perform rm of created files as part of Sanity Check """
        # Skipping below lines of code as running rm -rf parallely
        # from multiple clients is a known bug Refer BZ-1787328
        # cmd = 'rm -rf ' + mountpoint
        # results = g.run_parallel(clients, cmd)
        # for client, ret_values in results.items():
        #    ret, out, err = ret_values
        #    self.assertEqual(ret, 0, "rm -rf failed on %s with %s"
        #                     % (client, err))
        ret = rmdir(choice(clients), mountpoint + '/*', force=True)
        self.assertTrue(ret, "rm -rf failed")
        ret, out, err = g.run(choice(clients), 'ls ' + mountpoint)
        self.assertEqual((ret, out, err), (0, '', ''),
                         "Some entries still exist even after rm -rf ;"
                         " the entries are %s and error msg is %s"
                         % (out, err))
        g.log.info("rm -rf was successful")

    def test_eagerlock(self):
        """
        Test Steps:
        1) Create an ecvolume
        2) Test EagerLock and Other-EagerLock default values and timeout-values
        3) Set the timeout values to 60
        4) Write to a file and check backend brick for
        "trusted.ec.dirty=0x00000000000000000000000000000000", must be non-zero
        5) Create some  dirs and  files in each dir
        6) Do ls -lRt * --> must not take more than 2-3sec
        7) disable eager lock
        8) retest write to a file and this time lock must be released
           immediately with dirty.xattr value all zeros
        """
        # Get list of clients
        clients = []
        for mount_obj in self.mounts:
            clients.append(mount_obj.client_system)
            mountpoint = mount_obj.mountpoint

        # Check if EC Eagerlock set of options enabled with correct values
        ret = get_volume_options(self.mnode, self.volname)
        self.assertTrue(bool((ret['disperse.eager-lock'] ==
                              ret['disperse.other-eager-lock'] == 'on') and
                             (ret['disperse.eager-lock-timeout'] ==
                              ret['disperse.other-eager-lock-timeout'] ==
                              '1')),
                        'Some EC-eagerlock options set are not correct')
        # Test behavior with default timeout value of 1sec
        objectname, ret, hashed_subvol = self._filecreate_and_hashcheck('1sec')
        sleep(2)
        ret = self._get_dirty_xattr_value(ret, hashed_subvol, objectname)
        self.assertEqual(ret, '0x00000000000000000000000000000000',
                         "Unexpected dirty xattr value is %s on %s"
                         % (ret, objectname))
        self._file_dir_create(clients, mountpoint)
        # Now test the performance issue wrt lookups
        self._lookup_response_time(clients, mountpoint)
        # Do rm -rf of created data as sanity test
        self._rmdir_on_mountpoint(clients, mountpoint)

        # Increasing timeout values to 60sec in order to test the functionality
        self._change_eagerlock_timeouts('60')
        self._file_dir_create(clients, mountpoint)
        objectname, ret, hashed_subvol =\
            self._filecreate_and_hashcheck('60seconds')
        # Check in all the bricks "trusted.ec.dirty" value
        # It should be "0x00000000000000010000000000000001"
        _ = self._get_dirty_xattr_value(ret, hashed_subvol, objectname)
        self.assertEqual(_, '0x00000000000000010000000000000001',
                         "Unexpected dirty xattr value %s on %s"
                         % (_, objectname))
        # Sleep 60sec after which dirty_val should reset to "0x00000..."
        sleep(62)
        _ = self._get_dirty_xattr_value(ret, hashed_subvol, objectname)
        self.assertEqual(_, '0x00000000000000000000000000000000',
                         "Unexpected dirty xattr value is %s on %s"
                         % (_, objectname))
        # Test the performance issue wrt lookups
        self._lookup_response_time(clients, mountpoint)
        # Do rm -rf of created data as sanity test
        self._rmdir_on_mountpoint(clients, mountpoint)

        # Disable EagerLock and other-Eagerlock
        ret = set_volume_options(self.mnode, self.volname,
                                 {'disperse.eager-lock': 'off',
                                  'disperse.other-eager-lock': 'off'})
        self.assertTrue(ret, "failed to turn off eagerlock and "
                             "other eagerlock on %s" % self.volname)
        g.log.info("SUCCESS: Turned off eagerlock and other-eagerlock on %s",
                   self.volname)
        # Again create same dataset and retest ls -lRt, shouldnt take much time
        self._file_dir_create(clients, mountpoint)
        # Create a new file see the dirty flag getting unset immediately
        objectname, ret, hashed_subvol = self._filecreate_and_hashcheck(
            'Eagerlock_Off')
        # Check in all the bricks "trusted.ec.dirty value"
        # It should be "0x00000000000000000000000000000000"
        ret = self._get_dirty_xattr_value(ret, hashed_subvol, objectname)
        self.assertEqual(ret, '0x00000000000000000000000000000000',
                         "Unexpected dirty xattr value is %s on %s"
                         % (ret, objectname))
        # Test the performance issue wrt ls
        self._lookup_response_time(clients, mountpoint)
        # Cleanup created data as sanity test
        self._rmdir_on_mountpoint(clients, mountpoint)

    def tearDown(self):
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
