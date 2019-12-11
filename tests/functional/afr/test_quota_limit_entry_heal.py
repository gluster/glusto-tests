#  Copyright (C) 2015-2018  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           are_bricks_online,
                                           get_all_bricks)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.io.utils import wait_for_io_to_complete
from glustolibs.gluster.quota_ops import (quota_enable,
                                          is_quota_enabled,
                                          quota_limit_objects)
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['replicated'],
          ['glusterfs']])
class QuotaEntrySelfHealTest(GlusterBaseClass):
    """
     Description:
        Verify entry-selfheal happens in the right direction with Quota
        enforcement and no conservative merge happens.
    """
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

        # Override Volumes
        if cls.volume_type == "replicated":
            # Define 1x3 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp'}

    def setUp(self):
        self.get_super_method(self, 'setUp')()
        g.log.info("Starting to Setup Volume %s", self.volname)

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
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
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

    def test_entry_heal_with_quota(self):
        """
        - Create a 1x3 volume
        - Set quota object limit
        - Create files less than the limit
        - Bring down a brick and create more files until limit is hit
        - Delete one file so that we are below the limit, and create one more
          file
        - Bring the brick back up and launch heal
        - Verify that after heal is complete, the deleted file does not
          re-appear in any of the bricks.
        """
        # pylint: disable=too-many-statements
        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Check if quota is enabled
        g.log.info("Validate Quota is enabled on the volume %s", self.volname)
        ret = is_quota_enabled(self.mnode, self.volname)
        self.assertTrue(ret, ("Quota is not enabled on the volume %s",
                              self.volname))
        g.log.info("Successfully Validated quota is enabled on volume %s",
                   self.volname)

        # Set quota related options
        options = {"quota-deem-statfs": "on",
                   "soft-timeout": "0",
                   "hard-timeout": "0"}
        g.log.info("setting quota volume options %s", options)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for "
                              "volume %s" % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # Create directory on mount
        ret = mkdir(self.mounts[0].client_system, "%s/dir"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, "mkdir failed")

        # Set Quota limit on the directory
        path = "/dir"
        g.log.info("Setting Quota Limit object on the path %s of the "
                   "volume %s", path, self.volname)
        ret, _, _ = quota_limit_objects(self.mnode, self.volname,
                                        path=path, limit="10")
        self.assertEqual(ret, 0, ("Failed to set quota limit object "
                                  "on path %s of the volume %s",
                                  path, self.volname))
        g.log.info("Successfully set the Quota limit object on %s of the "
                   "volume %s", path, self.volname)

        cmd = ("touch %s/dir/file{1..5}" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "file creation failed")

        # Bring brick3 offline
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info('Bringing brick %s offline', bricks_list[2])
        ret = bring_bricks_offline(self.volname, bricks_list[2])
        self.assertTrue(ret, 'Failed to bring brick %s offline'
                        % bricks_list[2])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[2]])
        self.assertTrue(ret, 'Brick %s is not offline'
                        % bricks_list[2])
        g.log.info('Bringing brick %s offline was successful',
                   bricks_list[2])

        # Create files until quota object limit
        cmd = ("touch %s/dir/file{6..9}" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "file creation failed")

        # The next create must fail
        cmd = ("touch %s/dir/file10" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 1, ("Creation of %s/dir/file10 succeeded while "
                                  "it was not supposed to."
                                  % self.mounts[0].mountpoint))
        g.log.info("Creation of %s/dir/file10 failed as expected due to "
                   "quota object limit.", self.mounts[0].mountpoint)

        # Delete one file and re-try the create to succeed.
        cmd = ("rm %s/dir/file1" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "File deletion failed")
        cmd = ("touch %s/dir/file10" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "File creation failed")

        # Bring brick3 online and check status
        g.log.info('Bringing brick %s online...', bricks_list[2])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[2]])
        self.assertTrue(ret, 'Failed to bring brick %s online' %
                        bricks_list[2])
        g.log.info('Bringing brick %s online is successful', bricks_list[2])

        g.log.info("Verifying if brick3 is online....")
        ret = are_bricks_online(self.mnode, self.volname, bricks_list)
        self.assertTrue(ret, ("brick3 did not come up"))
        g.log.info("brick3 has come online.")

        # Trigger heal
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Starting heal failed')
        g.log.info('Index heal launched')

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Verify that file10 did not get recreated on the down brick by an
        # accidental conservative merge.
        for brick in bricks_list:
            node, brick_path = brick.split(':')
            ret, _, _ = g.run(node, 'stat %s/dir/file10' % brick_path)
            self.assertFalse(ret, 'File present!')
