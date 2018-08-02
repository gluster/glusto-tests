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

import time
import random
import string
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.lib_utils import (search_pattern_in_file,
                                          append_string_to_file)
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_limit_usage,
                                          quota_set_default_soft_limit,
                                          quota_set_soft_timeout,
                                          quota_set_hard_timeout,
                                          quota_set_alert_time)
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterdir import mkdir


@runs_on([['distributed-replicated', 'replicated', 'dispersed', 'distributed',
           'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class QuotaTimeOut(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        """
        Setup volume, mount volume and initialize necessary variables
        which is used in tests
        """
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup and Mount Volume %s",
                   cls.volname)
        ret = cls.setup_volume_and_mount_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume %s"
                                 % cls.volname)
        g.log.info("Successful in Setup and Mount Volume %s", cls.volname)

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume and umount volume from client
        """
        # Stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDownClass
        GlusterBaseClass.tearDownClass.im_func(cls)

    def test_alert_time_out(self):
        """
        Verifying directory quota functionality with respect to
        alert-time, soft-timeout and hard-timeout.

        * Enable quota
        * Set limit on '/'
        * Set default-soft-limit to 50%
        * Set alert time to 1 sec
        * Set soft-timeout to 0 sec
        * Set hard timeout to 0 sec
        * Check quota list
        * Perform some IO such that soft limit is not exceeded
        * Check for alert message in brick logfile (NO alert present)
        * Perform IO and exceed the soft limit
        * Check for alert message in brick logfile (Alert present)
        * Remove some files so that usage falls below soft limit
        * Create some files such that hard limit is exceeded
        * Check for alert message in brick logfile
        * Remove some files so that usage falls below soft limit
        * Check for alert message in brick logfile (NO alert present)

        """

        # pylint: disable=too-many-statements
        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertFalse(ret, "Failed to enable quota on the volume %s"
                         % self.volname)
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Path to set quota limit
        path = "/"

        # Create a directory from mount point
        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        g.log.info("Creating dir named 'foo' from client %s",
                   client)
        ret = mkdir(client, "%s/foo" % mount_dir)
        self.assertTrue(ret, "Failed to create dir under %s-%s"
                        % (client, mount_dir))
        g.log.info("Directory 'foo' created successfully")

        # Set Quota limit on the root of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="100MB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  " the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

        # Set default soft limit to 50%
        g.log.info("Set default soft limit:")
        ret, _, _ = quota_set_default_soft_limit(self.mnode, self.volname,
                                                 '50%')
        self.assertEqual(ret, 0, ("Failed to set quota default soft limit"))
        g.log.info("Quota default soft limit set successfully")

        # Check quota list to validate limits
        g.log.info("List all files and directories:")
        ret = quota_validate(self.mnode, self.volname, path=path,
                             soft_limit_percent=50, hard_limit=104857600)
        self.assertTrue(ret, "Failed to validate Quota list")
        g.log.info("Quota List successful")

        # Set alert time to 1 second
        g.log.info("Set quota alert timeout:")
        ret, _, _ = quota_set_alert_time(self.mnode, self.volname, '1sec')
        self.assertEqual(ret, 0, ("Failed to set alert timeout"))
        g.log.info("Quota alert time set successful")

        # Set soft timeout to 0 second
        g.log.info("Set quota soft timeout:")
        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, '0sec')
        self.assertEqual(ret, 0, ("Failed to set soft timeout"))
        g.log.info("Quota soft timeout set successful")

        # Set hard timeout to 0 second
        g.log.info("Set quota hard timeout:")
        ret, _, _ = quota_set_hard_timeout(self.mnode, self.volname, '0sec')
        self.assertEqual(ret, 0, ("Failed to set hard timeout"))
        g.log.info("Quota hard timeout set successful")

        # Get the brick log file path for a random node
        bricks = get_all_bricks(self.mnode, self.volname)
        selected_node, brick_path = random.choice(bricks[0:6]).split(':')
        brickpath = string.replace(brick_path, '/', '-')
        brickpathfinal = brickpath[1:]
        brick_log = "/var/log/glusterfs/bricks/%s.log" % brickpathfinal

        # Append unique string to the brick log
        g.log.info("Appending string 'appended_string_1' to the log:")
        append_string_to_file(selected_node, brick_log, "appended_string_1")

        # Starting IO on the mounts without crossing the soft limit
        # file creation should be normal
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s/foo ; "
               "for i in `seq 1 9` ; "
               "do dd if=/dev/urandom of=file$i "
               "bs=5M "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, ("Failed to create files on mountpoint"))
        g.log.info("Files created succesfully on mountpoint")

        # Append unique string to the brick log
        g.log.info("Appending string 'appended_string_2' to the log:")
        append_string_to_file(selected_node, brick_log, "appended_string_2")

        # Soft limit not crossed
        # Check if alert message is logged in the brick (shouldn't be logged)
        g.log.info("Check for alert message in logfile:")
        ret = search_pattern_in_file(selected_node, "120004",
                                     brick_log, "appended_string_1",
                                     "appended_string_2")
        self.assertFalse(ret, "Found unnecessary alert in logfile")
        g.log.info("Alert message not seen before crossing soft limit")

        # Continue IO on the mounts to exceed soft limit
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s/foo ; "
               "for i in `seq 1 200` ; "
               "do dd if=/dev/urandom of=foo$i "
               "bs=100K "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, ("Failed to create files on mountpoint"))
        g.log.info("Files created succesfully on mountpoint and "
                   "exceed soft limit")

        # Check if quota soft limit exceeded
        g.log.info("Check soft limit:")
        ret = quota_validate(self.mnode, self.volname, path=path,
                             sl_exceeded=True)
        self.assertTrue(ret, "Failed: soft limit not exceeded")
        g.log.info('Quota soft limit exceeded')

        # Inserting sleep of 2 seconds so the alert message gets enough time
        # to be logged
        time.sleep(2)

        # Append unique string to the brick log
        g.log.info("Appending string 'appended_string_3' to the log:")
        append_string_to_file(selected_node, brick_log, "appended_string_3")

        # Check if alert message logged in the brick
        g.log.info("Check for message:")
        ret = search_pattern_in_file(selected_node, "120004",
                                     brick_log, "appended_string_2",
                                     "appended_string_3")
        self.assertTrue(ret, "Alert message not found")
        g.log.info("Pattern Found: got alert message")

        # Append unique string to the brick log
        g.log.info("Appending string 'appended_string_4' to the log:")
        append_string_to_file(selected_node, brick_log, "appended_string_4")

        # Continue IO on the mounts by removing data
        g.log.info("Removing Files on %s:%s", client, mount_dir)
        cmd = ("rm -rfv %s/foo/foo* " % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, ("Failed to delete files on mountpoint"))
        g.log.info("Files removed succesfully from mountpoint and reached "
                   "below soft limit")

        # Check if quota soft limit exceeded
        g.log.info("Check soft limit:")
        ret = quota_validate(self.mnode, self.volname, path=path,
                             sl_exceeded=False)
        self.assertTrue(ret, "Failed: soft limit exceeded")
        g.log.info('Quota soft limit not exceeded')

        # Inserting sleep of 2 seconds so the alert message gets enough time
        # to be logged
        time.sleep(2)

        # Append unique string to the brick log
        g.log.info("Appending string 'appended_string_5' to the log:")
        append_string_to_file(selected_node, brick_log, "appended_string_5")

        # Check if alert message is logged in the brick
        g.log.info("Check for message:")
        ret = search_pattern_in_file(selected_node, "120004",
                                     brick_log, "appended_string_4",
                                     "appended_string_5")
        self.assertFalse(ret, "Found unnecessary alert message in logfile")
        g.log.info("Alert message not seen before crossing soft limit")

        # Continue IO on the mounts to exceed hard limit
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s/foo ; "
               "for i in `seq 11 20` ; "
               "do dd if=/dev/urandom of=file$i "
               "bs=10M "
               "count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 1, ("Failed: Files created successfully inspite "
                                  "of crossing hard-limit"))
        g.log.info("Files creation stopped on mountpoint once exceeded "
                   "hard limit")

        # Inserting sleep of 2 seconds so the alert message gets enough time
        # to be logged
        time.sleep(2)

        # Append unique string to the brick log
        g.log.info("Appending string 'appended_string_6' to the log:")
        append_string_to_file(selected_node, brick_log, "appended_string_6")

        # Check if alert message is logged in the brick
        g.log.info("Check for message:")
        ret = search_pattern_in_file(selected_node, "120004",
                                     brick_log, "appended_string_5",
                                     "appended_string_6")
        self.assertTrue(ret, "Alert message not seen in logfile")
        g.log.info("Pattern Found: got alert message")

        # Append unique string to the brick log
        g.log.info("Appending string 'Done_with_alert_check_7' to the log:")
        append_string_to_file(selected_node, brick_log,
                              "Done_with_alert_check_7")

        # Continue IO on the mounts by removing data to come below hard limit
        g.log.info("Removing Files on %s:%s", client, mount_dir)
        cmd = ("rm -rfv %s/foo/file{11..20}" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, ("Failed to delete files on mountpoint"))
        g.log.info("Files removed succesfully on mountpoint and "
                   "reached below soft limit")

        # Inserting sleep of 2 seconds so the alert message gets enough time
        # to be logged
        time.sleep(2)

        # Append unique string to the brick log
        g.log.info("Appending string 'Done_with_alert_check_8' to the log:")
        append_string_to_file(selected_node, brick_log,
                              "Done_with_alert_check_8")

        # Check if alert message is logged in the brick
        g.log.info("Check for message:")
        ret = search_pattern_in_file(selected_node, "120004",
                                     brick_log, "Done_with_alert_check_7",
                                     "Done_with_alert_check_8")
        self.assertFalse(ret, "Found unnecessary alert in logfile")
        g.log.info("EXPECTED: Alert message not seen before crossing "
                   "soft limit")

        # have got below the soft and hard limit
        # check quota list
        g.log.info("List all files and directories:")
        ret = quota_validate(self.mnode, self.volname, path=path,
                             sl_exceeded=False, hl_exceeded=False)
        self.assertTrue(ret, "Failed to validate Quota list with "
                             "soft and hard limits")
        g.log.info("Quota List validated successfully")
