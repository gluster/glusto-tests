#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable=too-many-statements, too-many-locals

""" Description:
        Test cases in this module tests whether SHD heals the
        files in a directory when directory quota is exceeded.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           are_bricks_online)
from glustolibs.gluster.heal_libs import (is_heal_complete,
                                          monitor_heal_completion)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_limit_usage,
                                          quota_set_soft_timeout,
                                          quota_set_hard_timeout)


@runs_on([['distributed-replicated', 'replicated'],
          ['glusterfs']])
class HealFilesWhenDirQuotaExceeded(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Override Volumes
        if cls.volume_type == "replicated":
            # Define 1x3 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp'}

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, script_local_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    @classmethod
    def tearDownClass(cls):

        # Cleanup Volume
        g.log.info("Starting to clean up Volume %s", cls.volname)
        ret = cls.unmount_volume_and_cleanup_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to create volume")
        g.log.info("Successful in cleaning up Volume %s", cls.volname)

        GlusterBaseClass.tearDownClass.im_func(cls)

    def test_heal_when_dir_quota_exceeded_(self):
        # Create a directory to set the quota_limit_usage
        path = "/dir"
        g.log.info("Creating a directory")
        self.all_mounts_procs = []
        for mount_object in self.mounts:
            cmd = ("python %s create_deep_dir -d 0 -l 0 %s%s "
                   % (self.script_upload_path, mount_object.mountpoint,
                      path))
            ret = g.run(mount_object.client_system, cmd)
            self.assertTrue(ret, "Failed to create directory on mountpoint")
            g.log.info("Directory created succesfully on mountpoint")

        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully enabled quota on the volume %s",
                   self.volname)

        # Set quota-soft-timeout to 0
        g.log.info("Setting up soft timeout to 0")
        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, "0")
        self.assertEqual(ret, 0, ("Failed to set quota-soft-timeout"))
        g.log.info("Successfully set the quota-soft-timeout")

        # Set quota-hard-timeout to 0
        g.log.info("Setting up hard timeout with 0")
        ret, _, _ = quota_set_hard_timeout(self.mnode, self.volname, "0")
        self.assertEqual(ret, 0, ("Failed to set quota-hard-timeout"))
        g.log.info("successfully set the quota-hard-timeout")

        # Set Quota limit on the newly created directory
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="1GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  " the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume "
                   "%s", path, self.volname)

        # Create 2 files of size 400MB inside the directory
        for mount_object in self.mounts:
            g.log.info("Creating Files on %s:%s", mount_object.client_system,
                       path)
            cmd = ("cd %s%s && for i in `seq 1 2` ;"
                   "do dd if=/dev/urandom of=file$i bs=20M "
                   "count=20; done" % (mount_object.mountpoint, path))
            ret, _, _ = g.run(mount_object.client_system, cmd)
            self.assertEqual(ret, 0, ("Failed to create files on %s", path))
            g.log.info("Files created succesfully on mountpoint")

        bricks_list = get_all_bricks(self.mnode, self.volname)

        # Bring brick2 offline
        g.log.info('Bringing bricks %s offline', bricks_list[2])
        ret = bring_bricks_offline(self.volname, bricks_list[2])
        self.assertTrue(ret, 'Failed to bring bricks %s offline'
                        % bricks_list[2])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[2]])
        self.assertTrue(ret, 'Brick %s is not offline'
                        % bricks_list[2])
        g.log.info('Bringing brick %s offline is successful',
                   bricks_list[2])

        # Create a file of size 500MB inside the directory and it should fail
        # as the quota limit exceeds
        cmd = ("cd %s%s && dd if=/dev/urandom of=file3 bs=20M count=25"
               % (mount_object.mountpoint, path))
        ret, _, _ = g.run(mount_object.client_system, cmd)
        self.assertEqual(ret, 1, ("Writing a file of 500MB succeeded while "
                                  "it was not supposed to."))
        g.log.info("Writing a file of size 500MB failed as expected "
                   "due to quota limit on the directory.")

        # Bring brick2 online and check status
        g.log.info('Bringing brick %s online...', bricks_list[2])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[2]])
        self.assertTrue(ret, 'Failed to bring brick %s online' %
                        bricks_list[2])
        g.log.info('Bringing brick %s online is successful', bricks_list[2])

        g.log.info("Verifying if brick %s is online", bricks_list[2])
        ret = are_bricks_online(self.mnode, self.volname, bricks_list)
        self.assertTrue(ret, ("Brick %s did not come up", bricks_list[2]))
        g.log.info("Brick %s has come online.", bricks_list[2])

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
