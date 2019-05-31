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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_set_hard_timeout,
                                          quota_set_soft_timeout,
                                          quota_limit_usage)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline,
                                           bring_bricks_online)
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.rebalance_ops import rebalance_start


@runs_on([['distributed-replicated'],
          ['glusterfs', 'nfs']])
class TestQuotaRebalanceHeal(GlusterBaseClass):
    """
        Test if the quota limits are honored while a rebalance and
        heal is in progress.
    """
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [script_local_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        self.all_mounts_procs = []

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
        GlusterBaseClass.tearDown.im_func(self)

    def test_quota_rebalance_heal(self):
        """
        * Enable quota on the volume
        * set hard and soft time out to zero.
        * Create some files and directories from mount point
           so that the limits are reached.
        * Perform add-brick operation on the volume.
        * Start rebalance on the volume.
        * While rebalance is running, kill one of the bricks of the volume
          and start after a while.
        * While rebalance + self heal is in progress,
          create some more files and
          directories from the mount point until limit is hit
        """

        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Set the Quota timeouts to 0 for strict accounting
        ret, _, _ = quota_set_hard_timeout(self.mnode, self.volname, 0)
        self.assertEqual(ret, 0, ("Failed to set softtimeout ot 0 for %s",
                                  self.volname))

        ret, _, _ = quota_set_soft_timeout(self.mnode, self.volname, 0)
        self.assertEqual(ret, 0, ("Failed to set softtimeout ot 0 for %s",
                                  self.volname))
        g.log.info("soft and hard timeout has been set to 0 for %s",
                   self.volname)

        # Create Directories and files (write 4MB of data)
        for mount_object in self.mounts:
            g.log.info("Creating Directories on %s:%s",
                       mount_object.client_system, mount_object.mountpoint)
            cmd = ('python %s create_deep_dirs_with_files -d 0 -f 1024 -l 4'
                   ' --fixed-file-size 1k %s'
                   % (self.script_upload_path, mount_object.mountpoint))

            proc = g.run_async(mount_object.client_system, cmd,
                               user=mount_object.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")

        # Set limit of 4 MB on root dir
        g.log.info("Set Quota Limit on root directory of the volume %s",
                   self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname, '/', '4MB')
        self.assertEqual(ret, 0, "Failed to set Quota for dir /.")
        g.log.info("Set quota for dir / successfully.")

        # Add bricks
        replica_count_of_volume = self.volume['voltype']['replica_count']
        bricks_list = form_bricks_list(self.mnode, self.volname,
                                       replica_count_of_volume, self.servers,
                                       self.all_servers_info)
        g.log.info("new brick list: %s", str(bricks_list))
        ret, _, _ = add_brick(self.mnode, self.volname,
                              bricks_list, False)
        self.assertEqual(ret, 0, "Failed to add the bricks to the volume")
        g.log.info("Successfully added bricks to volume")

        # Perform rebalance start operation
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, "Rebalance start is success")

        # Sleep until rebalance has done some work
        g.log.info("wait for rebalance to make progress")
        sleep(3)

        # Kill a brick and bring it up to trigerr self heal
        all_bricks = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(all_bricks, "unable to get list of bricks")
        g.log.info("bringing down brick: %s", all_bricks[0])
        ret = bring_bricks_offline(self.volname, all_bricks[0])
        self.assertTrue(ret, "unable to bring brick1 offline")
        g.log.info("Successfully brought the following brick offline "
                   ": %s", str(all_bricks[0]))

        ret = bring_bricks_online(
            self.mnode, self.volname,
            [all_bricks[0]])
        self.assertTrue(ret, "unable to bring %s online" % all_bricks[0])
        g.log.info("Successfully brought the following brick online "
                   ": %s", str(all_bricks[0]))

        # Do some more IO and check if hard limit is honoured
        all_mounts_procs = []
        for mount_object in self.mounts:
            cmd = ("python %s create_files "
                   "-f 100 --base-file-name file %s"
                   % (self.script_upload_path, mount_object.mountpoint))
            proc = g.run_async(mount_object.client_system, cmd,
                               user=mount_object.user)
            all_mounts_procs.append(proc)

        # Validate I/O
        g.log.info("Wait for IO to complete and validate IO.....")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertFalse(ret, "Writes allowed past quota limit")
        g.log.info("Quota limits honored as expected")
