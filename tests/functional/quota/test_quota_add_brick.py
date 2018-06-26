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
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
from glustolibs.gluster.quota_ops import (quota_enable,
                                          quota_set_soft_timeout,
                                          quota_set_hard_timeout,
                                          quota_limit_usage)
from glustolibs.gluster.quota_libs import quota_validate
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.lib_utils import form_bricks_list


@runs_on([['distributed-replicated', 'replicated', 'distributed',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs', 'nfs']])
class QuotaAddBrick(GlusterBaseClass):

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

    def test_quota_add_brick(self):
        """
        Verifying quota functionality with respect to the
        add-brick without rebalance

        * Enable Quota
        * Set limit of 1GB on /
        * Mount the volume
        * Create some random amount of data inside each directory until quota
          is reached
        * Perform a quota list operation
        * Perform add-brick
        * Trying add files and see if quota is honored.
        """
        # Enable Quota
        g.log.info("Enabling quota on the volume %s", self.volname)
        ret, _, _ = quota_enable(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to enable quota on the volume %s",
                                  self.volname))
        g.log.info("Successfully enabled quota on the volume %s", self.volname)

        # Path to set quota limit
        path = "/"

        # Set Quota limit on the root of the volume
        g.log.info("Set Quota Limit on the path %s of the volume %s",
                   path, self.volname)
        ret, _, _ = quota_limit_usage(self.mnode, self.volname,
                                      path=path, limit="1GB")
        self.assertEqual(ret, 0, ("Failed to set quota limit on path %s of "
                                  "the volume %s", path, self.volname))
        g.log.info("Successfully set the Quota limit on %s of the volume %s",
                   path, self.volname)

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

        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        # Create data inside each directory from mount point
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s/ ; "
               "for i in `seq 100` ; "
               "do dd if=/dev/zero of=testfile1$i "
               "bs=10M "
               "count=1 ; "
               "done"
               % (mount_dir))
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, ("Failed to create files"))
        g.log.info("Files created succesfully")

        # Quota validate
        ret = quota_validate(self.mnode, self.volname,
                             path=path, hard_limit=1073741824,
                             sl_exceeded=True, hl_exceeded=False)
        self.assertTrue(ret, "Quota validate Failed for /")

        # Add brick by forming the brick list
        # Form bricks list for add-brick command based on the voltype
        if 'replica_count' in self.volume['voltype']:
            new_bricks_count = self.volume['voltype']['replica_count']
        elif 'disperse_count' in self.volume['voltype']:
            new_bricks_count = self.volume['voltype']['disperse_count']
        else:
            new_bricks_count = 3
        bricks_list = form_bricks_list(self.mnode, self.volname,
                                       new_bricks_count, self.servers,
                                       self.all_servers_info)
        g.log.info("new brick list: %s", bricks_list)
        # Run add brick command
        ret, _, _ = add_brick(self.mnode, self.volname,
                              bricks_list, False)
        self.assertEqual(ret, 0, "Failed to add the bricks to the volume")
        g.log.info("Successfully added bricks to volume")

        # Create data inside each directory from mount point
        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s/ ; "
               "for i in `seq 50` ; "
               "do dd if=/dev/zero of=testfile2$i "
               "bs=1M "
               "count=1 ; "
               "done"
               % (mount_dir))
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 1, ("Failed: Files created successfully"))
        g.log.info("Quota limit honored")

        # Quota validate
        ret = quota_validate(self.mnode, self.volname,
                             path=path, hard_limit=1073741824,
                             sl_exceeded=True, hl_exceeded=True)
        self.assertTrue(ret, "Quota validate Failed for /")
