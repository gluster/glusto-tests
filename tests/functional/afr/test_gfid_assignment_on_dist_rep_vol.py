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
        Test cases in this module tests whether directory with null gfid
        is getting the gfids assigned on both the subvols of a dist-rep
        volume when lookup comes on that directory from the mount point.
"""

import time
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterfile import get_fattr
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['distributed-replicated'],
          ['glusterfs']])
class AssignGfidsOnAllSubvols(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

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

    def verify_gfid(self, dirname):
        dir_gfids = dict()
        bricks_list = get_all_bricks(self.mnode, self.volname)
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")

            ret = get_fattr(brick_node, '%s/%s' % (brick_path, dirname),
                            'trusted.gfid')
            self.assertIsNotNone(ret, "trusted.gfid is not present on"
                                 "%s/%s" % (brick, dirname))
            dir_gfids.setdefault(dirname, []).append(ret)

            for key in dir_gfids:
                self.assertTrue(all(value == dir_gfids[key][0]
                                    for value in dir_gfids[key]),
                                "gfid mismatch for %s" % dirname)

    def test_gfid_assignment_on_all_subvols(self):
        """
        - Create a dis-rep volume and mount it.
        - Create a directory on mount and check whether all the bricks have
          the same gfid.
        - On the backend create a new directory on all the bricks.
        - Do lookup from the mount.
        - Check whether all the bricks have the same gfid assigned.
        """
        # Enable client side healing
        g.log.info("Enable client side healing options")
        options = {"metadata-self-heal": "on",
                   "entry-self-heal": "on",
                   "data-self-heal": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # Create a directory on the mount
        g.log.info("Creating a directory")
        cmd = ("python %s create_deep_dir -d 0 -l 0 %s/dir1 "
               % (self.script_upload_path, self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to create directory on mountpoint")
        g.log.info("Directory created successfully on mountpoint")

        # Verify gfids are same on all the bricks
        self.verify_gfid("dir1")

        # Create a new directory on all the bricks directly
        bricks_list = get_all_bricks(self.mnode, self.volname)
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")

            ret, _, _ = g.run(brick_node, "mkdir %s/dir2" % (brick_path))
            self.assertEqual(ret, 0, "Failed to create directory on brick %s"
                             % (brick))

        # To circumvent is_fresh_file() check in glusterfs code.
        time.sleep(2)

        # Do a clinet side lookup on the new directory and verify the gfid
        # All the bricks should have the same gfid assigned
        ret, _, _ = g.run(self.clients[0], "ls %s/dir2"
                          % self.mounts[0].mountpoint)
        self.assertEqual(ret, 0, "Lookup on directory \"dir2\" failed.")
        g.log.info("Lookup on directory \"dir2\" successful")

        # Verify gfid is assigned on all the bricks and are same
        self.verify_gfid("dir2")
