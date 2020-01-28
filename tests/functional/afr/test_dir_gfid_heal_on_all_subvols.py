#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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
"""
Description:
    Test cases in this module tests whether directory with null gfid
    is getting the gfids assigned on both the subvols of a dist-rep
    volume when lookup comes on that directory from the mount point.
"""


from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterfile import (get_fattr, delete_fattr)
from glustolibs.io.utils import get_mounts_stat


@runs_on([['replicated', 'distributed-replicated', 'distributed'],
          ['glusterfs']])
class AssignGfidsOnAllSubvols(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        # Setup Volume and Mount Volume
        ret = cls.setup_volume_and_mount_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    @classmethod
    def tearDownClass(cls):

        # Cleanup Volume
        ret = cls.unmount_volume_and_cleanup_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to create volume")
        g.log.info("Successful in cleaning up Volume %s", cls.volname)

        cls.get_super_method(cls, 'tearDownClass')()

    def verify_gfid_and_retun_gfid(self, dirname):
        dir_gfids = dict()
        bricks_list = get_all_bricks(self.mnode, self.volname)
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")

            ret = get_fattr(brick_node, '%s/%s' % (brick_path, dirname),
                            'trusted.gfid')
            if ret is not None:
                self.assertIsNotNone(ret, "trusted.gfid is not present on"
                                     "%s/%s" % (brick, dirname))
                dir_gfids.setdefault(dirname, []).append(ret)
                for key in dir_gfids:
                    self.assertTrue(all(value == dir_gfids[key][0]
                                        for value in dir_gfids[key]),
                                    "gfid mismatch for %s" % dirname)
                    dir_gfid = dir_gfids.values()[0]
        return dir_gfid

    def test_dir_gfid_heal_on_all_subvols(self):
        """
        - Create a volume and mount it.
        - Create a directory on mount and check whether all the bricks have
          the same gfid.
        - Now delete gfid attr from all but one backend bricks,
        - Do lookup from the mount.
        - Check whether all the bricks have the same gfid assigned.
        """

        # Create a directory on the mount
        cmd = ("/usr/bin/env python %s create_deep_dir -d 0 -l 0 "
               "%s/dir1" % (self.script_upload_path,
                            self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "Failed to create directory on mountpoint")
        g.log.info("Directory created successfully on mountpoint")

        # Verify gfids are same on all the bricks and get dir1 gfid
        bricks_list = get_all_bricks(self.mnode, self.volname)[1:]
        dir_gfid = self.verify_gfid_and_retun_gfid("dir1")

        # Delete gfid attr from all but one backend bricks
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            ret = delete_fattr(brick_node, '%s/dir1' % (brick_path),
                               'trusted.gfid')
            self.assertTrue(ret, 'Failed to delete gfid for brick '
                            'path %s:%s/dir1' % (brick_node, brick_path))
            g.log.info("Successfully deleted gfid xattr for %s:%s/dir1",
                       brick_node, brick_path)
        g.log.info("Successfully deleted gfid xattr for dir1 on the "
                   "following bricks %s", str(bricks_list[1:]))

        # Trigger heal from mount point
        sleep(10)
        for mount_obj in self.mounts:
            g.log.info("Triggering heal for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            command = ('cd %s; ls -l' % mount_obj.mountpoint)
            ret, _, _ = g.run(mount_obj.client_system, command)
            self.assertFalse(ret, 'Failed to run lookup '
                             'on %s ' % mount_obj.client_system)
            sleep(10)

        ret = get_mounts_stat(self.mounts)
        self.assertTrue(ret, "Failed to stat lookup on clients")
        g.log.info('stat lookup on clients succeeded')

        # Verify that all gfids for dir1 are same and get the gfid
        dir_gfid_new = self.verify_gfid_and_retun_gfid("dir1")
        self.assertTrue(all(gfid in dir_gfid for gfid in dir_gfid_new),
                        'Previous gfid and new gfid are not equal, '
                        'which is not expected, previous gfid %s '
                        'and new gfid %s' % (dir_gfid, dir_gfid_new))
        g.log.info('gfid heal was successful from client lookup and all '
                   'backend bricks have same gfid xattr, no gfid mismatch')
