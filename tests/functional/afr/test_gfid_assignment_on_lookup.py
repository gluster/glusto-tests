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

""" Description:
        Test cases in this module tests whether directories with null gfid
        are getting the gfids assigned and directories get created on the
        remaining bricks when named lookup comes on those from the mount point.
"""

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.heal_libs import is_heal_complete
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.glusterfile import get_fattr
from glustolibs.gluster.volume_ops import set_volume_options


@runs_on([['replicated'],
          ['glusterfs']])
class AssignGfidOnLookup(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

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
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):

        self.get_super_method(self, 'setUp')()
        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to create volume")
        g.log.info("Successful in cleaning up Volume %s", self.volname)

        self.get_super_method(self, 'tearDown')()

    def verify_gfid(self, dirname):
        dir_gfids = dict()
        bricks_list = get_all_bricks(self.mnode, self.volname)
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            ret = get_fattr(brick_node, '%s/%s' % (brick_path, dirname),
                            'trusted.gfid')
            self.assertIsNotNone(ret, "trusted.gfid is not present on %s/%s"
                                 % (brick_path, dirname))
            dir_gfids.setdefault(dirname, []).append(ret)
            for key in dir_gfids:
                self.assertTrue(all(value == dir_gfids[key][0]
                                    for value in dir_gfids[key]),
                                "gfid mismatch for %s" % dirname)

    def test_gfid_assignment_on_lookup(self):
        '''
        1) create replicate volume ( 1 * 3 )
        2. Test the case with default afr options.
        3. Test the case with volume option 'self-heal-daemon'
        4) create dirs on bricks from backend. lets say dir1, dir2 and dir3
        5) From mount point,
            echo "hi" >dir1 ->must fail
            touch dir2 --> must pass
            mkdir dir3 ->must fail
        6) From mount point,
            ls -l  and find, must list both dir1 and dir2 and dir3
        7) check on all backend bricks, dir1, dir2 and dir3 should be created
        8) heal info should show zero, and also gfid and other attributes
         must exist
        '''
        g.log.info("Enable client side healing options")
        options = {"metadata-self-heal": "on",
                   "entry-self-heal": "on",
                   "data-self-heal": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        g.log.info("Creating directories on the backend.")
        bricks_list = get_all_bricks(self.mnode, self.volname)
        i = 0
        for brick in bricks_list:
            i += 1
            brick_node, brick_path = brick.split(":")
            ret, _, _ = g.run(brick_node, "mkdir %s/dir%d" % (brick_path, i))
            self.assertEqual(ret, 0, "Dir creation failed on %s" % brick_path)
        g.log.info("Created directories on the backend.")

        # To circumvent is_fresh_file() check in glusterfs code.
        sleep(2)

        # Do named lookup on directories from mount
        ret, _, err = g.run(self.clients[0], "echo Hi >  %s/dir1"
                            % self.mounts[0].mountpoint)
        errmsg = ("bash: %s/dir1: Is a directory\n"
                  % self.mounts[0].mountpoint)
        msg = "expected %s, but returned %s" % (errmsg, err)
        self.assertEqual(err, errmsg, msg)
        g.log.info("Writing a file with same name as directory \"dir1\" failed"
                   " as expected on mount point.")

        ret, _, _ = g.run(self.clients[0], "touch %s/dir2"
                          % self.mounts[0].mountpoint)
        self.assertEqual(ret, 0, "Touch of file with same name as directory "
                         "\"dir2\" failed.")
        g.log.info("Touch of file with same name as directory \"dir2\" passed"
                   " but it will not create the file since a directory is "
                   "already present with the same name.")

        ret, _, err = g.run(self.clients[0], "mkdir %s/dir3"
                            % self.mounts[0].mountpoint)
        self.assertNotEqual(ret, 0, "Creation of directory with same name as "
                            "directory \"dir3\" succeeded, which is not "
                            "supposed to.")
        g.log.info("Creation of directory \"dir3\" failed as expected")

        g.log.info("Do a named lookup on dirs")
        for number in range(1, 4):
            ret, _, _ = g.run(self.clients[0], "ls %s/dir%s"
                              % (self.mounts[0].mountpoint, number))
            ret, _, _ = g.run(self.clients[0], "find %s/dir%s"
                              % (self.mounts[0].mountpoint, number))
        g.log.info("Named lookup Successful")

        # Check if heal is completed
        counter = 0
        while True:
            ret = is_heal_complete(self.mnode, self.volname)
            if ret or counter > 30:
                break
            counter += 1
            sleep(2)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Verify directories are present on the backend and gfids are assigned
        self.verify_gfid("dir1")
        self.verify_gfid("dir2")
        self.verify_gfid("dir3")

        # Check whether all the directories are listed on the mount
        _, count, _ = g.run(self.clients[0], "ls %s | wc -l"
                            % self.mounts[0].mountpoint)
        self.assertEqual(int(count), 3, "Not all the directories are listed on"
                         "the mount")
        g.log.info("All the directories are listed on the mount.")
