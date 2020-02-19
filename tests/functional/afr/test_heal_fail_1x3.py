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

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline,
                                           bring_bricks_online)
from glustolibs.gluster.glusterfile import get_file_stat
from glustolibs.gluster.heal_libs import is_heal_complete
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs


@runs_on([['replicated'], ['glusterfs', 'nfs', 'cifs']])
class TestSelfHeal(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

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

        # Override Volumes
        if cls.volume_type == "replicated":
            # Define x3 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp'}

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(cls.mounts, True)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Cleanup Volume
        g.log.info("Starting to clean up Volume %s", self.volname)
        ret = self.unmount_volume_and_cleanup_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to create volume")
        g.log.info("Successful in cleaning up Volume %s", self.volname)

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_heal_gfid_1x3(self):

        """
        Description: This test case verifies the gfid self-heal on a 1x3
                 replicate volume.
                 1. file created at mount point
                 2. 2 bricks brought down
                 3. file deleted
                 4. created a new file from the mount point
                 5. all bricks brought online
                 6. check if gfid worked correctly
        """

        g.log.info("setting the quorum type to fixed")
        options = {"cluster.quorum-type": "fixed"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, "unable to set the quorum type to fixed")
        g.log.info("Successfully set the quorum type to fixed")

        g.log.info("creating a file from mount point")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_files -f 1 "
                   "--base-file-name test_file --fixed-file-size 10k %s" % (
                       self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
        # Validate I/O
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        g.log.info("Successfully created a file from mount point")

        # getting list of all bricks
        all_bricks = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(all_bricks, "unable to get list of bricks")
        g.log.info("bringing down brick1 and brick2")
        ret = bring_bricks_offline(self.volname, all_bricks[:2])
        self.assertTrue(ret, "unable to bring bricks offline")
        g.log.info("Successfully brought the following bricks offline "
                   ": %s", str(all_bricks[:2]))

        g.log.info("deleting the file from mount point")
        command = "rm -f " + self.mounts[0].mountpoint + "/test_file1"
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "unable to remove file from mount point")
        g.log.info("Successfully deleted file from mountpoint")

        g.log.info("creating a new file of same name and different size "
                   "from mount point")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("/usr/bin/env python %s create_files -f 1 "
                   "--base-file-name test_file --fixed-file-size 1M %s" % (
                       self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
        # Validate I/O
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        g.log.info("Successfully created a new file of same name "
                   "from mount point")

        g.log.info("bringing bricks 1 and 2 back online")
        ret = bring_bricks_online(self.mnode, self.volname, all_bricks[:2])
        self.assertIsNotNone(ret, "unable to bring bricks online")
        g.log.info("Successfully brought the following bricks online "
                   ": %s", str(all_bricks[:2]))

        g.log.info("checking if stat structure of the file is returned")
        ret = get_file_stat(self.mounts[0].client_system,
                            self.mounts[0].mountpoint+'/test_file0.txt')
        self.assertTrue(ret, "unable to get file stats")
        g.log.info("file stat structure returned successfully")

        g.log.info("checking if the heal has completed")
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, "heal not completed")
        g.log.info("Self heal was completed successfully")

        g.log.info("checking if the areequal checksum of all the bricks in "
                   "the subvol match")
        checksum_list = []
        for brick in all_bricks:
            node, brick_path = brick.split(':')
            command = "arequal-checksum -p " + brick_path + \
                      " -i .glusterfs -i .landfill"
            ret, out, _ = g.run(node, command)
            self.assertEqual(ret, 0, "unable to get the arequal checksum "
                                     "of the brick")
            checksum_list.append(out)
            # checking file size of healed file on each brick to verify
            # correctness of choice for sink and source
            stat_dict = get_file_stat(node, brick_path + '/test_file0.txt')
            self.assertEqual(stat_dict['size'], '1048576',
                             "file size of healed file is different "
                             "than expected")
        flag = all(val == checksum_list[0] for val in checksum_list)
        self.assertTrue(flag, "the arequal checksum of all bricks is"
                        "not same")
        g.log.info("the arequal checksum of all the bricks in the subvol "
                   "is same")
