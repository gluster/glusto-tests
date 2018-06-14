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

# pylint: disable=too-many-statements, too-many-locals, unused-variable

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_online,
                                           are_bricks_offline)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.heal_ops import (disable_self_heal_daemon,
                                         enable_self_heal_daemon,
                                         trigger_heal)
from glustolibs.gluster.heal_libs import (is_volume_in_split_brain,
                                          monitor_heal_completion)
from glustolibs.gluster.glusterfile import get_fattr


@runs_on([['replicated'], ['glusterfs']])
class TestSelfHeal(GlusterBaseClass):

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

        # Override Volumes
        if cls.volume_type == "replicated":
            # Define x2 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 2,
                'transport': 'tcp'}

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

    def test_afr_gfid_heal(self):

        """
        Description: This test case runs split-brain resolution
                     on a 5 files in split-brain on a 1x2 volume.
                     After resolving split-brain, it makes sure that
                     split brain resolution doesn't work on files
                     already in split brain.
        """

        g.log.info("disabling the self heal daemon")
        ret = disable_self_heal_daemon(self.mnode, self.volname)
        self.assertTrue(ret, "unable to disable self heal daemon")
        g.log.info("Successfully disabled the self heal daemon")

        # getting list of all bricks
        all_bricks = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(all_bricks, "failed to get list of bricks")
        g.log.info("bringing down brick1")
        ret = bring_bricks_offline(self.volname, all_bricks[0:1])
        self.assertTrue(ret, "unable to bring brick1 offline")
        g.log.info("Successfully brought the following brick offline "
                   ": %s", str(all_bricks[0]))
        g.log.info("verifying if brick1 is offline")
        ret = are_bricks_offline(self.mnode, self.volname, all_bricks[0:1])
        self.assertTrue(ret, "brick1 is still online")
        g.log.info("verified: brick1 is offline")

        g.log.info("creating 5 files from mount point")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 5 --base-file-name test_file --fixed-file-size 1k %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
        # Validate I/O
        g.log.info("Wait for IO to complete and validate IO.....")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")
        g.log.info("Successfully created a file from mount point")

        g.log.info("bringing brick 1 back online")
        ret = bring_bricks_online(self.mnode, self.volname, all_bricks[0:1])
        self.assertIsNotNone(ret, "unable to bring brick 1 online")
        g.log.info("Successfully brought the following brick online "
                   ": %s", str(all_bricks[0]))
        g.log.info("verifying if brick1 is online")
        ret = are_bricks_online(self.mnode, self.volname, all_bricks[0:1])
        self.assertTrue(ret, "brick1 is not online")
        g.log.info("verified: brick1 is online")

        g.log.info("bringing down brick2")
        ret = bring_bricks_offline(self.volname, all_bricks[1:2])
        self.assertTrue(ret, "unable to bring brick2 offline")
        g.log.info("Successfully brought the following brick offline "
                   ": %s", str(all_bricks[1]))
        g.log.info("verifying if brick2 is offline")
        ret = are_bricks_offline(self.mnode, self.volname, all_bricks[1:2])
        self.assertTrue(ret, "brick2 is still online")
        g.log.info("verified: brick2 is offline")

        g.log.info("creating 5 new files of same name from mount point")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 5 --base-file-name test_file --fixed-file-size 10k %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)
        # Validate I/O
        g.log.info("Wait for IO to complete and validate IO.....")
        ret = validate_io_procs(all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        g.log.info("IO is successful on all mounts")
        g.log.info("Successfully created a new file of same name "
                   "from mount point")

        g.log.info("bringing brick2 back online")
        ret = bring_bricks_online(self.mnode, self.volname, all_bricks[1:2])
        self.assertIsNotNone(ret, "unable to bring brick2 online")
        g.log.info("Successfully brought the following brick online "
                   ": %s", str(all_bricks[1]))
        g.log.info("verifying if brick2 is online")
        ret = are_bricks_online(self.mnode, self.volname, all_bricks[1:2])
        self.assertTrue(ret, "brick2 is not online")
        g.log.info("verified: brick2 is online")

        g.log.info("enabling the self heal daemon")
        ret = enable_self_heal_daemon(self.mnode, self.volname)
        self.assertTrue(ret, "failed to enable self heal daemon")
        g.log.info("Successfully enabled the self heal daemon")

        g.log.info("checking if volume is in split-brain")
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertTrue(ret, "unable to create split-brain scenario")
        g.log.info("Successfully created split brain scenario")

        g.log.info("resolving split-brain by choosing first brick as "
                   "the source brick")
        node, brick_path = all_bricks[0].split(':')
        for fcount in range(5):
            command = ("gluster v heal " + self.volname + " split-brain "
                       "source-brick " + all_bricks[0] + ' /test_file' +
                       str(fcount) + '.txt')
            ret, _, _ = g.run(node, command)
            self.assertEqual(ret, 0, "command execution not successful")
        # triggering heal
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, "heal not triggered")
        g.log.info("Successfully triggered heal")
        # waiting for heal to complete
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=240)
        self.assertTrue(ret, "heal not completed")
        g.log.info("Heal completed successfully")
        # checking if any file is in split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, "file still in split-brain")
        g.log.info("Successfully resolved split brain situation using "
                   "CLI based resolution")

        g.log.info("resolving split-brain on a file not in split-brain")
        node, brick_path = all_bricks[0].split(':')
        command = ("gluster v heal " + self.volname + " split-brain "
                   "source-brick " + all_bricks[1] + " /test_file0.txt")
        ret, _, _ = g.run(node, command)
        self.assertNotEqual(ret, 0, "Unexpected: split-brain resolution "
                                    "command is successful on a file which"
                                    " is not in split-brain")
        g.log.info("Expected: split-brian resolution command failed on "
                   "a file which is not in split-brain")

        g.log.info("checking the split-brain status of each file")
        for fcount in range(5):
            fpath = (self.mounts[0].mountpoint + '/test_file' +
                     str(fcount) + '.txt')
            status = get_fattr(self.mounts[0].client_system,
                               fpath, 'replica.split-brain-status')
            compare_string = ("The file is not under data or metadata "
                              "split-brain")
            self.assertEqual(status.rstrip('\x00'), compare_string,
                             "file test_file%s is under"
                             " split-brain" % str(fcount))
        g.log.info("none of the files are under split-brain")
