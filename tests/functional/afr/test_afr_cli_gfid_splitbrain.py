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

import sys

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline,
                                           bring_bricks_online)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.heal_ops import (disable_self_heal_daemon,
                                         enable_self_heal_daemon,
                                         trigger_heal)
from glustolibs.gluster.heal_libs import (is_volume_in_split_brain,
                                          monitor_heal_completion)


@runs_on([['replicated'], ['glusterfs', 'cifs']])
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
            # Define x2 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 2,
                'transport': 'tcp'}

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = cls.setup_volume_and_mount_volume(cls.mounts, True)
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

        cls.get_super_method(cls, 'tearDownClass')()

    def test_afr_gfid_heal(self):

        """
        Description: This test case runs split-brain resolution CLIs
                     on a file in gfid split-brain on 1x2 volume.
                     1. kill 1 brick
                     2. create a file at mount point
                     3. bring back the killed brick
                     4. kill the other brick
                     5. create same file at mount point
                     6. bring back the killed brick
                     7. try heal from CLI and check if it gets completed
        """

        g.log.info("disabling the self heal daemon")
        ret = disable_self_heal_daemon(self.mnode, self.volname)
        self.assertTrue(ret, "unable to disable self heal daemon")
        g.log.info("Successfully disabled the self heal daemon")

        # getting list of all bricks
        all_bricks = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(all_bricks, "unable to get list of bricks")
        g.log.info("bringing down brick1")
        ret = bring_bricks_offline(self.volname, all_bricks[0])
        self.assertTrue(ret, "unable to bring %s offline" % all_bricks[0])
        g.log.info("Successfully brought the following brick offline "
                   ": %s", all_bricks[0])

        g.log.info("creating a file from mount point")
        all_mounts_procs = []
        cmd = ("/usr/bin/env python%d %s create_files "
               "-f 1 --base-file-name test_file --fixed-file-size 1k %s" % (
                   sys.version_info.major, self.script_upload_path,
                   self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd)
        all_mounts_procs.append(proc)
        # Validate I/O
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        g.log.info("Successfully created a file from mount point")

        g.log.info("bringing brick 1 back online")
        ret = bring_bricks_online(self.mnode, self.volname, [all_bricks[0]])
        self.assertIsNotNone(ret, "unable to bring %s online" % all_bricks[0])
        g.log.info("Successfully brought the following brick online "
                   ": %s", all_bricks[0])

        g.log.info("bringing down brick2")
        ret = bring_bricks_offline(self.volname, all_bricks[1])
        self.assertTrue(ret, "unable to bring %s offline" % all_bricks[0])
        g.log.info("Successfully brought the following brick offline "
                   ": %s", all_bricks[1])

        g.log.info("creating a new file of same name from mount point")
        all_mounts_procs = []
        cmd = ("/usr/bin/env python%d %s create_files "
               "-f 1 --base-file-name test_file --fixed-file-size 1k %s" % (
                   sys.version_info.major, self.script_upload_path,
                   self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd)
        all_mounts_procs.append(proc)
        # Validate I/O
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        g.log.info("Successfully created a new file of same name "
                   "from mount point")

        g.log.info("bringing brick2 back online")
        ret = bring_bricks_online(self.mnode, self.volname, [all_bricks[1]])
        self.assertIsNotNone(ret, "unable to bring %s online" % all_bricks[0])
        g.log.info("Successfully brought the following brick online "
                   ": %s", all_bricks[1])

        g.log.info("enabling the self heal daemon")
        ret = enable_self_heal_daemon(self.mnode, self.volname)
        self.assertTrue(ret, "failed to enable self heal daemon")
        g.log.info("Successfully enabled the self heal daemon")

        g.log.info("checking if file is in split-brain")
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertTrue(ret, "unable to create split-brain scenario")
        g.log.info("Successfully created split brain scenario")

        g.log.info("resolving split-brain by choosing second brick as "
                   "the source brick")
        node, _ = all_bricks[0].split(':')
        command = ("gluster volume heal %s split-brain source-brick %s "
                   "/test_file0.txt" % (self.volname, all_bricks[1]))
        ret, _, _ = g.run(node, command)
        self.assertEqual(ret, 0, "command execution not successful")
        # triggering heal
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, "heal not triggered")
        # waiting for heal to complete
        ret = monitor_heal_completion(self.mnode, self.volname,
                                      timeout_period=120)
        self.assertTrue(ret, "heal not completed")
        # checking if file is in split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, "file still in split-brain")
        g.log.info("Successfully resolved split brain situation using "
                   "CLI based resolution")
