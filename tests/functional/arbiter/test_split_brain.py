#  Copyright (C) 2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           wait_for_bricks_to_be_online)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.heal_libs import is_volume_in_split_brain
from glustolibs.gluster.volume_libs import get_subvols


@runs_on([['arbiter', 'distributed-arbiter'], ['glusterfs']])
class TestSplitBrain(GlusterBaseClass):

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

        # Setup Volume and Mount Volume
        ret = cls.setup_volume_and_mount_volume(cls.mounts, True)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

    @classmethod
    def tearDownClass(cls):
        """
        Cleanup Volume
        """
        ret = cls.unmount_volume_and_cleanup_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to create volume")

        cls.get_super_method(cls, 'tearDownClass')()

    def _bring_bricks_online(self):
        """
        Bring bricks online and monitor heal completion
        """
        # Bring bricks online
        ret = bring_bricks_online(
            self.mnode,
            self.volname,
            self.bricks_to_bring_offline,
            bring_bricks_online_methods=['volume_start_force'])
        self.assertTrue(ret, 'Failed to bring bricks online')

        # Wait for volume processes to be online
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume {} processes to "
                              "be online".format(self.volname)))

    def test_split_brain(self):

        """
        Description: Create split-brain on files and check if IO's fail
        - Disable self-heal and cluster-quorum-type
        - Get the bricks from the volume
        - Write IO and validate IO
        - Bring 1st set of brick offline(1 Data brick and arbiter brick)
        - Write IO and validate IO
        - Bring 2nd set of bricks offline(1 Data brick and arbiter brick)
        - Write IO and validate IO
        - Check volume is in split-brain
        - Write IO and validate IO - should fail
        - Enable self-heal and cluster-quorum-type
        - Write IO and validate IO - should fail
        """
        # Disable self-heal and cluster-quorum-type
        options = {"self-heal-daemon": "off",
                   "cluster.quorum-type": "none"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for "
                              "volume %s" % (options, self.volname)))

        # Get the bricks from the volume
        sub_vols = get_subvols(self.mnode, self.volname)
        self.bricks_to_bring_offline = list(sub_vols['volume_subvols'][0])

        # Write IO's
        write_cmd = ("/usr/bin/env python %s create_files -f 1 "
                     "--base-file-name test_file --fixed-file-size 1k %s" %
                     (self.script_upload_path,
                      self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, write_cmd)

        # Bring 1st set of brick offline(1 Data brick and arbiter brick)
        for bricks in ((0, -1), (1, -1)):
            down_bricks = []
            for brick in bricks:
                down_bricks.append(self.bricks_to_bring_offline[brick])
            ret = bring_bricks_offline(self.volname, down_bricks)
            self.assertTrue(ret, 'Failed to bring bricks {} offline'.
                            format(down_bricks))
            proc = g.run_async(self.mounts[0].client_system, write_cmd)

            # Validate I/O
            self.assertTrue(
                validate_io_procs([proc], self.mounts),
                "IO failed on some of the clients"
            )

            # Bring bricks online
            self._bring_bricks_online()

        # Check volume is in split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertTrue(ret, "unable to create split-brain scenario")
        g.log.info("Successfully created split brain scenario")

        # Write IO's
        proc2 = g.run_async(self.mounts[0].client_system, write_cmd)

        # Validate I/O
        self.assertFalse(
            validate_io_procs([proc2], self.mounts),
            "IO passed on split-brain"
        )
        g.log.info("Expected - IO's failed due to split-brain")

        # Enable self-heal and cluster-quorum-type
        options = {"self-heal-daemon": "on",
                   "cluster.quorum-type": "auto"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for "
                              "volume %s" % (options, self.volname)))

        # Write IO's
        proc3 = g.run_async(self.mounts[0].client_system, write_cmd)

        # Validate I/O
        self.assertFalse(
            validate_io_procs([proc3], self.mounts),
            "IO passed on split-brain"
        )
        g.log.info("Expected - IO's failed due to split-brain")
