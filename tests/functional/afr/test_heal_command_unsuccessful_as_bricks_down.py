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
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           get_all_bricks)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.io.utils import (validate_io_procs,
                                 wait_for_io_to_complete)


@runs_on([['replicated'],
          ['glusterfs', 'cifs', 'nfs']])
class TestSelfHeal(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

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

        if cls.volume_type == "replicated":
            # Define x3 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp'}

    def setUp(self):
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        # If test method failed before validating IO, tearDown waits for the
        # IO's to complete and checks for the IO exit status
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_heal_command_unsuccessful_as_bricks_down(self):
        """
        - write 2 Gb file on mount
        - while write is in progress, kill brick b0
        - start heal on the volume (should fail and have error message)
        - bring up the brick which was down (b0)
        - bring down another brick (b1)
        - start heal on the volume (should fail and have error message)
        - bring bricks up
        - wait for heal to complete
        """
        # pylint: disable=too-many-statements
        bricks_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(bricks_list, 'Brick list is None')

        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create 2 Gb file
            g.log.info('Creating files...')
            command = ("cd %s ; dd if=/dev/zero of=file1  bs=10M  count=200"
                       % mount_obj.mountpoint)

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Bring brick0 offline
        g.log.info('Bringing bricks %s offline...', bricks_list[0])
        ret = bring_bricks_offline(self.volname, [bricks_list[0]])
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_list[0])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[0]])
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_list[0])
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_list[0])

        # Start healing
        # Need to use 'gluster volume heal' command to check error message
        # after g.run
        cmd = "gluster volume heal %s" % self.volname
        ret, _, err = g.run(self.mnode, cmd)
        self.assertTrue(ret, 'Heal is started')
        # Check for error message
        self.assertIn("Launching heal operation to perform index self heal on "
                      "volume %s has been unsuccessful" % self.volname,
                      err,
                      "Error message is not present or not valid")
        g.log.info('Expected: Healing is not started')

        # Bring brick0 online
        g.log.info("Bring bricks: %s online", bricks_list[0])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[0]])
        self.assertTrue(ret, "Failed to bring bricks: %s online"
                        % bricks_list[0])
        g.log.info("Successfully brought all bricks:%s online",
                   bricks_list[0])

        # Bring brick1 offline
        g.log.info('Bringing bricks %s offline...', bricks_list[1])
        ret = bring_bricks_offline(self.volname, [bricks_list[1]])
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_list[1])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[1]])
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_list[1])
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_list[1])

        # Start healing
        # Need to use 'gluster volume heal' command to check error message
        # after g.run
        cmd = "gluster volume heal %s" % self.volname
        ret, _, err = g.run(self.mnode, cmd)
        self.assertTrue(ret, 'Heal is started')
        # Check for error message
        self.assertIn("Launching heal operation to perform index self heal on "
                      "volume %s has been unsuccessful" % self.volname,
                      err,
                      "Error message is not present or not valid")
        g.log.info('Expected: Healing is not started')

        # Bring brick 1 online
        g.log.info("Bring bricks: %s online", bricks_list[1])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[1]])
        self.assertTrue(ret, "Failed to bring bricks: %s online"
                        % bricks_list[1])
        g.log.info("Successfully brought all bricks:%s online",
                   bricks_list[1])

        # Start healing
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not started')
        g.log.info('Healing is started')

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Check for split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertFalse(ret, 'Volume is in split-brain state')
        g.log.info('Volume is not in split-brain state')

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True
