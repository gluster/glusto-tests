#  Copyright (C) 2015-2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.brick_libs import (get_all_bricks,
                                           bring_bricks_offline,
                                           are_bricks_offline,
                                           bring_bricks_online)
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.volume_libs import get_volume_type_info
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete,
                                          is_volume_in_split_brain)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.io.utils import (validate_io_procs,
                                 wait_for_io_to_complete,
                                 collect_mounts_arequal)


@runs_on([['distributed'],
          ['glusterfs']])
class TestSelfHeal(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        if cls.volume_type == "distributed":
            # Define x1 distributed volume
            cls.volume['voltype'] = {
                'type': 'distributed',
                'dist_count': 1,
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

    def test_dist_to_repl_automatic_heal_should_be_triggered(self):
        """
        - create a single brick volume
        - add some files and directories
        - get arequal from mountpoint
        - add-brick such that this brick makes the volume a replica vol 1x2
        - make sure heal is completed
        - get arequals from all bricks and compare with arequal from mountpoint
        - bring down brick 0
        - create new files and validate IO
        - bring brick 0 up
        - make sure heal is completed
        """
        # pylint: disable=too-many-statements,too-many-locals
        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dir-length 1 "
                   "--dir-depth 1 "
                   "--max-num-of-dirs 1 "
                   "--num-of-files 10 %s" % (
                       self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            g.log.info("IO on %s:%s is started successfully",
                       mount_obj.client_system, mount_obj.mountpoint)
        self.io_validation_complete = False

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Get arequal for mount before adding bricks
        g.log.info('Getting arequal before adding bricks...')
        ret, arequals = collect_mounts_arequal(self.mounts)
        self.assertTrue(ret, 'Failed to get arequal')
        g.log.info('Getting arequal after healing is successful')
        mount_point_total = arequals[0].splitlines()[-1].split(':')[-1]

        # Form brick list to add
        g.log.info('Forming brick list to add...')
        bricks_to_add = form_bricks_list(self.mnode, self.volname, 1,
                                         self.servers, self.all_servers_info)
        g.log.info('Brick list to add: %s', bricks_to_add)

        # Add bricks
        g.log.info("Start adding bricks to volume...")
        ret, _, _ = add_brick(self.mnode, self.volname, bricks_to_add,
                              force=True, replica_count=2)
        self.assertFalse(ret, "Failed to add bricks %s" % bricks_to_add)
        g.log.info("Adding bricks is successful on volume %s", self.volname)

        # Make sure the newly added bricks are available in the volume
        # get the bricks for the volume
        g.log.info("Fetching bricks for the volume: %s", self.volname)
        bricks_list = get_all_bricks(self.mnode, self.volname)
        g.log.info("Brick list: %s", bricks_list)
        for brick in bricks_to_add:
            self.assertIn(brick, bricks_list,
                          'Brick %s is not in brick list' % brick)
        g.log.info('New bricks are present in the volume')

        # Make sure volume change from distribute to replicate volume
        vol_info_dict = get_volume_type_info(self.mnode, self.volname)
        vol_type = vol_info_dict['volume_type_info']['typeStr']
        self.assertEqual('Replicate', vol_type,
                         'Volume type is not converted to Replicate '
                         'after adding bricks')
        g.log.info('Volume type is successfully converted to Replicate '
                   'after adding bricks')

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

        # Get arequal on bricks and compare with mount_point_total
        # It should be the same
        g.log.info('Getting arequal on bricks...')
        arequals_after_heal = {}
        for brick in bricks_list:
            g.log.info('Getting arequal on bricks %s...', brick)
            node, brick_path = brick.split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan'
                       % brick_path)
            ret, arequal, _ = g.run(node, command)
            self.assertFalse(ret, 'Failed to get arequal on brick %s'
                             % brick)
            g.log.info('Getting arequal for %s is successful', brick)
            brick_total = arequal.splitlines()[-1].split(':')[-1]
            arequals_after_heal[brick] = brick_total
            self.assertEqual(mount_point_total, brick_total,
                             'Arequals for mountpoint and %s are not equal'
                             % brick)
            g.log.info('Arequals for mountpoint and %s are equal', brick)
        g.log.info('All arequals are equal for replicated')

        # Bring brick 0 offline
        g.log.info('Bringing bricks %s offline...', bricks_list[0])
        ret = bring_bricks_offline(self.volname, [bricks_list[0]])
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_list[0])

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [bricks_list[0]])
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_list[0])
        g.log.info('Bringing bricks %s offline is successful', bricks_list[0])

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_files -f 100 "
                   "--fixed-file-size 1k %s" % (
                       self.script_upload_path,
                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            g.log.info("IO on %s:%s is started successfully",
                       mount_obj.client_system, mount_obj.mountpoint)
        self.io_validation_complete = False

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Bring brick 0 online
        g.log.info('Bringing bricks %s online...', bricks_list[0])
        ret = bring_bricks_online(self.mnode, self.volname,
                                  [bricks_list[0]])
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_list[0])
        g.log.info('Bringing bricks %s online is successful', bricks_list[0])

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
