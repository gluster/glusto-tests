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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online,
                                           are_bricks_offline,
                                           wait_for_bricks_to_be_online,
                                           get_all_bricks)
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.heal_ops import (enable_self_heal_daemon,
                                         trigger_heal)
from glustolibs.gluster.heal_libs import (
    is_volume_in_split_brain,
    is_heal_complete,
    wait_for_self_heal_daemons_to_be_online,
    monitor_heal_completion)
from glustolibs.gluster.glusterfile import GlusterFile


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs']])
class TestSelfHeal(GlusterBaseClass):
    """
    Description:
        Test cases related to
        healing in default configuration of the volume
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Override replica count to be 3
        if cls.volume_type == "replicated":
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'transport': 'tcp'}

        if cls.volume_type == "distributed-replicated":
            cls.volume['voltype'] = {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 3,
                'transport': 'tcp'}

    def setUp(self):
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts,
                                                 volume_create_force=False)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        self.bricks_list = get_all_bricks(self.mnode, self.volname)

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

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

    def toggle_bricks_and_perform_io(self, file_list, brick_list):
        """
        Kills bricks, does I/O and brings the brick back up.
        """
        # Bring down bricks.
        g.log.info("Going to bring down the brick process for %s", brick_list)
        ret = bring_bricks_offline(self.volname, brick_list)
        self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                              "check the log file for more details."))
        g.log.info("Brought down the brick process "
                   "for %s successfully", brick_list)
        ret = are_bricks_offline(self.mnode, self.volname, brick_list)
        self.assertTrue(ret, 'Bricks %s are not offline' % brick_list)

        # Perform I/O
        for filename in file_list:
            fpath = self.mounts[0].mountpoint + "/test_gfid_split_brain/" + \
                    filename
            cmd = ("dd if=/dev/urandom of=%s bs=1024 count=1" % fpath)
            ret, _, _ = g.run(self.clients[0], cmd)
            self.assertEqual(ret, 0, "Creating %s failed" % fpath)

        # Bring up bricks
        ret = bring_bricks_online(self.mnode, self.volname, brick_list)
        self.assertTrue(ret, 'Failed to bring brick %s online' % brick_list)
        g.log.info('Bringing brick %s online is successful', brick_list)

        # Waiting for bricks to come online
        g.log.info("Waiting for brick process to come online")
        timeout = 30
        ret = wait_for_bricks_to_be_online(self.mnode, self.volname, timeout)
        self.assertTrue(ret, "bricks didn't come online after adding bricks")
        g.log.info("Bricks are online")

    def resolve_gfid_split_brain(self, filename, source_brick):
        """
        resolves gfid split-brain on files using source-brick option
        """
        node, _ = source_brick.split(':')
        command = ("gluster volume heal " + self.volname + " split-brain "
                   "source-brick " + source_brick + " " + filename)
        ret, _, _ = g.run(node, command)
        self.assertEqual(ret, 0, "command execution not successful")

    def test_gfid_split_brain_resolution(self):
        """
        - create gfid split-brain of files and resolves them using source-brick
          option of the CLI.
        """

        # pylint: disable=too-many-statements
        # pylint: disable=too-many-locals

        # Disable all self-heals and client-quorum
        options = {"self-heal-daemon": "off",
                   "data-self-heal": "off",
                   "metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "cluster.quorum-type": "none"}
        g.log.info("setting volume options %s", options)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for "
                              "volume %s" % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # Create dir inside which I/O will be performed.
        ret = mkdir(self.mounts[0].client_system, "%s/test_gfid_split_brain"
                    % self.mounts[0].mountpoint)
        self.assertTrue(ret, "mkdir failed")

        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s:", num_subvols)

        # Toggle bricks and perform I/O
        file_list = ["file1.txt", "file2.txt", "file3.txt", "file4.txt",
                     "file5.txt", "file6.txt", "file7.txt", "file8.txt",
                     "file9.txt", "file10.txt"]
        brick_index = 0
        offline_bricks = []
        for _ in range(0, 3):
            for i in range(0, num_subvols):
                subvol_brick_list = subvols_dict['volume_subvols'][i]
                offline_bricks.append(subvol_brick_list[brick_index % 3])
                offline_bricks.append(subvol_brick_list[(brick_index+1) % 3])
            self.toggle_bricks_and_perform_io(file_list, offline_bricks)
            brick_index += 1
            offline_bricks[:] = []

        # Enable shd
        g.log.info("enabling the self heal daemon")
        ret = enable_self_heal_daemon(self.mnode, self.volname)
        self.assertTrue(ret, "failed to enable self heal daemon")
        g.log.info("Successfully enabled the self heal daemon")

        # Wait for self heal processes to come online
        g.log.info("Wait for selfheal process to come online")
        timeout = 300
        ret = wait_for_self_heal_daemons_to_be_online(self.mnode, self.volname,
                                                      timeout)
        self.assertTrue(ret, "Self-heal process are not online")
        g.log.info("All self heal process are online")

        # Trigger heal
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Starting heal failed')
        g.log.info('Index heal launched')

        # checking if file is in split-brain
        ret = is_volume_in_split_brain(self.mnode, self.volname)
        self.assertTrue(ret, "Files are not in split-brain as expected.")
        g.log.info("Files are still in split-brain")

        # First brick of each replica will be used as source-brick
        first_brick_list = []
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            brick = subvol_brick_list[0]
            first_brick_list.append(brick)

        # Find which dht subvols the 10 files are present in and trigger heal
        for filename in file_list:
            fpath = self.mounts[0].mountpoint + "/test_gfid_split_brain/" + \
                    filename
            gfile = GlusterFile(self.clients[0], fpath)
            for brick in first_brick_list:
                _, brick_path = brick.split(':')
                match = [brick for item in gfile.hashed_bricks if brick_path
                         in item]
                if match:
                    self.resolve_gfid_split_brain("/test_gfid_split_brain/" +
                                                  filename, brick)

        # Trigger heal to complete pending data/metadata heals
        ret = trigger_heal(self.mnode, self.volname)
        self.assertTrue(ret, 'Starting heal failed')
        g.log.info('Index heal launched')

        # Monitor heal completion
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal has not yet completed')

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Get arequals and compare
        for i in range(0, num_subvols):
            # Get arequal for first brick
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            node, brick_path = subvol_brick_list[0].split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan'
                       % brick_path)
            ret, arequal, _ = g.run(node, command)
            first_brick_total = arequal.splitlines()[-1].split(':')[-1]

            # Get arequal for every brick and compare with first brick
            for brick in subvol_brick_list[1:]:
                node, brick_path = brick.split(':')
                command = ('arequal-checksum -p %s '
                           '-i .glusterfs -i .landfill -i .trashcan'
                           % brick_path)
                ret, brick_arequal, _ = g.run(node, command)
                self.assertFalse(ret,
                                 'Failed to get arequal on brick %s'
                                 % brick)
                g.log.info('Getting arequal for %s is successful', brick)
                brick_total = brick_arequal.splitlines()[-1].split(':')[-1]

                self.assertEqual(first_brick_total, brick_total,
                                 'Arequals for subvol and %s are not equal'
                                 % brick)
                g.log.info('Arequals for subvol and %s are equal', brick)
