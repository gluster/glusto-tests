#  Copyright (C) 2021 Red Hat, Inc. <http://www.redhat.com>
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
                                           are_bricks_offline)
from glustolibs.gluster.heal_ops import trigger_heal
from glustolibs.gluster.heal_libs import (is_volume_in_split_brain,
                                          monitor_heal_completion,
                                          is_heal_complete)

from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.glusterfile import create_link_file


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestSelfHeal(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Override Volumes
        if cls.volume_type == "distributed-replicated":
            # Define x3 distributed-replicated volume
            cls.volume['voltype'] = {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 3,
                'transport': 'tcp'}

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

    def _test_brick_down_with_file_rename(self, pfile, rfile, brick):
        # Bring brick offline
        g.log.info('Bringing brick %s offline', brick)
        ret = bring_bricks_offline(self.volname, brick)
        self.assertTrue(ret, 'Failed to bring brick %s offline'
                        % brick)

        ret = are_bricks_offline(self.mnode, self.volname,
                                 [brick])
        self.assertTrue(ret, 'Brick %s is not offline'
                        % brick)
        g.log.info('Bringing brick %s offline is successful',
                   brick)

        # Rename file
        cmd = ("mv %s/%s %s/%s"
               % (self.mounts[0].mountpoint, pfile,
                  self.mounts[0].mountpoint, rfile))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "rename of file failed")

        # Bring brick back online
        g.log.info('Bringing brick %s online', brick)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  brick)
        self.assertTrue(ret, 'Failed to bring brick %s online' %
                        brick)
        g.log.info('Bringing brick %s online is successful', brick)

    def test_afr_heal_with_brickdown_hardlink(self):
        """
        Steps:
        1. Create  2 * 3 distribute replicate volume and disable all heals
        2. Create a file and 3 hardlinks to it from fuse mount.
        3. Kill brick4, rename HLINK1 to an appropriate name so that
           it gets hashed to replicate-1
        4. Likewise rename HLINK3 and HLINK7 as well, killing brick5 and brick6
           respectively each time. i.e. a different brick of the 2nd
           replica is down each time.
        5. Now enable shd and let selfheals complete.
        6. Heal should complete without split-brains.
        """
        bricks_list = get_all_bricks(self.mnode, self.volname)
        options = {"metadata-self-heal": "off",
                   "entry-self-heal": "off",
                   "data-self-heal": "off",
                   "self-heal-daemon": "off"}
        g.log.info("setting options %s", options)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for"
                              "volume %s" % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        cmd = ("touch %s/FILE" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "file creation failed")

        # Creating a hardlink for the file created
        for i in range(1, 4):
            ret = create_link_file(self.clients[0],
                                   '{}/FILE'.format(self.mounts[0].mountpoint),
                                   '{}/HLINK{}'.format
                                   (self.mounts[0].mountpoint, i))
            self.assertTrue(ret, "Unable to create hard link file ")

        # Bring brick3 offline,Rename file HLINK1,and bring back brick3 online
        self._test_brick_down_with_file_rename("HLINK1", "NEW-HLINK1",
                                               bricks_list[3])

        # Bring brick4 offline,Rename file HLINK2,and bring back brick4 online
        self._test_brick_down_with_file_rename("HLINK2", "NEW-HLINK2",
                                               bricks_list[4])

        # Bring brick5 offline,Rename file HLINK3,and bring back brick5 online
        self._test_brick_down_with_file_rename("HLINK3", "NEW-HLINK3",
                                               bricks_list[5])

        # Setting options
        options = {"self-heal-daemon": "on"}
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, 'Failed to set options %s' % options)
        g.log.info("Option 'self-heal-daemon' is set to 'on' successfully")

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

        # Check data on mount point
        cmd = ("ls %s" % (self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.clients[0], cmd)
        self.assertEqual(ret, 0, "failed to fetch data from mount point")
