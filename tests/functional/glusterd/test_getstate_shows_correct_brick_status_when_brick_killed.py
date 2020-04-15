#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.volume_ops import (volume_stop,
                                           volume_start,
                                           get_gluster_state)
from glustolibs.gluster.brick_libs import (get_offline_bricks_list,
                                           bring_bricks_online,
                                           get_online_bricks_list,
                                           bring_bricks_offline)


@runs_on([['distributed-dispersed', 'replicated', 'arbiter',
           'distributed-replicated', 'distributed', 'dispersed',
           'distributed-arbiter'],
          ['glusterfs']])
class TestGetStateBrickStatus(GlusterBaseClass):

    def setUp(self):
        self.get_super_method(self, 'setUp')()
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def test_validate_get_state(self):
        """
        TestCase:
        1. Execute "gluster get-state" say on N1(Node1)
        2. Start one by one volume and check brick status in get-state output
        3. Make sure there are multiple glusterfsd on one node say N1
            Kill one glusterfsd (kill -9 <piod>) and check
        4. Execute "gluster get-state" on N1
        """
        # Stop Volume
        ret, _, _ = volume_stop(self.mnode, self.volname, force=True)
        self.assertEqual(ret, 0, ("Failed to stop the volume "
                                  "%s", self.volname))

        # Execute 'gluster get-state' on mnode
        get_state_data = get_gluster_state(self.mnode)
        self.assertIsNotNone(get_state_data, "Getting gluster state failed.")

        # Getting Brick 1 Status - It should be in Stopped State
        brick_status = (get_state_data['Volumes']
                        ['volume1.brick1.status'].strip())
        self.assertEqual(brick_status, "Stopped",
                         "The brick is not in Stopped State")

        # Start the volume and check the status of brick again
        ret, _, _ = volume_start(self.mnode, self.volname, force=True)
        self.assertFalse(ret, 'Failed to start volume %s with "force" option'
                         % self.volname)

        # Execute 'gluster get-state' on mnode
        get_state_data = get_gluster_state(self.mnode)
        self.assertIsNotNone(get_state_data, "Getting gluster state failed.")
        # Getting Brick 1 Status - It should be in Started State
        brick_status = (get_state_data['Volumes']
                        ['volume1.brick1.status'].strip())
        self.assertEqual(brick_status, "Started",
                         "The brick is not in Started State")

        # Bringing the brick offline
        vol_bricks = get_online_bricks_list(self.mnode, self.volname)
        ret = bring_bricks_offline(self.volname, vol_bricks[0])
        self.assertTrue(ret, 'Failed to bring brick %s offline' %
                        vol_bricks[0])

        # Execute 'gluster get-state' on mnode
        get_state_data = get_gluster_state(self.mnode)
        self.assertIsNotNone(get_state_data, "Getting gluster state failed.")
        # Getting Brick 1 Status - It should be in Stopped State
        brick_status = (get_state_data['Volumes']
                        ['volume1.brick1.status'].strip())
        self.assertEqual(brick_status, "Stopped",
                         "The brick is not in Stopped State")
        g.log.info("Brick 1 is in Stopped state as expected.")

        # Checking the server 2 for the status of Brick.
        # It should be 'Started' state
        node2 = self.servers[1]
        get_state_data = get_gluster_state(node2)
        self.assertIsNotNone(get_state_data, "Getting gluster state failed.")
        # Getting Brick 2 Status - It should be in Started State
        brick_status = (get_state_data['Volumes']
                        ['volume1.brick2.status'].strip())
        self.assertEqual(brick_status, "Started",
                         "The brick is not in Started State")
        g.log.info("Brick2 is in started state.")

        # Bringing back the offline brick
        offline_brick = get_offline_bricks_list(self.mnode, self.volname)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  offline_brick)
        self.assertTrue(ret, 'Failed to bring brick %s online' %
                        offline_brick)

    def tearDown(self):
        # stopping the volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume & Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # calling GlusterBaseClass tearDownClass
        self.get_super_method(self, 'tearDown')()
