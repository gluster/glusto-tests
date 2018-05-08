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

import random
import time
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_libs import (setup_volume,
                                            replace_brick_from_volume)
from glustolibs.gluster.brick_libs import get_online_bricks_list
from glustolibs.gluster.lib_utils import form_bricks_list
from glustolibs.gluster.brick_ops import replace_brick
from glustolibs.gluster.brick_libs import are_bricks_online


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed-dispersed'], ['glusterfs']])
class TestReplaceBrick(GlusterBaseClass):
    def setUp(self):
        GlusterBaseClass.setUp.im_func(self)
        self.test_method_complete = False
        # Creating a volume and starting it
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        if not ret:
            raise ExecutionError("Failed to create volume")
        g.log.info("Volume created successfully")

    def tearDown(self):
        GlusterBaseClass.setUp.im_func(self)
        self.test_method_complete = False
        ret = self.cleanup_volume()
        if not ret:
            raise ExecutionError("Failed to cleanup volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

    def test_glusterd_replace_brick(self):
        """
        Create a volume and start it.
        - Get list of all the bricks which are online
        - Select a brick randomly from the bricks which are online
        - Form a non-existing brick path on node where the brick has to replace
        - Perform replace brick and it should fail
        - Form a new brick which valid brick path replace brick should succeed
        """
        # pylint: disable=too-many-function-args
        # Getting all the bricks which are online
        bricks_online = get_online_bricks_list(self.mnode, self.volname)
        self.assertIsNotNone(bricks_online, "Unable to get the online bricks")
        g.log.info("got the brick list from the volume")

        # Getting one random brick from the online bricks to be replaced
        brick_to_replace = random.choice(bricks_online)
        g.log.info("Brick to replace %s", brick_to_replace)
        node_for_brick_replace = brick_to_replace.split(':')[0]
        new_brick_to_replace = form_bricks_list(
            self.mnode, self.volname, 1,
            node_for_brick_replace, self.all_servers_info)

        # performing replace brick with non-existing brick path
        path = ":/brick/non_existing_path"
        non_existing_path = node_for_brick_replace + path

        # Replace brick for non-existing path
        ret, _, _ = replace_brick(self.mnode, self.volname,
                                  brick_to_replace, non_existing_path)
        self.assertNotEqual(ret, 0, ("Replace brick with commit force"
                                     " on a non-existing brick passed"))
        g.log.info("Replace brick with non-existing brick with commit"
                   "force failed as expected")

        # calling replace brick by passing brick_to_replace and
        # new_brick_to_replace with valid brick path
        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        self.servers, self.all_servers_info,
                                        brick_to_replace,
                                        new_brick_to_replace[0],
                                        delete_brick=True)
        self.assertTrue(ret, ("Replace brick with commit force failed"))

        # Validating whether the brick replaced is online
        halt = 20
        counter = 0
        _rc = False
        g.log.info("Wait for some seconds for the replaced brick "
                   "to get online")
        while counter < halt:
            ret = are_bricks_online(self.mnode, self.volname,
                                    new_brick_to_replace)
            if not ret:
                g.log.info("The replaced brick isn't online, "
                           "Retry after 2 seconds .......")
                time.sleep(2)
                counter = counter + 2
            else:
                _rc = True
                g.log.info("The replaced brick is online after being replaced")
                break
        if not _rc:
            raise ExecutionError("The replaced brick isn't online")
