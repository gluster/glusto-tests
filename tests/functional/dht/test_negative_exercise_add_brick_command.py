#  Copyright (C) 2017-2018 Red Hat, Inc. <http://www.redhat.com>
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

"""Negative test - Exercise Add-brick command"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass,
                                                   runs_on)
# pylint: disable=no-name-in-module
from glustolibs.gluster.volume_libs import (form_bricks_list_to_add_brick,
                                            get_subvols, setup_volume,
                                            cleanup_volume)
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.volume_ops import get_volume_list
from glustolibs.gluster.exceptions import ExecutionError


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class ExerciseAddbrickCommand(GlusterBaseClass):
    """BaseClass for running Negative test - Exercise Add-brick command"""
    @classmethod
    def setUpClass(cls):
        """Upload the necessary scripts to run tests"""
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

    def setUp(self):
        """Setup Volume"""
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume Volume
        g.log.info("Starting to Setup Volume")
        ret = self.setup_volume()
        if not ret:
            raise ExecutionError("Failed to Setup_Volume")
        g.log.info("Successful in Setting up Volume")

    def tearDown(self):
        """Cleanup Volume"""
        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume % s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_add_brick_without_volname(self):
        """Test add-brick command without volume"""
        # Form bricks list for add-brick
        bricks_list = form_bricks_list_to_add_brick(self.mnode, self.volname,
                                                    self.servers,
                                                    self.all_servers_info)
        cmd = ("gluster volume add-brick %s " % (' '.join(bricks_list)))
        g.log.info("Adding bricks without specifying volume name")
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertTrue(ret, "add-brick is successful")
        g.log.info("Volume %s: add-brick failed", self.volname)

    def test_add_duplicate_brick(self):
        """Test add-bricks to the volume which are already part of the volume
        """
        # Get sub-vols for adding the same bricks to the volume
        sub_vols = get_subvols(self.mnode, self.volname)['volume_subvols']
        cmd = ("gluster volume add-brick %s %s " % (self.volname,
                                                    ' '.join(sub_vols[0])))
        g.log.info("Adding bricks which are already part of the volume %s ",
                   self.volname)
        _, _, err = g.run(self.mnode, cmd)
        self.assertIn("Brick may be containing or be contained by an existing"
                      " brick", err, "add-brick is successful")
        g.log.info("Volume add-brick failed with error %s ", err)

    def test_add_nested_brick(self):
        """Test add nested bricks to the volume"""
        # Get sub-vols for forming a nested bricks list
        sub_vol = get_subvols(self.mnode, self.volname)['volume_subvols'][0]
        nested_bricks_list = [x + '/nested' for x in sub_vol]
        cmd = ('gluster volume add-brick %s %s ' % (
            self.volname, (' '.join(nested_bricks_list))))
        g.log.info("Adding nested bricks to the volume %s ", self.volname)
        _, _, err = g.run(self.mnode, cmd)
        self.assertIn("Brick may be containing or be contained by an existing"
                      " brick", err, "add-brick is successful")
        g.log.info("Volume add-brick failed with error %s ", err)

    def test_add_brick_non_existent_volume(self):
        """Test add-bricks to an non existent volume"""
        # Form bricks list for add-brick
        bricks_list = form_bricks_list_to_add_brick(self.mnode, self.volname,
                                                    self.servers,
                                                    self.all_servers_info)
        cmd = ("gluster volume add-brick novolume %s " %
               (' '.join(bricks_list)))
        g.log.info("Trying to add-bricks to a non-existent volume")
        _, _, err = g.run(self.mnode, cmd)
        self.assertIn("does not exist", err, "add-brick is successful")
        g.log.info("Volume add-brick failed with error %s ", err)

    def test_add_brick_peer_not_in_cluster(self):
        """ Test add bricks to the volume from the host which is not
        in the cluster.
        """
        # Form bricks list for add-brick
        bricks_list = get_subvols(self.mnode,
                                  self.volname)['volume_subvols'][0]
        for (i, item) in enumerate(bricks_list):
            server, _ = item.split(":")
            item.replace(server, "abc.def.ghi.jkl")
            bricks_list[i] = item.replace(server, "abc.def.ghi.jkl")
        g.log.info("Adding bricks to the volume %s from the host which is not"
                   " in the cluster", self.volname)
        _, _, err = add_brick(self.mnode, self.volname, bricks_list)
        self.assertIn("Pre-validation failed on localhost", err,
                      "add-brick is successful")
        g.log.info("Volume add-brick failed with error %s ", err)


@runs_on([['replicated', 'distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed'],
          ['glusterfs']])
class AddBrickAlreadyPartOfAnotherVolume(GlusterBaseClass):
    """Base class for running test add-brick which is already part of an
    another volume
    """
    def tearDown(self):
        """Cleanup Volume"""
        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if vol_list is None:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume %s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        self.get_super_method(self, 'tearDown')()

    def test_add_brick_already_part_of_another_volume(self):
        """ Test adding bricks to the volume which are already part of another
        volume.
        """
        # create and start a volume
        self.volume['name'] = "existing_volume"
        self.volname = "existing_volume"
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")
        g.log.info("Volume created and started successfully")
        sub_vols = get_subvols(self.mnode, self.volname)['volume_subvols']

        # create and start a new volume
        self.volume['name'] = "new_volume"
        self.volname = "new_volume"
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")
        g.log.info("Volume created and started successfully")
        cmd = ("gluster volume add-brick %s %s " % (self.volname,
                                                    ' '.join(sub_vols[0])))
        g.log.info("Adding bricks to volume %s which are already part of an"
                   "another volume", self.volname)
        _, _, err = g.run(self.mnode, cmd)
        self.assertIn("Brick may be containing or be contained by an existing"
                      " brick", err, "add-brick is successful")
        g.log.info("Volume add-brick failed with error %s ", err)
