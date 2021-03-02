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

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_ops import add_brick
from glustolibs.gluster.glusterfile import occurences_of_pattern_in_file
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           get_volume_options)
from glustolibs.gluster.volume_libs import (expand_volume, shrink_volume,
                                            form_bricks_list_to_add_brick)


@runs_on([['distributed', 'replicated'], ['glusterfs']])
class TestClientIOThreadsOnReplicatedVolumes(GlusterBaseClass):

    def setUp(self):

        self.get_super_method(self, 'setUp')()

        if self.volume_type == "distributed":
            # Changing dist_count to 1
            self.volume['voltype']['dist_count'] = 1

        # Setup Volume
        if not self.setup_volume():
            raise ExecutionError("Failed to setup volume")

    def tearDown(self):

        if not self.cleanup_volume():
            raise ExecutionError("Failed to cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _check_value_of_performance_client_io_threads(self, enabled=True):
        """Check value of performance.client-io-threads"""
        # Setting output value based on enabled param value
        value, instances = "off", 0
        if enabled:
            value, instances = "on", 3

        # Check if output value is same as expected or not
        ret = get_volume_options(self.mnode, self.volname,
                                 option="performance.client-io-threads")
        self.assertEqual(ret['performance.client-io-threads'], value,
                         "performance.client-io-threads value {} instead "
                         "of {}".format(ret['performance.client-io-threads'],
                                        value))

        # Check if io-threads is loaded or not based on enabled param value
        ret = occurences_of_pattern_in_file(
            self.mnode, 'io-threads', "/var/lib/glusterd/vols/{}/trusted-{}."
            "tcp-fuse.vol".format(self.volname, self.volname))
        self.assertEqual(ret, instances, "Number of io-threads more than {}"
                         .format(instances))

    def test_client_io_threads_on_replicate_volumes(self):
        """
        Test case 1:
        1. Create distrubuted volume and start it.
        2. Check the value of performance.client-io-threads it should be ON.
        3. io-threads should be loaded in trusted-.tcp-fuse.vol.
        4. Add brick to convert to replicate volume.
        5. Check the value of performance.client-io-threads it should be OFF.
        6. io-threads shouldn't be loaded in trusted-.tcp-fuse.vol.
        7. Remove brick so thate volume type is back to distributed.
        8. Check the value of performance.client-io-threads it should be ON.
        9. performance.client-io-threads should be loaded in
           trusted-.tcp-fuse.vol.

        Test case 2:
        1. Create a replicate volume and start it.
        2. Set performance.client-io-threads to ON.
        3. Check the value of performance.client-io-threads it should be ON.
        4. io-threads should be loaded in trusted-.tcp-fuse.vol.
        5. Add bricks to convert to make the volume 2x3.
        6. Check the value of performance.client-io-threads it should be ON.
        7. io-threads should be loaded in trusted-.tcp-fuse.vol.
        8. Remove brick to make the volume 1x3 again.
        9. Check the value of performance.client-io-threads it should be ON.
        10. performance.client-io-threads should be loaded in
            trusted-.tcp-fuse.vol.
        """
        # If volume type is distributed then run test case 1.
        if self.volume_type == "distributed":

            # Check the value of performance.client-io-threads it should be ON
            # and io-threads should be loaded in trusted-.tcp-fuse.vol
            self._check_value_of_performance_client_io_threads()

            # Add brick to convert to replicate volume
            brick = form_bricks_list_to_add_brick(self.mnode, self.volname,
                                                  self.servers,
                                                  self.all_servers_info)
            self.assertIsNotNone(brick,
                                 "Failed to form brick list to add brick")

            ret, _, _ = add_brick(self.mnode, self.volname, brick,
                                  force=True, replica_count=2)
            self.assertFalse(ret, "Failed to add brick on volume %s"
                             % self.volname)
            g.log.info("Add-brick successful on volume")

            # Check the value of performance.client-io-threads it should be ON
            # and io-threads should be loaded in trusted-.tcp-fuse.vol
            self._check_value_of_performance_client_io_threads(enabled=False)

            # Remove brick so thate volume type is back to distributed
            ret = shrink_volume(self.mnode, self.volname, replica_num=1)
            self.assertTrue(ret, "Failed to remove-brick from volume")
            g.log.info("Remove-brick successful on volume")

            # Check the value of performance.client-io-threads it should be ON
            # and io-threads should be loaded in trusted-.tcp-fuse.vol
            self._check_value_of_performance_client_io_threads()

        # If volume type is replicated then run test case 2.
        else:
            # Set performance.client-io-threads to ON
            options = {"performance.client-io-threads": "on"}
            ret = set_volume_options(self.mnode, self.volname, options)
            self.assertTrue(ret, "Unable to set volume option %s for"
                            "volume %s" % (options, self.volname))
            g.log.info("Successfully set %s for volume %s",
                       options, self.volname)

            # Check the value of performance.client-io-threads it should be ON
            # and io-threads should be loaded in trusted-.tcp-fuse.vol
            self._check_value_of_performance_client_io_threads()

            # Add bricks to convert to make the volume 2x3
            ret = expand_volume(self.mnode, self.volname, self.servers,
                                self.all_servers_info)
            self.assertTrue(ret, "Failed to add brick on volume %s"
                            % self.volname)
            g.log.info("Add-brick successful on volume")

            # Check the value of performance.client-io-threads it should be ON
            # and io-threads should be loaded in trusted-.tcp-fuse.vol
            self._check_value_of_performance_client_io_threads()

            # Remove brick to make the volume 1x3 again
            ret = shrink_volume(self.mnode, self.volname)
            self.assertTrue(ret, "Failed to remove-brick from volume")
            g.log.info("Remove-brick successful on volume")

            # Check the value of performance.client-io-threads it should be ON
            # and io-threads should be loaded in trusted-.tcp-fuse.vol
            self._check_value_of_performance_client_io_threads()
