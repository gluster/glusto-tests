#  Copyright (C) 2019-2020 Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.brickmux_ops import (disable_brick_mux,
                                             is_brick_mux_enabled,
                                             get_brick_mux_status)
from glustolibs.gluster.lib_utils import search_pattern_in_file


@runs_on([['replicated'],
          ['glusterfs']])
class TestBrickMultiplexing(GlusterBaseClass):
    """
    Tests for brick multiplexing statuses
    """
    def tearDown(self):
        # Disable brick multiplexing
        g.log.info("Checking for brick multiplexing status...")
        if is_brick_mux_enabled(self.mnode):
            g.log.info("Disabling brick multiplexing...")
            if not disable_brick_mux(self.mnode):
                raise ExecutionError("Failed to disable brick multiplexing")
            g.log.info("Disabled brick multiplexing successfully")

        # Calling GlusterBaseClass teardown
        self.get_super_method(self, 'tearDown')()

    def test_enabling_brick_mux(self):
        """
        Test case:
        - check if brick multiplex is disable by default
        - check for warning message triggering by setting brick-multiplex and
        choosing 'n' in y/n
        - check if brick multiplex is disabled after triggering warning message
        - check brick multiplex for all possible statuses
        (positive and negative)
        - check for brick multiplex status in /var/lib/glusterd/options file
        """
        # Check if brickmux is disabled by default
        g.log.info('Checking if brick multiplex operation is disabled...')
        self.assertFalse(is_brick_mux_enabled(self.mnode),
                         "Brick multiplex is not disabled by default")

        # Check for warning message while changing status
        warning_message = ("Brick-multiplexing is supported only for "
                           "OCS converged or independent mode. Also it is "
                           "advised to make sure that either all volumes are "
                           "in stopped state or no bricks are running before "
                           "this option is modified."
                           "Do you still want to continue? (y/n)")

        g.log.info('Triggering warning message...')
        cmd = "gluster v set all cluster.brick-multiplex enable"
        _, out, _ = g.run(self.mnode, cmd)

        g.log.info('Checking for warning message in output...')
        if "volume set: success" not in out:
            self.assertIn(warning_message, out,
                          'There is no warning message in '
                          'output or message is incorrect.')
            g.log.info('Warning message is correct.')

        else:
            g.log.info('Skipped warning message check.')

            # If brick-mux is enabled then disabling it.
            if is_brick_mux_enabled(self.mnode):
                if not disable_brick_mux(self.mnode):
                    g.log.info("Disabling brick multiplexing as it"
                               " was enabled due to no warning message.")

        # Check if brickmux is still disabled
        g.log.info('Checking if brick multiplex is still disabled')
        self.assertFalse(is_brick_mux_enabled(self.mnode),
                         "Brick multiplex operation is not disabled")

        # Enable brick multiplex with all possible statuses
        statuses = ['on', 'enable', '1', 'true',
                    'off', 'disable', '0', 'false']
        for status in statuses:
            g.log.info('Enabling brick multiplex with %s status...',
                       status)
            cmd = ("yes | gluster v set all cluster.brick-multiplex %s"
                   % status)
            _, out, _ = g.run(self.mnode, cmd)
            self.assertIn('success', out,
                          'Failed on enabling brick multiplexing')

            # Check if brick multiplex status is correct
            g.log.info('Checking if brick multiplexing status is correct...')
            gluster_status = get_brick_mux_status(self.mnode)
            self.assertEqual(status, gluster_status,
                             "Brick multiplex status is not correct")
            g.log.info('Brick multiplex status "%s" is correct',
                       status)

            # Check for brick multiplexing status in file 'options'
            g.log.info("Checking for brick multiplexing status '%s' in file "
                       "'/var/lib/glusterd/options'...", status)
            search_pattern = 'cluster.brick-multiplex=%s' % status
            self.assertTrue(search_pattern_in_file(self.mnode, search_pattern,
                                                   '/var/lib/glusterd/options',
                                                   '', ''))
            g.log.info("Brick multiplexing status '%s' in file "
                       "'/var/lib/glusterd/options' is correct", status)

        # Check brick multiplex with incorrect status
        g.log.info('Checking brick multiplex with incorrect status...')
        cmd = "yes | gluster v set all cluster.brick-multiplex incorrect"
        ret, _, _ = g.run(self.mnode, cmd)
        self.assertEqual(ret, 1, 'Incorrect status has passed')
