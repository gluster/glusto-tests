#!/usr/bin/env python
#  Copyright (C) 2018-2020 Red Hat, Inc. <http://www.redhat.com>
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
#
"""Module for library DHT layout class and related functions"""

from glusto.core import Glusto as g
from glustolibs.gluster.brickdir import BrickDir
from glustolibs.gluster.gluster_init import get_gluster_version


class Layout(object):
    """Default layout class for equal-sized bricks.
        Other layouts should inherit from this class
        and override/add where needed.
    """
    def _get_layout(self):
        """Discover brickdir data and cache in instance for further use"""
        # Adding here to avoid cyclic imports
        from glustolibs.gluster.volume_libs import get_volume_type

        self._brickdirs = []
        for brickdir_path in self._pathinfo['brickdir_paths']:
            (host, _) = brickdir_path.split(':')
            if get_gluster_version(host) >= 6.0:
                ret = get_volume_type(brickdir_path)
                if ret in ('Replicate', 'Disperse', 'Arbiter'):
                    g.log.info("Cannot get layout as volume under test is"
                               " Replicate/Disperse/Arbiter and DHT"
                               " pass-through was enabled after Gluster 6.")
                else:
                    brickdir = BrickDir(brickdir_path)
                    if brickdir is None:
                        g.log.error("Failed to get the layout")
                    else:
                        g.log.debug("%s: %s" % (brickdir.path,
                                    brickdir.hashrange))
                        self._brickdirs.append(brickdir)

    def __init__(self, pathinfo):
        """Init the layout class

        Args:
            pathinfo (dict): pathinfo collected from client directory
        """
        self._pathinfo = pathinfo
        self._get_layout()
        self._zero_hashrange_brickdirs = None
        self._brickdirs = None

    @property
    def brickdirs(self):
        """list: a list of brickdirs associated with this layout"""
        if self._brickdirs is None:
            self._get_layout()

        return self._brickdirs

    @property
    def is_complete(self):
        """Layout starts at zero,
        ends at 32-bits high,
        and has no holes or overlaps
        """
        # Adding here to avoid cyclic imports
        from glustolibs.gluster.volume_libs import get_volume_type

        for brickdir_path in self._pathinfo['brickdir_paths']:
            (host, _) = brickdir_path.split(':')
            if (get_gluster_version(host) >= 6.0 and
                    get_volume_type(brickdir_path) in ('Replicate', 'Disperse',
                                                       'Arbiter')):
                g.log.info("Cannot check for layout completeness as volume"
                           " under test is Replicate/Disperse/Arbiter and DHT"
                           " pass-though was enabled after Gluster 6.")
            else:
                joined_hashranges = []
                for brickdir in self.brickdirs:
                    # join all of the hashranges into a single list
                    joined_hashranges += brickdir.hashrange
                g.log.debug("joined range list: %s" % joined_hashranges)
                # remove duplicate hashes
                collapsed_ranges = list(set(joined_hashranges))
                # sort the range list for good measure
                collapsed_ranges.sort()

                # first hash in the list is 0?
                if collapsed_ranges[0] != 0:
                    g.log.error('First hash in range (%d) is not zero' %
                                collapsed_ranges[0])
                    return False

                # last hash in the list is 32-bits high?
                if collapsed_ranges[-1] != int(0xffffffff):
                    g.log.error('Last hash in ranges (%s) is not 0xffffffff' %
                                hex(collapsed_ranges[-1]))
                    return False

                # remove the first and last hashes
                clipped_ranges = collapsed_ranges[1:-1]
                g.log.debug('clipped: %s' % clipped_ranges)

                # walk through the list in pairs and look for diff == 1
                iter_ranges = iter(clipped_ranges)
                for first in iter_ranges:
                    second = next(iter_ranges)
                    hash_difference = second - first
                    g.log.debug('%d - %d = %d' % (second, first,
                                                  hash_difference))
                    if hash_difference > 1:
                        g.log.error("Layout has holes")

                        return False
                    elif hash_difference < 1:
                        g.log.error("Layout has overlaps")

                        return False

                return True

    @property
    def has_zero_hashranges(self):
        """Check brickdirs for zero hashrange"""
        # TODO: change this to use self.zero_hashrange_brickdirs and set bool
        low_and_high_zero = False
        for brickdir in self._brickdirs:
            if brickdir.has_zero_hashrange:
                low_and_high_zero = True

        return low_and_high_zero

    @property
    def zero_hashrange_brickdirs(self):
        """list: the list of zero_hashrange_brickdirs"""
        if self._zero_hashrange_brickdirs is None:
            zero_hashrange_brickdirs = []
            for brickdir in self._brickdirs:
                if brickdir.has_zero_hashrange():
                    zero_hashrange_brickdirs.append(brickdir)
            self._zero_hashrange_brickdirs = zero_hashrange_brickdirs

        return self._zero_hashrange_brickdirs

    @property
    def is_balanced(self):
        """Checks for balanced distribution in equal-sized bricks"""
        baseline_size = None
        for brickdir in self._brickdirs:
            hashrange_low = brickdir.hashrange_low
            hashrange_high = brickdir.hashrange_high

            if baseline_size is None:
                baseline_size = int(hashrange_high) - int(hashrange_low)
                g.log.debug('Baseline  size: %d' % baseline_size)
                continue
            else:
                size = int(hashrange_high) - int(hashrange_low)
                g.log.debug('Hashrange size: %d' % size)

                # if any of the range diffs differ, exit immediately False
                if int(size) != int(baseline_size):
                    g.log.error('Brick distribution is not balanced.')
                    return False

        return True
