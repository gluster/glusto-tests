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
"""Module for library brick class and related functions"""

import os

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_init import get_gluster_version
from glustolibs.gluster.volume_libs import get_volume_type


def check_hashrange(brickdir_path):
    """Check the hash range for a brick

    Args:
        brickdir_path (str): path of the directory as returned from pathinfo
            (e.g., server1.example.com:/bricks/brick1/testdir1)

    Returns:
        list containing the low and high hash for the brickdir. None on fail.
    """
    (host, fqpath) = brickdir_path.split(':')
    command = ("getfattr -n trusted.glusterfs.dht -e hex %s "
               "2> /dev/null | grep -i trusted.glusterfs.dht | "
               "cut -d= -f2" % fqpath)
    rcode, rout, rerr = g.run(host, command)
    full_hash_hex = rout.strip()

    if rcode == 0:
        # Grab the trailing 16 hex bytes
        trailing_hash_hex = full_hash_hex[-16:]
        # Split the full hash into low and high
        hash_range_low = int(trailing_hash_hex[0:8], 16)
        hash_range_high = int(trailing_hash_hex[-8:], 16)

        return (hash_range_low, hash_range_high)

    g.log.error('Could not get hashrange: %s' % rerr)
    return None


def get_hashrange(brickdir_path):
    """Check the gluster version and then the volume type.
       And accordingly, get the int hash range for a brick.

    Note:
        If the Gluster version is equal to or greater than 6, the hash range
        can be calculated only for distributed, distributed-dispersed,
        distributed-arbiter and distributed-replicated volume types because of
        DHT pass-through option which was introduced in Gluster 6.

        About DHT pass-through option:
        There are no user controllable changes with this feature.
        The distribute xlator now skips unnecessary checks and operations when
        the distribute count is one for a volume, resulting in improved
        performance.It comes into play when there is only 1 brick or it is a
        pure-replicate or pure-disperse or pure-arbiter volume.

    Args:
        brickdir_path (str): path of the directory as returned from pathinfo
            (e.g., server1.example.com:/bricks/brick1/testdir1)

    Returns:
        list containing the low and high hash for the brickdir. None on fail.

    """

    (host, _) = brickdir_path.split(':')
    gluster_version = get_gluster_version(host)
    # Check for the Gluster version and then volume type
    """If the GLuster version is lower than 6.0, the hash range
       can be calculated for all volume types"""
    if gluster_version < 6.0:
        ret = check_hashrange(brickdir_path)
        hash_range_low = ret[0]
        hash_range_high = ret[1]
        if ret is not None:
            return (hash_range_low, hash_range_high)
        else:
            g.log.error("Could not get hashrange")
            return None
    elif gluster_version >= 6.0:
        ret = get_volume_type(brickdir_path)
        if ret in ('replicate', 'disperse', 'arbiter'):
            g.log.info("Cannot find hash-range for Replicate/Disperse/Arbiter"
                       " volume type on Gluster 6.0 and higher.")
            return "Skipping for replicate/disperse/arbiter volume type"
        else:
            ret = check_hashrange(brickdir_path)
            hash_range_low = ret[0]
            hash_range_high = ret[1]
            if ret is not None:
                return (hash_range_low, hash_range_high)
            else:
                g.log.error("Could not get hashrange")
                return None
    else:
        g.log.info("Failed to get hash range")
        return None


def file_exists(host, filename):
    """Check if file exists at path on host

    Args:
        host (str): hostname or ip of system
        filename (str): fully qualified path of file

    Returns:
        True if file exists. False if file does not exist
    """
    command = "ls -ld %s" % filename
    rcode, _, _ = g.run(host, command)
    if rcode == 0:
        return True

    return False


class BrickDir(object):
    """Directory on a brick"""
    def __init__(self, path):
        self._path = path
        (self._host, self._fqpath) = self._path.split(':')
        self._hashrange = None
        self._hashrange_low = None
        self._hashrange_high = None

    def _check_hashrange(self):
        """get the hash range for a brick from a remote system"""
        self._hashrange = check_hashrange(self._path)
        self._hashrange_low = self._hashrange[0]
        self._hashrange_high = self._hashrange[1]

    def _get_hashrange(self):
        """get the hash range for a brick from a remote system"""
        gluster_version = get_gluster_version(self._host)
        if gluster_version < 6.0:
            self._hashrange = get_hashrange(self._path)
            self._hashrange_low = self._hashrange[0]
            self._hashrange_high = self._hashrange[1]
        elif gluster_version >= 6.0:
            ret = get_volume_type(self._path)
            if ret in ('replicate', 'disperse', 'arbiter'):
                g.log.info("Cannot find hash-range as the volume type under"
                           " test is Replicate/Disperse/Arbiter")
            else:
                self._hashrange = get_hashrange(self._path)
                self._hashrange_low = self._hashrange[0]
                self._hashrange_high = self._hashrange[1]
        else:
            g.log.info("Failed to get hashrange")

    @property
    def path(self):
        """The brick url
        Example:
            server1.example.com:/bricks/brick1
        """
        return self._path

    @property
    def host(self):
        """The hostname/ip of the system hosting the brick"""
        return self._host

    @property
    def fqpath(self):
        """The fully qualified path of the brick directory"""
        return self._fqpath

    @property
    def hashrange(self):
        """A list containing the low and high hash of the brick hashrange"""
        if self._hashrange is None:
            g.log.info("Retrieving hash range for %s" % self._path)
            self._get_hashrange()

        return (self._hashrange_low, self._hashrange_high)

    @property
    def hashrange_low(self):
        """The low hash of the brick hashrange"""
        if self.hashrange is None or self._hashrange_low is None:
            self._get_hashrange()

        return self._hashrange_low

    @property
    def hashrange_high(self):
        """The high hash of the brick hashrange"""
        if self.hashrange is None or self._hashrange_high is None:
            self._get_hashrange()
            if self._get_hashrange() is None:
                gluster_version = get_gluster_version(self._host)
                if gluster_version >= 6.0:
                    ret = get_volume_type(self._path)
                    if ret in ('replicate', 'disperse', 'arbiter'):
                        g.log.info("Cannot find hash-range as the volume type"
                                   " under test is Replicate/Disperse/Arbiter")
            else:
                return self._hashrange_high

    def hashrange_contains_hash(self, filehash):
        """Check if a hash number falls between the brick hashrange

        Args:
            filehash (int): hash being checked against range

        Returns:
            True if hash is in range. False if hash is out of range
        """
        if self._hashrange is None:
            self._get_hashrange()

        if self._hashrange_low <= filehash <= self._hashrange_high:
            return True

        return False

    def has_zero_hashrange(self):
        """figure out if the brickdir has a low and high zero value hash"""
        if self.hashrange_low == 0 and self.hashrange_high == 0:
            return True

        return False

    def resync_hashrange(self):
        """Reset the hashrange attributes and update hashrange from brick
        Args:
            None

        Returns:
            None
        """
        self._hashrange = None
        self._hashrange_low = None
        self._hashrange_high = None
        self._get_hashrange()

    def file_exists(self, filename):
        """Check if the file exists on the brick

        Args:
            filename (int): relative path of the file

        Returns:
            True if the file exists on the brick
            False if the file does not exist on the brick
        """
        fqfilepath = os.path.join(self._fqpath, filename)

        if file_exists(self._host, fqfilepath):
            return True

        return False
