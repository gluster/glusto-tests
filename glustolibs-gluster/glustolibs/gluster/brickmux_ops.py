#!/usr/bin/env python
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

"""
    Description: Module for gluster brick multiplex operations
"""

from glusto.core import Glusto as g


def is_brick_mux_enabled(mnode):
    """Checks for brick multiplex operation enabled or not

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        bool : True if successfully enabled brickmux. False otherwise.
    """
    cmd = ("gluster v get all all | grep cluster.brick-multiplex |"
           "awk '{print $2}'")
    ret, out, err = g.run(mnode, cmd)
    if "enable" in out:
        return True
    else:
        return False


def enable_brick_mux(mnode):
    """Enables brick multiplex operation on all servers

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        bool : True if successfully enabled brickmux. False otherwise.
    """
    cmd = ("gluster v set all cluster.brick-multiplex enable")
    _, out, _ = g.run(mnode, cmd)
    if "success" in out:
        return True
    return False
