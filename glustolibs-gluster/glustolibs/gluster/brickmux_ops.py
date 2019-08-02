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
from glustolibs.gluster.volume_ops import get_volume_status


def get_brick_mux_status(mnode):
    """Gets brick multiplex status

        Args:
            mnode (str): Node on which cmd has to be executed.

        Returns:
            str : Brick multiplex status. None otherwise
        """
    cmd = ("gluster v get all all | grep cluster.brick-multiplex |"
           "awk '{print $2}'")
    _, out, _ = g.run(mnode, cmd)
    return out.strip()


def is_brick_mux_enabled(mnode):
    """Gets brick multiplex status and checks for positive and negative states

        Args:
            mnode (str): Node on which cmd has to be executed.

        Returns:
            True : Brick multiplex status is one of 'true, on, 1, enable'.
            False : Brick multiplex status is one of 'false, off, 0, disable'.
            Exception : otherwise
        """
    positive_states = ['true', 'on', '1', 'enable']
    negative_states = ['off', 'disable', '0', 'false']
    if get_brick_mux_status(mnode) in positive_states:
        return True
    elif get_brick_mux_status(mnode) in negative_states:
        return False
    else:
        raise ValueError('Brick mux status %s is incorrect',
                         get_brick_mux_status(mnode))


def enable_brick_mux(mnode):
    """Enables brick multiplex operation on all servers

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        bool : True if successfully enabled brickmux. False otherwise.
    """
    cmd = "gluster v set all cluster.brick-multiplex enable --mode=script"
    ret, _, err = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to enable brick multiplex: %s", err)
        return False
    return True


def disable_brick_mux(mnode):
    """Disables brick multiplex operation on all servers

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        bool : True if successfully disabled brickmux. False otherwise.
    """
    cmd = "gluster v set all cluster.brick-multiplex disable --mode=script"
    ret, _, err = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to disable brick multiplex: %s", err)
        return False
    return True


def check_brick_pid_matches_glusterfsd_pid(mnode, volname):
    # pylint: disable=invalid-name
    """Checks for brick process(es) both volume status
       and 'ps -eaf | grep glusterfsd' matches for
       the given volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): Name of the volume.

    Returns:
        bool : True if pid's matches. False otherwise.
    """
    from glustolibs.gluster.brick_libs import get_all_bricks
    _rc = True
    bricks_list = get_all_bricks(mnode, volname)
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        ret = get_volume_status(mnode, volname)
        brick_pid = ret[volname][brick_node][brick_path]["pid"]
        if brick_pid == "None":
            g.log.error("Failed to get brick pid on node %s "
                        "of brick path %s", brick_node, brick_path)
            _rc = False

        cmd = ("ps -eaf | grep glusterfsd | "
               "grep %s.%s | grep -v 'grep %s.%s'"
               % (volname, brick_node,
                  volname, brick_node))
        ret, pid, _ = g.run(brick_node, cmd)
        if ret != 0:
            g.log.error("Failed to run the command %s on "
                        "node %s", cmd, brick_node)
            _rc = False
        glusterfsd_pid = pid.split()[1]

        if glusterfsd_pid != brick_pid:
            g.log.error("Brick pid %s doesn't match glusterfsd "
                        "pid %s of the node %s", brick_pid,
                        glusterfsd_pid, brick_node)
            _rc = False

    return _rc


def get_brick_processes_count(mnode):
    """
    Get the brick process count for a given node.

    Args:
        mnode (str): Node on which brick process has to be counted.

    Returns:
        int: Number of brick processes running on the node.
        None: If the command fails to execute.
    """
    ret, out, _ = g.run(mnode, "pidof glusterfsd")
    if not ret:
        return len(out.split(" "))
    else:
        return None
