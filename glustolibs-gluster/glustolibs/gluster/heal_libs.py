#!/usr/bin/env python
#  Copyright (C) 2016 Red Hat, Inc. <http://www.redhat.com>
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

"""
    Description: Module for gluster heal related helper functions.
"""

import time
from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_status
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree


def is_heal_enabled(mnode, volname):
    """Check if heal is enabled for a volume.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        bool : True if heal is enabled on volume. False otherwise.
        NoneType: None if unable to get the volume status.
    """
    enabled = True
    vol_status_dict = get_volume_status(mnode, volname, service='shd')
    if vol_status_dict is None:
        g.log.error("Failed to check if heal is enabled on volume %s or not" %
                    volname)
        return None
    for node in vol_status_dict[volname].keys():
        if not ('Self-heal Daemon' in vol_status_dict[volname][node]):
            enabled = False
    return enabled


def is_heal_disabled(mnode, volname):
    """Check if heal is disabled for a volume.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        bool : True if heal is diabled on volume. False otherwise.
        NoneType: None if unable to get the volume status shd or parse error.
    """
    cmd = "gluster volume status %s shd --xml" % volname
    ret, out, _ = g.run(mnode, cmd, log_level='DEBUG')
    if ret != 0:
        g.log.error("Failed to get the self-heal-daemon status for the "
                    "volume" % volname)
        return None

    try:
        root = etree.XML(out)
    except etree.ParseError:
        g.log.error("Failed to parse the volume status shd xml output.")
        return None

    operr = root.find("opErrstr")
    if operr:
        if "Self-heal Daemon is disabled for volume" in operr.text:
            return True
    return False


def are_all_self_heal_daemons_are_online(mnode, volname):
    """Verifies whether all the self-heal-daemons are online for the specified
        volume.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        bool : True if all the self-heal-daemons are online for the volume.
            False otherwise.
        NoneType: None if unable to get the volume status
    """
    service = 'shd'
    failure_msg = ("Verifying all self-heal-daemons are online failed for "
                   "volume %s" % volname)
    # Get volume status
    vol_status = get_volume_status(mnode=mnode, volname=volname,
                                   service=service)
    if vol_status is None:
        g.log.error(failure_msg)
        return None

    # Get all nodes from pool list
    from glustolibs.gluster.peer_ops import nodes_from_pool_list
    all_nodes = nodes_from_pool_list(mnode)
    if not all_nodes:
        g.log.error(failure_msg)
        return False

    online_status = True
    for node in all_nodes:
        node_shd_status_value = (vol_status[volname][node]['Self-heal Daemon']
                                 ['status'])
        if node_shd_status_value != '1':
            online_status = False
    g.run(mnode, ("gluster volume status %s shd" % volname))
    if online_status is True:
        g.log.info("All self-heal Daemons are online")
        return True
    else:
        g.log.error("Some of the self-heal Daemons are offline")
        return False


def monitor_heal_completion(mnode, volname, timeout_period=1200):
    """Monitors heal completion by looking into .glusterfs/indices/xattrop
        directory of every brick for certain time. When there are no entries
        in all the brick directories then heal is successful. Otherwise heal is
        pending on the volume.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume
        heal_monitor_timeout : time until which the heal monitoring to be done.
                               Default: 1200 i.e 20 minutes.

    Return:
        bool: True if heal is complete within timeout_period. False otherwise
    """
    if timeout_period != 0:
        heal_monitor_timeout = timeout_period
    time_counter = heal_monitor_timeout
    g.log.info("The heal monitoring timeout is : %d minutes" %
               (heal_monitor_timeout / 60))

    # Get all bricks
    from glustolibs.gluster.brick_libs import get_all_bricks
    bricks_list = get_all_bricks(mnode, volname)
    if bricks_list is None:
        g.log.error("Unable to get the bricks list. Hence unable to verify "
                    "whether self-heal-daemon process is running or not "
                    "on the volume %s" % volname)
        return False

    while time_counter > 0:
        heal_complete = True
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            cmd = ("ls -1 %s/.glusterfs/indices/xattrop/ | "
                   "grep -ve \"xattrop-\" | wc -l" % brick_path)
            ret, out, err = g.run(brick_node, cmd)
            if out.strip('\n') != "0":
                heal_complete = False
        if heal_complete:
            break
        else:
            time.sleep(120)
            time_counter = time_counter - 120

    if heal_complete:
        heal_completion_status = is_heal_complete(mnode, volname)
        if heal_completion_status is True:
            g.log.info("Heal has successfully completed on volume %s" %
                       volname)
            return True

    g.log.info("Heal has not yet completed on volume %s" % volname)
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        cmd = ("ls -1 %s/.glusterfs/indices/xattrop/ " % brick_path)
        g.run(brick_node, cmd)
    return False


def is_heal_complete(mnode, volname):
    """Verifies there are no pending heals on the volume.
        The 'number of entries' in the output of heal info
        for all the bricks should be 0 for heal to be completed.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Return:
        bool: True if heal is complete. False otherwise
    """
    from glustolibs.gluster.heal_ops import get_heal_info
    heal_info_data = get_heal_info(mnode, volname)
    if heal_info_data is None:
        g.log.error("Unable to verify whether heal is successful or not on "
                    "volume %s" % volname)
        return False

    heal_complete = True
    for brick_heal_info_data in heal_info_data:
        if brick_heal_info_data['numberOfEntries'] != '0':
            heal_complete = False

    if not heal_complete:
        g.log.error("Heal is not complete on some of the bricks for the "
                    "volume %s" % volname)
        return False
    g.log.info("Heal is complete on all the bricks for the volume %s" %
               volname)
    return True


def is_volume_in_split_brain(mnode, volname):
    """Verifies there are no split-brain on the volume.
        The 'number of entries' in the output of heal info split-brain
        for all the bricks should be 0 for volume not to be in split-brain.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Return:
        bool: True if volume is not in split-brain. False otherwise
    """
    from glustolibs.gluster.heal_ops import get_heal_info_split_brain
    heal_info_split_brain_data = get_heal_info_split_brain(mnode, volname)
    if heal_info_split_brain_data is None:
        g.log.error("Unable to verify whether volume %s is not in split-brain "
                    "or not" % volname)
        return False

    split_brain = False
    for brick_heal_info_split_brain_data in heal_info_split_brain_data:
        if brick_heal_info_split_brain_data['numberOfEntries'] == '-':
            continue
        if brick_heal_info_split_brain_data['numberOfEntries'] != '0':
            split_brain = True

    if split_brain:
        g.log.error("Volume %s is in split-brain state." % volname)
        return True

    g.log.info("Volume %s is not in split-brain state." % volname)
    return False


def get_unhealed_entries_info(volname, mnode=''):
    """Get the information of all gfid's on which heal is pending. The
        information includes - stat of gfid, getfattr output for all the dirs/
        files for a given gfid

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Return:
        bool: True if getting unhealed entries info is successful.
            False otherwise
    """
    return True
