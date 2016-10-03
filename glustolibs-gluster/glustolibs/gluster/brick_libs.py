#!/usr/bin/env python
#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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
    Description: Module for gluster brick related helper functions.
"""

import random
from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import (get_volume_info, get_volume_status)
from glustolibs.gluster.volume_libs import get_subvols


def get_all_bricks(mnode, volname):
    """Get list of all the bricks of the specified volume.
        If the volume is 'Tier' volume, the list will contian both
        'hot tier' and 'cold tier' bricks.

    Args:
        mnode (str): Node on which command has to be executed
        volname (str): Name of the volume

    Returns:
        list: List of all the bricks of the volume on Success.
        NoneType: None on failure.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volinfo of %s." % volname)
        return None

    if 'Tier' in volinfo[volname]['typeStr']:
        # Get bricks from hot-tier in case of Tier volume
        hot_tier_bricks = get_hot_tier_bricks(mnode, volname)
        if hot_tier_bricks is None:
            return None
        # Get cold-tier bricks in case of Tier volume
        cold_tier_bricks = get_cold_tier_bricks(mnode, volname)
        if cold_tier_bricks is None:
            return None

        return hot_tier_bricks + cold_tier_bricks

    # Get bricks from a non Tier volume
    all_bricks = []
    if 'bricks' in volinfo[volname]:
        if 'brick' in volinfo[volname]['bricks']:
            for brick in volinfo[volname]['bricks']['brick']:
                if 'name' in brick:
                    all_bricks.append(brick['name'])
                else:
                    g.log.error("brick %s doesn't have the key 'name' "
                                "for the volume: %s" % (brick, volname))
                    return None
            return all_bricks
        else:
            g.log.error("Bricks not found in Bricks section of volume "
                        "info for the volume %s" % volname)
            return None
    else:
        g.log.error("Bricks not found for the volume %s" % volname)
        return None


def get_hot_tier_bricks(mnode, volname):
    """Get list of hot-tier bricks of the specified volume

    Args:
        mnode (str): Node on which command has to be executed
        volname (str): Name of the volume

    Returns:
        list : List of hot-tier bricks of the volume on Success.
        NoneType: None on failure.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volinfo of %s." % volname)
        return None

    if 'Tier' not in volinfo[volname]['typeStr']:
        g.log.error("Volume %s is not a tiered volume" % volname)
        return None

    hot_tier_bricks = []
    if 'bricks' in volinfo[volname]:
        if 'hotBricks' in volinfo[volname]['bricks']:
            if 'brick' in volinfo[volname]['bricks']['hotBricks']:
                for brick in volinfo[volname]['bricks']['hotBricks']['brick']:
                    if 'name' in brick:
                        hot_tier_bricks.append(brick['name'])
                    else:
                        g.log.error("brick %s doesn't have the key 'name' "
                                    "for the volume: %s" % (brick, volname))
                        return None
            else:
                g.log.error("Bricks not found in hotBricks section of volume "
                            "info for the volume %s" % volname)
                return None
        return hot_tier_bricks
    else:
        g.log.error("Bricks not found for the volume %s" % volname)
        return None


def get_cold_tier_bricks(mnode, volname):
    """Get list of cold-tier bricks of the specified volume

    Args:
        mnode (str): Node on which command has to be executed
        volname (str): Name of the volume

    Returns:
        list : List of cold-tier bricks of the volume on Success.
        NoneType: None on failure.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volinfo of %s." % volname)
        return None

    if 'Tier' not in volinfo[volname]['typeStr']:
        g.log.error("Volume %s is not a tiered volume" % volname)
        return None

    cold_tier_bricks = []
    if 'bricks' in volinfo[volname]:
        if 'coldBricks' in volinfo[volname]['bricks']:
            if 'brick' in volinfo[volname]['bricks']['coldBricks']:
                for brick in volinfo[volname]['bricks']['coldBricks']['brick']:
                    if 'name' in brick:
                        cold_tier_bricks.append(brick['name'])
                    else:
                        g.log.error("brick %s doesn't have the key 'name' "
                                    "for the volume: %s" % (brick, volname))
                        return None
            else:
                g.log.error("Bricks not found in coldBricks section of volume "
                            "info for the volume %s" % volname)
                return None
        return cold_tier_bricks
    else:
        g.log.error("Bricks not found for the volume %s" % volname)
        return None


def bring_bricks_offline(volname, bricks_list,
                         bring_bricks_offline_methods=['service_kill']):
    """Bring the bricks specified in the bricks_list offline.

    Args:
        volname (str): Name of the volume
        bricks_list (list): List of bricks to bring them offline.

    Kwargs:
        bring_bricks_offline_methods (list): List of methods using which bricks
            will be brought offline. The method to bring a brick offline is
            randomly selected from the bring_bricks_offline_methods list.
            By default all bricks will be brought offline with
            'service_kill' method.

    Returns:
        bool : True on successfully bringing all bricks offline.
            False otherwise
    """
    rc = True
    failed_to_bring_offline_list = []
    for brick in bricks_list:
        bring_brick_offline_method = (random.choice
                                      (bring_bricks_offline_methods))
        if bring_brick_offline_method == 'service_kill':
            brick_node, brick_path = brick.split(":")
            brick_path = brick_path.replace("/", "-")
            kill_cmd = ("pid=`cat /var/lib/glusterd/vols/%s/run/%s%s.pid` &&"
                        "kill -15 $pid || kill -9 $pid" %
                        (volname, brick_node, brick_path))
            ret, _, _ = g.run(brick_node, kill_cmd)
            if ret != 0:
                g.log.error("Unable to kill the brick %s" % brick)
                failed_to_bring_offline_list.append(brick)
                rc = False
        else:
            g.log.error("Invalid method '%s' to bring brick offline" %
                        bring_brick_offline_method)
            return False

    if not rc:
        g.log.error("Unable to bring some of the bricks %s offline" %
                    failed_to_bring_offline_list)
        return False

    g.log.info("All the bricks : %s are brought offline" % bricks_list)
    return True


def bring_bricks_online(mnode, volname, bricks_list,
                        bring_bricks_online_methods=['glusterd_restart',
                                                     'volume_start_force']):
    """Bring the bricks specified in the bricks_list online.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.
        bricks_list (list): List of bricks to bring them online.

    Kwargs:
        bring_bricks_online_methods (list): List of methods using which bricks
            will be brought online. The method to bring a brick online is
            randomly selected from the bring_bricks_online_methods list.
            By default all bricks will be brought online with
            ['glusterd_restart', 'volume_start_force'] methods.
            If 'volume_start_force' command is randomly selected then all the
            bricks would be started with the command execution. Hence we break
            from bringing bricks online individually

    Returns:
        bool : True on successfully bringing all bricks online.
            False otherwise
    """
    rc = True
    failed_to_brick_online_list = []
    for brick in bricks_list:
        bring_brick_online_method = random.choice(bring_bricks_online_methods)
        if bring_brick_online_method == 'glusterd_restart':
            bring_brick_online_command = "service glusterd restart"
            brick_node, brick_path = brick.split(":")
            ret, _, _ = g.run(brick_node, bring_brick_online_command)
            if ret != 0:
                g.log.error("Unable to restart glusterd on node %s" %
                            (brick_node))
                rc = False
                failed_to_bring_online_list.append(brick)
        elif bring_brick_online_method == 'volume_start_force':
            bring_brick_online_command = ("gluster volume start %s force" %
                                          volname)
            ret, _, _ = g.run(mnode, bring_brick_online_command)
            if ret != 0:
                g.log.error("Unable to start the volume %s with force option" %
                            (volname))
                rc = False
            else:
                break
        else:
            g.log.error("Invalid method '%s' to bring brick online" %
                        bring_brick_online_method)
            return False
    if not rc:
        g.log.error("Unable to bring some of the bricks %s online" %
                    failed_to_bring_online_list)
        return False

    g.log.info("All the bricks : %s are brought online" % bricks_list)
    return True


def are_bricks_offline(mnode, volname, bricks_list):
    """Verify all the specified list of bricks are offline.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.
        bricks_list (list): List of bricks to verify offline status.

    Returns:
        bool : True if all bricks offline. False otherwise.
        NoneType: None on failure in getting volume status
    """
    rc = True
    online_bricks_list = []
    volume_status = get_volume_status(mnode, volname)
    if not volume_status:
        g.log.error("Unable to check if bricks are offline for the volume %s" %
                    volname)
        return None
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        status = int(volume_status[volname][brick_node][brick_path]['status'])
        if status != 0:
            g.log.error("BRICK : %s is not offline" % (brick))
            online_bricks_list.append(brick)
            rc = False
    if not rc:
        g.log.error("Some of the bricks %s are not offline" %
                    online_bricks_list)
        return False

    g.log.info("All the bricks in %s are offline" % bricks_list)
    return True


def are_bricks_online(mnode, volname, bricks_list):
    """Verify all the specified list of bricks are online.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.
        bricks_list (list): List of bricks to verify online status.

    Returns:
        bool : True if all bricks online. False otherwise.
        NoneType: None on failure in getting volume status
    """
    rc = True
    offline_bricks_list = []
    volume_status = get_volume_status(mnode, volname)
    if not volume_status:
        g.log.error("Unable to check if bricks are online for the volume %s" %
                    volname)
        return None
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        status = int(volume_status[volname][brick_node][brick_path]['status'])
        if status != 1:
            g.log.error("BRICK : %s is not online" % (brick))
            offline_bricks_list.append(brick)
            rc = False

    if not rc:
        g.log.error("Some of the bricks %s are not online" %
                    offline_bricks_list)
        return False

    g.log.info("All the bricks %s are online" % bricks_list)
    return True

def get_offline_bricks_list(mnode, volname):
    """Get list of bricks which are offline.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Returns:
        list : List of bricks in the volume which are offline.
        NoneType: None on failure in getting volume status
    """
    offline_bricks_list = []
    volume_status = get_volume_status(mnode, volname)
    if not volume_status:
        g.log.error("Unable to get offline bricks_list for the volume %s" %
                    volname)
        return None

    bricks_list = get_all_bricks(mnode, volname)
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        status = int(volume_status[volname][brick_node][brick_path]['status'])
        if status != 1:
            offline_bricks_list.append(brick)

    return offline_bricks_list


def get_online_bricks_list(mnode, volname):
    """Get list of bricks which are online.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Returns:
        list : List of bricks in the volume which are online.
        NoneType: None on failure in getting volume status
    """
    online_bricks_list = []
    volume_status = get_volume_status(mnode, volname)
    if not volume_status:
        g.log.error("Unable to get online bricks_list for the volume %s" %
                    volname)
        return None

    bricks_list = get_all_bricks(mnode, volname)
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        status = int(volume_status[volname][brick_node][brick_path]['status'])
        if status == 1:
            online_bricks_list.append(brick)

    return online_bricks_list


def delete_bricks(bricks_list):
    """Deletes list of bricks specified from the brick nodes.

    Args:
        bricks_list (list): List of bricks to be deleted.

    Returns:
        bool : True if all the bricks are deleted. False otherwise.
    """
    rc = True
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        _, _, _ = g.run(brick_node, "rm -rf %s" % brick_path)
        ret, out, err = g.run(brick_node, "ls %s" % brick_path)
        if ret == 0:
            g.log.error("Unable to delete brick %s on node %s" %
                        (brick_path, brick_node))
            rc = False
    return rc
