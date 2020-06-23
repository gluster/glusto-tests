#  Copyright (C) 2015-2020  Red Hat, Inc. <http://www.redhat.com>
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

""" Description: Module for gluster brick related helper functions. """

import random
from math import floor
import time
from glusto.core import Glusto as g
from glustolibs.gluster.brickmux_ops import is_brick_mux_enabled
from glustolibs.gluster.volume_ops import (get_volume_info, get_volume_status)
from glustolibs.gluster.volume_libs import (get_subvols,
                                            get_client_quorum_info,
                                            get_volume_type_info)
from glustolibs.gluster.lib_utils import (get_extended_attributes_info)


def get_all_bricks(mnode, volname):
    """Get list of all the bricks of the specified volume.

    Args:
        mnode (str): Node on which command has to be executed
        volname (str): Name of the volume

    Returns:
        list: List of all the bricks of the volume on Success.
        NoneType: None on failure.
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volinfo of %s.", volname)
        return None

    # Get bricks from a volume
    all_bricks = []
    if 'bricks' in volinfo[volname]:
        if 'brick' in volinfo[volname]['bricks']:
            for brick in volinfo[volname]['bricks']['brick']:
                if 'name' in brick:
                    all_bricks.append(brick['name'])
                else:
                    g.log.error("brick %s doesn't have the key 'name' "
                                "for the volume: %s", brick, volname)
                    return None
            return all_bricks
        g.log.error("Bricks not found in Bricks section of volume "
                    "info for the volume %s", volname)
        return None
    g.log.error("Bricks not found for the volume %s", volname)
    return None


def bring_bricks_offline(volname, bricks_list,
                         bring_bricks_offline_methods=None):
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
    if bring_bricks_offline_methods is None:
        bring_bricks_offline_methods = ['service_kill']
    elif not isinstance(bring_bricks_offline_methods, list):
        bring_bricks_offline_methods = [bring_bricks_offline_methods]

    if not isinstance(bricks_list, list):
        bricks_list = [bricks_list]

    node_list = []
    for brick in bricks_list:
        node, _ = brick.split(":")
        node_list.append(node)

    if is_brick_mux_enabled(node_list[0]):
        _rc = True
        failed_to_bring_offline_list = []
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            cmd = ("pgrep glusterfsd")
            _, out, _ = g.run(brick_node, cmd)
            if len(out.split()) > 1:
                cmd = ("ps -eaf | grep glusterfsd | "
                       " grep %s.%s | grep -o '/var/run/gluster/.*' | "
                       " awk '{ print $3 }' | grep -v 'awk' "
                       % (volname, brick_node))
            else:
                cmd = ("ps -eaf | grep glusterfsd | "
                       "grep -o '/var/run/gluster.*' | "
                       " awk '{ print $3 }' | grep -v 'awk'")
            _, socket_path, _ = g.run(brick_node, cmd)
            uds_path = socket_path.strip()
            kill_cmd = ("gf_attach -d %s %s"
                        % (uds_path, brick_path))
            ret, _, _ = g.run(brick_node, kill_cmd)
            if ret != 0:
                g.log.error("Unable to kill the brick %s", brick)
                failed_to_bring_offline_list.append(brick)
                _rc = False

        if not _rc:
            g.log.error("Unable to bring some of the bricks %s offline",
                        failed_to_bring_offline_list)
            return False

        g.log.info("All the bricks : %s are brought offline", bricks_list)
        return True

    _rc = True
    failed_to_bring_offline_list = []
    for brick in bricks_list:
        bring_brick_offline_method = (random.choice
                                      (bring_bricks_offline_methods))
        if bring_brick_offline_method == 'service_kill':
            brick_node, brick_path = brick.split(":")
            brick_path = brick_path.replace("/", "-")
            kill_cmd = ("pid=`ps -ef | grep -ve 'grep' | "
                        "grep -e '%s%s.pid' | awk '{print $2}'` && "
                        "kill -15 $pid || kill -9 $pid" %
                        (brick_node, brick_path))
            ret, _, _ = g.run(brick_node, kill_cmd)
            if ret != 0:
                g.log.error("Unable to kill the brick %s", brick)
                failed_to_bring_offline_list.append(brick)
                _rc = False
        else:
            g.log.error("Invalid method '%s' to bring brick offline",
                        bring_brick_offline_method)
            return False

    if not _rc:
        g.log.error("Unable to bring some of the bricks %s offline",
                    failed_to_bring_offline_list)
        return False

    g.log.info("All the bricks : %s are brought offline", bricks_list)
    return True


def bring_bricks_online(mnode, volname, bricks_list,
                        bring_bricks_online_methods=None):
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
    if bring_bricks_online_methods is None:
        bring_bricks_online_methods = ['glusterd_restart',
                                       'volume_start_force']
    elif not isinstance(bring_bricks_online_methods, list):
        bring_bricks_online_methods = [bring_bricks_online_methods]

    g.log.info("Bringing bricks '%s' online with '%s'",
               bricks_list, bring_bricks_online_methods)

    _rc = True
    failed_to_bring_online_list = []
    for brick in bricks_list:
        bring_brick_online_method = random.choice(bring_bricks_online_methods)
        if is_brick_mux_enabled(mnode):
            bring_bricks_online_command = ("gluster volume start %s force" %
                                           volname)
            ret, _, _ = g.run(mnode, bring_bricks_online_command)
            if ret != 0:
                g.log.error("Unable to start the volume %s with force option",
                            volname)
                _rc = False
            else:
                g.log.info("Successfully restarted volume %s to bring all "
                           "the bricks '%s' online", volname, bricks_list)

        elif bring_brick_online_method == 'glusterd_restart':
            bring_brick_online_command = "service glusterd restart"
            brick_node, _ = brick.split(":")
            ret, _, _ = g.run(brick_node, bring_brick_online_command)
            if ret != 0:
                g.log.error("Unable to restart glusterd on node %s",
                            brick_node)
                _rc = False
                failed_to_bring_online_list.append(brick)
            else:
                g.log.info("Successfully restarted glusterd on node %s to "
                           "bring back brick %s online", brick_node, brick)

        elif bring_brick_online_method == 'volume_start_force':
            bring_brick_online_command = ("gluster volume start %s force" %
                                          volname)
            ret, _, _ = g.run(mnode, bring_brick_online_command)
            if ret != 0:
                g.log.error("Unable to start the volume %s with force option",
                            volname)
                _rc = False
            else:
                g.log.info("Successfully restarted volume %s to bring all "
                           "the bricks '%s' online", volname, bricks_list)
                break
        else:
            g.log.error("Invalid method '%s' to bring brick online",
                        bring_brick_online_method)
            return False

    g.log.info("Waiting for 30 seconds for all the bricks to be online")
    time.sleep(30)
    return _rc


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
    _rc = True
    online_bricks_list = []
    volume_status = get_volume_status(mnode, volname)
    if not volume_status:
        g.log.error("Unable to check if bricks are offline for the volume %s",
                    volname)
        return None
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        status = int(volume_status[volname][brick_node][brick_path]['status'])
        if status != 0:
            g.log.error("BRICK : %s is not offline", brick)
            online_bricks_list.append(brick)
            _rc = False
    if not _rc:
        g.log.error("Some of the bricks %s are not offline",
                    online_bricks_list)
        return False

    g.log.info("All the bricks in %s are offline", bricks_list)
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
    _rc = True
    offline_bricks_list = []
    volume_status = get_volume_status(mnode, volname)
    if not volume_status:
        g.log.error("Unable to check if bricks are online for the volume %s",
                    volname)
        return None
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        status = int(volume_status[volname][brick_node][brick_path]['status'])
        if status != 1:
            g.log.error("BRICK : %s is not online", brick)
            offline_bricks_list.append(brick)
            _rc = False

    if not _rc:
        g.log.error("Some of the bricks %s are not online",
                    offline_bricks_list)
        return False

    g.log.info("All the bricks %s are online", bricks_list)
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
        g.log.error("Unable to get offline bricks_list for the volume %s",
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
        g.log.error("Unable to get online bricks_list for the volume %s",
                    volname)
        return None

    bricks_list = get_all_bricks(mnode, volname)
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        try:
            status = int(volume_status[volname]
                         [brick_node][brick_path]['status'])
        except KeyError:
            continue
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
    _rc = True
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        _, _, _ = g.run(brick_node, "rm -rf %s" % brick_path)
        ret, _, _ = g.run(brick_node, "ls %s" % brick_path)
        if ret == 0:
            g.log.error("Unable to delete brick %s on node %s",
                        brick_path, brick_node)
            _rc = False
    return _rc


def select_bricks_to_bring_offline(mnode, volname):
    """Randomly selects bricks to bring offline without affecting the cluster

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Returns:
        dict: On success returns dict. Value of each key is list of bricks to
            bring offline.
            If volume doesn't exist returns dict with value of each item
            being empty list.
            Example:
                brick_to_bring_offline = {
                    'volume_bricks': []
                    }
    """
    # Defaulting the values to empty list
    bricks_to_bring_offline = {
        'volume_bricks': []
    }

    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volume info for volume %s", volname)
        return bricks_to_bring_offline

    # Select bricks from the volume.
    volume_bricks = select_volume_bricks_to_bring_offline(mnode, volname)
    bricks_to_bring_offline['volume_bricks'] = volume_bricks

    return bricks_to_bring_offline


def select_volume_bricks_to_bring_offline(mnode, volname):
    """Randomly selects bricks to bring offline without affecting the cluster
    from a volume.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Returns:
        list: On success returns list of bricks that can be brough offline.
            If volume doesn't exist returns empty list
    """
    volume_bricks_to_bring_offline = []

    # get volume type
    volume_type_info = get_volume_type_info(mnode, volname)
    volume_type = volume_type_info['volume_type_info']['typeStr']

    # get subvols
    subvols_dict = get_subvols(mnode, volname)
    volume_subvols = subvols_dict['volume_subvols']

    # select bricks from distribute volume
    if volume_type == 'Distribute':
        volume_bricks_to_bring_offline = []

    # select bricks from replicated, distributed-replicated volume
    elif (volume_type == 'Replicate' or
          volume_type == 'Distributed-Replicate'):
        # Get replica count
        volume_replica_count = (volume_type_info['volume_type_info']
                                ['replicaCount'])

        # Get quorum info
        quorum_info = get_client_quorum_info(mnode, volname)
        volume_quorum_info = quorum_info['volume_quorum_info']

        # Get list of bricks to bring offline
        volume_bricks_to_bring_offline = (
            get_bricks_to_bring_offline_from_replicated_volume(
                volume_subvols, volume_replica_count, volume_quorum_info))

    # select bricks from Disperse, Distribured-Disperse volume
    elif (volume_type == 'Disperse' or
          volume_type == 'Distributed-Disperse'):

        # Get redundancy count
        volume_redundancy_count = (volume_type_info['volume_type_info']
                                   ['redundancyCount'])

        # Get list of bricks to bring offline
        volume_bricks_to_bring_offline = (
            get_bricks_to_bring_offline_from_disperse_volume(
                volume_subvols, volume_redundancy_count))

    return volume_bricks_to_bring_offline


def get_bricks_to_bring_offline_from_replicated_volume(subvols_list,
                                                       replica_count,
                                                       quorum_info):
    """Randomly selects bricks to bring offline without affecting the cluster
        for a replicated volume.

    Args:
        subvols_list: list of subvols.
            For example:
                subvols = volume_libs.get_subvols(mnode, volname)
                volume_subvols = subvols_dict['volume_subvols']
        replica_count: Replica count of a Replicate or Distributed-Replicate
            volume.
        quorum_info: dict containing quorum info of the volume. The dict should
            have the following info:
                - is_quorum_applicable, quorum_type, quorum_count
            For example:
                quorum_dict = get_client_quorum_info(mnode, volname)
                volume_quorum_info = quorum_info['volume_quorum_info']

    Returns:
        list: List of bricks that can be brought offline without affecting the
            cluster. On any failure return empty list.
    """
    list_of_bricks_to_bring_offline = []
    try:
        is_quorum_applicable = quorum_info['is_quorum_applicable']
        quorum_type = quorum_info['quorum_type']
        quorum_count = quorum_info['quorum_count']
    except KeyError:
        g.log.error("Unable to get the proper quorum data from quorum info: "
                    "%s", quorum_info)
        return list_of_bricks_to_bring_offline

    # offline_bricks_limit: Maximum Number of bricks that can be offline
    # without affecting the cluster
    if is_quorum_applicable:
        if 'fixed' in quorum_type:
            if quorum_count is None:
                g.log.error("Quorum type is 'fixed' for the volume. But "
                            "Quorum count not specified. Invalid Quorum")
                return list_of_bricks_to_bring_offline
            else:
                offline_bricks_limit = int(replica_count) - int(quorum_count)

        elif 'auto' in quorum_type:
            offline_bricks_limit = floor(int(replica_count) // 2)

        elif quorum_type is None:
            offline_bricks_limit = int(replica_count) - 1

        else:
            g.log.error("Invalid Quorum Type : %s", quorum_type)
            return list_of_bricks_to_bring_offline

        for subvol in subvols_list:
            random.shuffle(subvol)

            # select a random count.
            random_count = random.randint(1, offline_bricks_limit)

            # select random bricks.
            bricks_to_bring_offline = random.sample(subvol, random_count)

            # Append the list with selected bricks to bring offline.
            list_of_bricks_to_bring_offline.extend(bricks_to_bring_offline)

    return list_of_bricks_to_bring_offline


def get_bricks_to_bring_offline_from_disperse_volume(subvols_list,
                                                     redundancy_count):
    """Randomly selects bricks to bring offline without affecting the cluster
        for a disperse volume.

    Args:
        subvols_list: list of subvols.
            For example:
                subvols = volume_libs.get_subvols(mnode, volname)
                volume_subvols = subvols_dict['volume_subvols']
        redundancy_count: Redundancy count of a Disperse or
            Distributed-Disperse volume.

    Returns:
        list: List of bricks that can be brought offline without affecting the
            cluster.On any failure return empty list.
    """
    list_of_bricks_to_bring_offline = []
    for subvol in subvols_list:
        random.shuffle(subvol)

        # select a random value from 1 to redundancy_count.
        random_count = random.randint(1, int(redundancy_count))

        # select random bricks.
        bricks_to_bring_offline = random.sample(subvol, random_count)

        # Append the list with selected bricks to bring offline.
        list_of_bricks_to_bring_offline.extend(bricks_to_bring_offline)

    return list_of_bricks_to_bring_offline


def wait_for_bricks_to_be_online(mnode, volname, timeout=300):
    """Waits for the bricks to be online until timeout

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Kwargs:
        timeout (int): timeout value in seconds to wait for bricks to be
        online

    Returns:
        True if all bricks are online within timeout, False otherwise
    """
    all_bricks = get_all_bricks(mnode, volname)
    if not all_bricks:
        return False

    counter = 0
    flag = 0
    while counter < timeout:
        status = are_bricks_online(mnode, volname, all_bricks)

        if status:
            flag = 1
            break
        if not status:
            time.sleep(10)
            counter = counter + 10

    if not flag:
        g.log.error("All Bricks of the volume '%s' are not online "
                    "even after %d minutes", volname, timeout/60.0)
        return False
    else:
        g.log.info("All Bricks of the volume '%s' are online ", volname)
    return True


def is_broken_symlinks_present_on_bricks(mnode, volname):
    """ Checks if the backend bricks have broken symlinks.

    Args:
        mnode(str): Node on which command has to be executed
        volname(str): Name of the volume

    Retruns:
        (bool):Returns True if present else returns False.
    """
    brick_list = get_all_bricks(mnode, volname)
    for brick in brick_list:
        brick_node, brick_path = brick.split(":")
        cmd = ("find %s -xtype l | wc -l" % brick_path)
        ret, out, _ = g.run(brick_node, cmd)
        if ret:
            g.log.error("Failed to run command on node %s", brick_node)
            return True
        if out:
            g.log.error("Error: Broken symlink found on brick path: "
                        "%s on node %s.", (brick_path, brick_node))
            return True
    return False


def validate_xattr_on_all_bricks(bricks_list, file_path, xattr):
    """Checks if the xattr of the file/dir is same on all bricks.

    Args:
        bricks_list (list): List of bricks.
        file_path (str): The path to the file/dir.
        xattr (str): The file attribute to get from file.

    Returns:
        True if the xattr is same on all the fqpath. False otherwise

    Example:
        validate_xattr_on_all_bricks("bricks_list",
                                     "dir1/file1",
                                     "xattr")
    """

    time_counter = 250
    g.log.info("The heal monitoring timeout is : %d minutes",
               (time_counter // 60))
    while time_counter > 0:
        attr_vals = {}
        for brick in bricks_list:
            brick_node, brick_path = brick.split(":")
            attr_vals[brick] = (
                get_extended_attributes_info(brick_node,
                                             ["{0}/{1}".format(brick_path,
                                                               file_path)],
                                             attr_name=xattr))
        ec_version_vals = [list(val.values())[0][xattr] for val in
                           list(attr_vals.values())]
        if len(set(ec_version_vals)) == 1:
            return True
        else:
            time.sleep(120)
            time_counter -= 120
    return False
