#!/usr/bin/env python
#  Copyright (C) 2016-2020 Red Hat, Inc. <http://www.redhat.com>
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
        bool : True if heal is disabled on volume. False otherwise.
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
    from glustolibs.gluster.volume_libs import is_distribute_volume
    if is_distribute_volume(mnode, volname):
        g.log.info("Volume %s is a distribute volume. "
                   "Hence not checking for self-heal daemons "
                   "to be online", volname)
        return True

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


def monitor_heal_completion(mnode, volname, timeout_period=1200,
                            bricks=None, interval_check=120):
    """Monitors heal completion by looking into .glusterfs/indices/xattrop
        directory of every brick for certain time. When there are no entries
        in all the brick directories then heal is successful. Otherwise heal is
        pending on the volume.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume
        heal_monitor_timeout : time until which the heal monitoring to be done.
                               Default: 1200 i.e 20 minutes.

    Kwargs:
        bricks : list of bricks to monitor heal, if not provided
                 heal will be monitored on all bricks of volume
        interval_check : Time in seconds, for every given interval checks
                         the heal info, defaults to 120.

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
    bricks_list = bricks or get_all_bricks(mnode, volname)
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
            time.sleep(interval_check)
            time_counter = time_counter - interval_check

    if heal_complete and bricks:
        # In EC volumes, check heal completion only on online bricks
        # and `gluster volume heal info` fails for an offline brick
        return True

    if heal_complete and not bricks:
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


def wait_for_self_heal_daemons_to_be_online(mnode, volname, timeout=300):
    """Waits for the volume self-heal-daemons to be online until timeout

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Kwargs:
        timeout (int): timeout value in seconds to wait for self-heal-daemons
        to be online.

    Returns:
        True if all self-heal-daemons are online within timeout,
        False otherwise
    """
    from glustolibs.gluster.volume_libs import is_distribute_volume
    if is_distribute_volume(mnode, volname):
        g.log.info("Volume %s is a distribute volume. "
                   "Hence not waiting for self-heal daemons "
                   "to be online", volname)
        return True

    counter = 0
    flag = 0
    while counter < timeout:
        status = are_all_self_heal_daemons_are_online(mnode, volname)
        if status:
            flag = 1
            break
        if not status:
            time.sleep(10)
            counter = counter + 10

    if not flag:
        g.log.error("All self-heal-daemons of the volume '%s' are not online "
                    "even after %d minutes" % (volname, timeout/60.0))
        return False
    else:
        g.log.info("All self-heal-daemons of the volume '%s' are online ",
                   volname)
    return True


def get_self_heal_daemon_pid(nodes):
    """
    Checks if self-heal daemon process is running and
    return the process id's in dictionary format

    Args:
        nodes ( str|list ) : Node/Nodes of the cluster

    Returns:
        tuple : Tuple containing two elements (ret, glustershd_pids).
        The first element 'ret' is of type 'bool', True if and only if
        glustershd is running on all the nodes in the list and each
        node contains only one instance of glustershd running.
        False otherwise.

        The second element 'glustershd_pids' is of type dictonary and it
        contains the process ID's for glustershd
    """
    glustershd_pids = {}
    _rc = True
    if not isinstance(nodes, list):
        nodes = [nodes]
    cmd = r"pgrep -f glustershd | grep -v ^$$\$"
    g.log.info("Executing cmd: %s on node %s" % (cmd, nodes))
    results = g.run_parallel(nodes, cmd)
    for node in results:
        ret, out, err = results[node]
        if ret == 0:
            if len(out.strip().split("\n")) == 1:
                if not out.strip():
                    g.log.error("NO self heal daemon process found "
                                "on node %s" % node)
                    _rc = False
                    glustershd_pids[node] = [-1]
                else:
                    g.log.info("Single Self Heal Daemon process with "
                               "pid %s found on %s",
                               out.strip().split("\n"), node)
                    glustershd_pids[node] = (out.strip().split("\n"))
            else:
                g.log.error("More than One self heal daemon process "
                            "found on node %s" % node)
                _rc = False
                glustershd_pids[node] = [-1]
        else:
            g.log.error("Not able to get self heal daemon process "
                        "from node %s" % node)
            _rc = False
            glustershd_pids[node] = [-1]

    return _rc, glustershd_pids


def do_bricks_exist_in_shd_volfile(mnode, volname, brick_list):
    """
    Checks whether the given brick list is present in glustershd
    server volume file

    Args:
        mnode (str)         : Node on which commands will be executed.
        volname (str)       : Name of the volume.
        brick_list ( list ) : brick list of a volume which needs to
                              compare in glustershd server volume file

    Returns:
        bool : True if brick exists in glustershd server volume file.
               False Otherwise
    """
    GLUSTERSHD = "/var/lib/glusterd/glustershd/glustershd-server.vol"
    brick_list_server_vol = []
    volume_clients = "volume " + volname + "-client-"
    host = brick = None
    parse = False

    cmd = "cat {0}".format(GLUSTERSHD)
    ret, out, _ = g.run(mnode, cmd)
    if ret:
        g.log.error("Unable to cat the GLUSTERSHD file.")
        return False
    fd = out.split('\n')

    for each_line in fd:
        each_line = each_line.strip()
        if volume_clients in each_line:
            parse = True
        elif "end-volume" in each_line:
            if parse:
                brick_list_server_vol.append("%s:%s" % (host, brick))
            parse = False
        elif parse:
            if "option remote-subvolume" in each_line:
                brick = each_line.split(" ")[2]
            if "option remote-host" in each_line:
                host = each_line.split(" ")[2]

    g.log.info("Brick List from volume info : %s" % brick_list)
    g.log.info("Brick List from glustershd server volume "
               "file : %s" % brick_list_server_vol)

    if set(brick_list) != set(brick_list_server_vol):
        return False
    return True


def is_shd_daemonized(nodes, timeout=120):
    """
    wait for the glustershd process to release parent process.

    Args:
        nodes ( str|list ) : Node/Nodes of the cluster

    Kwargs:
        timeout (int): timeout value in seconds to wait for self-heal-daemons
        to be online.

    Returns:
        bool : True if glustershd releases its parent.
               False Otherwise

    """
    counter = 0
    flag = 0
    if not isinstance(nodes, list):
        nodes = [nodes]
    while counter < timeout:
        ret, pids = get_self_heal_daemon_pid(nodes)
        if not ret:
            g.log.info("Retry after 3 sec to get self heal "
                       "daemon process....")
            time.sleep(3)
            counter = counter + 3
        else:
            flag = 1
            break

    if not flag:
        g.log.error("Either No self heal daemon process found or more than"
                    "One self heal daemon process found even "
                    "after %d minutes", (timeout/60.0))
        return False
    else:
        g.log.info("Single self heal daemon process on all nodes %s",
                   nodes)
    return True


def bring_self_heal_daemon_process_offline(nodes):
    """
    Bring the self-heal daemon process offline for the nodes

    Args:
        nodes ( str|list ) : Node/Nodes of the cluster to bring
                             self-heal daemon process offline

    Returns:
        bool : True on successfully bringing self-heal daemon process offline.
               False otherwise
    """
    if not isinstance(nodes, list):
        nodes = [nodes]

    failed_nodes = []
    _rc = True

    g.log.info("Starting to get self heal daemon process on nodes %s" % nodes)
    ret, pids = get_self_heal_daemon_pid(nodes)
    if not ret:
        g.log.error("Either no self heal daemon process found or more than"
                    " one self heal daemon process found : %s" % pids)
        return False
    g.log.info("Successful in getting single self heal daemon process"
               " on all nodes %s", nodes)

    for node in pids:
        pid = pids[node][0]
        kill_cmd = "kill -SIGKILL %s" % pid
        ret, _, _ = g.run(node, kill_cmd)
        if ret != 0:
            g.log.error("Unable to kill the self heal daemon "
                        "process on %s" % node)
            failed_nodes.append(node)

    if failed_nodes:
        g.log.info("Unable to kill the self heal daemon "
                   "process on nodes %s" % nodes)
        _rc = False

    return _rc


def is_shd_daemon_running(mnode, node, volname):
    """
    Verifies whether the shd daemon is up and running on a particular node by
    checking the existence of shd pid and parsing the get volume status output.

    Args:
        mnode (str): The first node in servers list
        node (str): The node to be checked for whether the glustershd
                    process is up or not
        volname (str): Name of the volume created

    Returns:
        boolean: True if shd is running on the node, False, otherwise
    """

    # Get glustershd pid from node.
    ret, glustershd_pids = get_self_heal_daemon_pid(node)
    if not ret and glustershd_pids[node] != -1:
        return False
    # Verifying glustershd process is no longer running from get status.
    vol_status = get_volume_status(mnode, volname)
    if vol_status is None:
        return False
    try:
        _ = vol_status[volname][node]['Self-heal Daemon']
        return True
    except KeyError:
        return False
