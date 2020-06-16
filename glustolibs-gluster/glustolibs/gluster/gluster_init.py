#!/usr/bin/env python
#  Copyright (C) 2015-2020 Red Hat, Inc. <http://www.redhat.com>
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
    Description: This file contains the methods for starting/stopping glusterd
        and other initial gluster environment setup helpers.
"""
from time import sleep
from glusto.core import Glusto as g


def start_glusterd(servers):
    """Starts glusterd on specified servers if they are not running.

    Args:
        servers (str|list): A server|List of server hosts on which glusterd
            has to be started.

    Returns:
        bool : True if starting glusterd is successful on all servers.
            False otherwise.
    """
    if not isinstance(servers, list):
        servers = [servers]

    cmd = "pgrep glusterd || service glusterd start"
    results = g.run_parallel(servers, cmd)

    _rc = True
    for server, ret_values in results.items():
        retcode, _, _ = ret_values
        if retcode != 0:
            g.log.error("Unable to start glusterd on server %s", server)
            _rc = False
    if not _rc:
        return False

    return True


def stop_glusterd(servers):
    """Stops the glusterd on specified servers.

    Args:
        servers (str|list): A server|List of server hosts on which glusterd
            has to be stopped.

    Returns:
        bool : True if stopping glusterd is successful on all servers.
            False otherwise.
    """
    if not isinstance(servers, list):
        servers = [servers]

    cmd = "service glusterd stop"
    results = g.run_parallel(servers, cmd)

    _rc = True
    for server, ret_values in results.items():
        retcode, _, _ = ret_values
        if retcode != 0:
            g.log.error("Unable to stop glusterd on server %s", server)
            _rc = False
    if not _rc:
        return False

    return True


def restart_glusterd(servers):
    """Restart the glusterd on specified servers.

    Args:
        servers (str|list): A server|List of server hosts on which glusterd
            has to be restarted.

    Returns:
        bool : True if restarting glusterd is successful on all servers.
            False otherwise.
    """
    if not isinstance(servers, list):
        servers = [servers]

    cmd = "service glusterd restart"
    results = g.run_parallel(servers, cmd)

    _rc = True
    for server, ret_values in results.items():
        retcode, _, _ = ret_values
        if retcode != 0:
            g.log.error("Unable to restart glusterd on server %s", server)
            _rc = False
    if not _rc:
        return False

    return True


def reset_failed_glusterd(servers):
    """Reset-failed glusterd on specified servers.

    Args:
        servers (str|list): A server|List of server hosts on which glusterd
            has to be reset-failed.

    Returns:
        bool : True if reset-failed glusterd is successful on all servers.
            False otherwise.
    """
    if not isinstance(servers, list):
        servers = [servers]

    cmd = "systemctl reset-failed glusterd"
    results = g.run_parallel(servers, cmd)

    for server, (retcode, _, _) in results.items():
        if retcode:
            g.log.error("Unable to reset glusterd on server %s", server)
            return False
    return True


def is_glusterd_running(servers):
    """Checks the glusterd status on specified servers.

    Args:
        servers (str|list): A server|List of server hosts on which glusterd
            status has to be checked.

    Returns:
            0  : if glusterd running
            1  : if glusterd not running
           -1  : if glusterd not running and PID is alive

    """
    if not isinstance(servers, list):
        servers = [servers]

    cmd1 = "service glusterd status"
    cmd2 = "pidof glusterd"
    cmd1_results = g.run_parallel(servers, cmd1)
    cmd2_results = g.run_parallel(servers, cmd2)

    _rc = 0
    for server, ret_values in cmd1_results.items():
        retcode, _, _ = ret_values
        if retcode != 0:
            g.log.error("glusterd is not running on the server %s", server)
            _rc = 1
            if cmd2_results[server][0] == 0:
                g.log.error("PID of glusterd is alive and status is not "
                            "running")
                _rc = -1
    return _rc


# TODO: THIS IS NOT IMPLEMENTED YET. PLEASE DO THIS MANUALLY
# TILL WE IMPLEMENT THIS
def env_setup_servers(servers):
    """Set up environment on all the specified servers.

    Args:
        servers (str|list): A server|List of server hosts on which environment
            has to be setup.

    Returns:
        bool : True if setting up environment is successful on all servers.
            False otherwise.

    """
    if not isinstance(servers, list):
        servers = [servers]

    g.log.info("The function isn't implemented fully")
    g.log.info("Please setup the bricks manually.")

    if not start_glusterd(servers):
        return False

    return True


def get_glusterd_pids(nodes):
    """
    Checks if glusterd process is running and
    return the process id's in dictionary format

    Args:
        nodes (str|list) : Node(s) of the cluster

    Returns:
        tuple : Tuple containing two elements (ret, gluster_pids).
        The first element 'ret' is of type 'bool', True if only if
        glusterd is running on all the nodes in the list and each
        node contains only one instance of glusterd running.
        False otherwise.

        The second element 'glusterd_pids' is of type dictonary and
        it contains the process ID's for glusterd.

    """
    glusterd_pids = {}
    _rc = True
    if not isinstance(nodes, list):
        nodes = [nodes]

    cmd = "pidof glusterd"
    g.log.info("Executing cmd: %s on node %s", cmd, nodes)
    results = g.run_parallel(nodes, cmd)
    for node in results:
        ret, out, _ = results[node]
        if ret == 0:
            if len(out.strip().split("\n")) == 1:
                if not out.strip():
                    g.log.error("NO glusterd process found "
                                "on node %s", node)
                    _rc = False
                    glusterd_pids[node] = ['-1']
                else:
                    g.log.info("glusterd process with "
                               "pid %s found on %s",
                               out.strip().split("\n"), node)
                    glusterd_pids[node] = (out.strip().split("\n"))
            else:
                g.log.error("More than one glusterd process "
                            "found on node %s", node)
                _rc = False
                glusterd_pids[node] = out
        else:
            g.log.error("Not able to get glusterd process "
                        "from node %s", node)
            _rc = False
            glusterd_pids[node] = ['-1']

    return _rc, glusterd_pids


def wait_for_glusterd_to_start(servers, glusterd_start_wait_timeout=80):
    """Checks glusterd is running on nodes with timeout.

    Args:
        servers (str|list): A server|List of server hosts on which glusterd
            status has to be checked.
        glusterd_start_wait_timeout: timeout to retry glusterd running
            check in node.

    Returns:
    bool : True if glusterd is running on servers.
        False otherwise.

    """
    if not isinstance(servers, list):
        servers = [servers]
    count = 0
    while count <= glusterd_start_wait_timeout:
        ret = is_glusterd_running(servers)
        if not ret:
            g.log.info("glusterd is running on %s", servers)
            return True
        sleep(1)
        count += 1
    g.log.error("glusterd is not running on %s", servers)
    return False


def get_gluster_version(host):
    """Checks the gluster version on the nodes

    Args:
        host(str): IP of the host whose gluster version has to be checked.

    Returns:
        (float): The gluster version value.
    """
    command = 'gluster --version'
    _, out, _ = g.run(host, command)
    g.log.info("The Gluster verion of the cluster under test is %s",
               out)
    return float(out.split(' ')[1])
