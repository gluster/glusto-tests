#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY :or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

""" Description: Module for gluster quota related helper functions. """


from glusto.core import Glusto as g
from glustolibs.gluster.quota_ops import (quota_fetch_list)


def quota_validate(mnode, volname, path, **kwargs):
    """ Validate if the hard limit, soft limit, usage match the expected values.
        If any of the arguments are None, they are not verified.

    Args:
        mnode (str)             : Node on which command has to be executed.
        volname (str)           : volume name.
        path (str)              : Path to be verified.
        kwargs
        hard_limit(int)         : hard limit is verified with this value.
        soft_limit_percent(int) : soft limit (in %) is verified with this value
        used_space(int)         : if set, usage as displayed in quota list is
                                  verified with expected value.
        avail_space(int)         : if set, usage as displayed in quota list is
                                  verified with expected value.
        sl_exceeded(bool)       : expected value of soft limit flag.
        hl_exceeded(bool)       : expected value of hard limit flag.

    """

    if kwargs is None:
        g.log.error("No arguments given for validation")
        return False

    quotalist = quota_fetch_list(mnode, volname, path)

    if path not in quotalist:
        g.log.error("Path not found (script issue) path: %s", path)
        return False
    else:
        listinfo = quotalist[path]

    ret = True
    for key, value in kwargs.iteritems():
        if key and listinfo[key] != value:
            g.log.error("%s = %s does not match with expected value %s",
                        key, str(listinfo[key]), str(value))
            ret = False

    return ret


def quota_fetch_daemon_pid(nodes):
    """
    Checks if quota daemon process is running and
    return the process id's in dictionary format

    Args:
        nodes ( str|list ) : Node/Nodes of the cluster

    Returns:
        tuple : Tuple containing two elements (ret, quotad_pids).
        The first element 'ret' is of type 'bool', True if and only if
        quotad is running on all the nodes in the list and each
        node contains only one instance of quotad running.
        False otherwise.

        The second element 'quotad_pids' is of type dictonary and it
        contains the 'nodes' as the key and 'quotad PID' as the value.

        If there is NO quota daemon running on few nodes, the first element
        will be 'False' and the nodes which do not have a quota daemon running
        will have a value as '-1'.

        If there are MORE THAN ONE quota daemon for a node, the first element
        will be 'False' and the value for that node will be '-1'.

        Example:
            quota_fetch_daemon_pid(["node1", "node2"])
            (False, {'node2': ['8012'], 'node1': [-1]})

            Here 'node1' doesn't have quota daemon running. Hence, value
            of 'node1' is '-1'.
    """
    quotad_pids = {}
    _rc = True
    if isinstance(nodes, str):
        nodes = [nodes]
    cmd = "pgrep -f quotad | grep -v ^$$\$"
    g.log.info("Executing cmd: %s on node %s" % (cmd, nodes))
    results = g.run_parallel(nodes, cmd)
    for node in results:
        ret, out, err = results[node]
        if ret == 0:
            if len(out.strip().split("\n")) == 1:
                if not out.strip():
                    g.log.info("NO Quota daemon process found "
                               "on node %s" % node)
                    _rc = False
                    quotad_pids[node] = [-1]
                else:
                    g.log.info("Single Quota Daemon process with "
                               "pid %s found on %s",
                               out.strip().split("\n"), node)
                    quotad_pids[node] = (out.strip().split("\n"))
            else:
                g.log.info("More than One Quota daemon process "
                           "found on node %s" % node)
                _rc = False
                quotad_pids[node] = [-1]
        else:
            g.log.info("Not able to get Quota daemon process "
                       "from node %s" % node)
            _rc = False
            quotad_pids[node] = [-1]

    return _rc, quotad_pids
