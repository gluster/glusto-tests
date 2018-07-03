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
    Description: Library for gluster quota operations.
"""
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_options


def quota_enable(mnode, volname):
    """Enables quota on given volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        quota_enable("abc.xyz.com", testvol)
    """

    cmd = "gluster volume quota %s enable" % volname
    return g.run(mnode, cmd)


def quota_disable(mnode, volname):
    """Disables quota on given volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        quota_disable("abc.xyz.com", testvol)
    """

    cmd = "gluster volume quota %s disable --mode=script" % volname
    return g.run(mnode, cmd)


def is_quota_enabled(mnode, volname):
    """Checks if quota is enabled on given volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        bool: True, if quota is enabled
            False, if quota is disabled

    Example:
        is_quota_enabled(mnode, testvol)
    """

    output = get_volume_options(mnode, volname, "features.quota")
    if output is None:
        return False

    g.log.info("Quota Status in volume %s %s",
               volname, output["features.quota"])
    if output["features.quota"] != 'on':
        return False

    return True


def quota_check_deem_statfs(mnode, volname):
    """Checks if quota-deem-statfs is enabled
    on given volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        bool: True, if quota-deem-statfs is enabled
            False, if quota-deem-statfs is disabled

    Example:
        quota_check_deem_statfs(mnode, testvol)
    """

    output = get_volume_options(mnode, volname,
                                "features.quota-deem-statfs")
    if not output:
        g.log.error("Failed to get current status of "
                    "'features.quota-deem-statfs' option "
                    "for volume %s", volname)
        return False

    g.log.info("Quota deem-statfs status in volume %s: %s",
               volname, output["features.quota-deem-statfs"])

    if 'features.quota-deem-statfs' in output.keys():
        if output['features.quota-deem-statfs'] == 'on':
            g.log.info("Volume option 'features.quota-deem-statfs' "
                       "is currently enabled for volume %s", volname)
            return True
        g.log.info("Volume option 'features.quota-deem-statfs' "
                   "is not in enabled state for volume %s", volname)
        return False
    g.log.error("Failed to get status of 'features.quota-deem-statfs' "
                "option for the volume %s", volname)
    return False


def quota_limit_usage(mnode, volname, path='/', limit='100GB',
                      soft_limit=''):
    """Sets limit-usage on the path of the specified volume to
        specified limit

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        path (str): path to which quota limit usage is set.
            Defaults to /.
        limit (str): quota limit usage. defaults to 100GB
        soft_limit (str): quota soft limit to be set

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Examples:
        >>> quota_limit_usage("abc.com", "testvol")

    """

    cmd = ("gluster volume quota %s limit-usage %s %s %s --mode=script"
           % (volname, path, limit, soft_limit))
    return g.run(mnode, cmd)


def quota_fetch_list(mnode, volname, path=None):
    """Parse the output of 'gluster quota list' command.

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name

    Kwargs:
        path (str): Quota path

    Returns:
        NoneType: None if command execution fails, parse errors.
        dict: dict on success.

    Examples:
        >>> quota_fetch_list('abc.lab.eng.xyz.com', "testvol")
        {'/': {'used_space': '0', 'hl_exceeded': 'No', 'soft_limit_percent':
        '60%', 'avail_space': '2147483648', 'soft_limit_value': '1288490188',
        'sl_exceeded': 'No', 'hard_limit': '2147483648'}}
    """
    if not path:
        path = ''

    cmd = "gluster volume quota %s list %s --xml" % (volname, path)
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to execute 'quota list' on node %s. "
                    "Hence failed to get the quota list.", mnode)
        return None

    try:
        root = etree.XML(out)
    except etree.ParseError:
        g.log.error("Failed to parse the gluster quota list xml output.")
        return None

    quotalist = {}
    for path in root.findall("volQuota/limit"):
        for elem in path.getchildren():
            if elem.tag == "path":
                path = elem.text
                quotalist[path] = {}
            elif elem.text == 'N/A':
                quotalist[path][elem.tag] = elem.text
            elif elem.tag in ("hard_limit", "soft_limit_value",
                              "used_space", "avail_space"):
                quotalist[path][elem.tag] = int(elem.text)
            elif elem.tag == "soft_limit_percent":
                quotalist[path][elem.tag] = int(elem.text[:-1])
            elif elem.tag in ("sl_exceeded", "hl_exceeded"):
                quotalist[path][elem.tag] = bool(elem.text == 'Yes')
            else:
                g.log.error("Failed to parse the gluster quota"
                            "list xml output.")
                return None
    return quotalist


def quota_limit_objects(mnode, volname, path='/', limit='10',
                        soft_limit=''):
    """Sets limit-objects on the path of the specified volume to
        specified limit

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name

    Kwargs:
        path (str): path to which quota limit usage is set.
            Defaults to /.
        limit (str): quota limit objects. defaults to 10.
        soft_limit (str): quota soft limit to be set

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Examples:
        >>> quota_limit_objects("abc.com", "testvol")

    """

    cmd = ("gluster volume quota %s limit-objects %s %s %s --mode=script"
           % (volname, path, limit, soft_limit))
    return g.run(mnode, cmd)


def quota_fetch_list_objects(mnode, volname, path=None):
    """Parse the output of 'gluster quota list-objects' command.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        path (str): Quota path

    Returns:
        NoneType: None if command execution fails, parse errors.
        dict: dict of dict on success.

    Examples:
        >>> quota_fetch_list_objects('abc.lab.eng.xyz.com', "testvol")
        {'/': {'available': '7', 'hl_exceeded': 'No', 'soft_limit_percent':
        '80%', 'soft_limit_value': '8', 'dir_count': '3', 'sl_exceeded':
        'No', 'file_count': '0', 'hard_limit': '10'}}
    """

    if not path:
        path = ''

    cmd = "gluster volume quota %s list-objects %s --xml" % (volname, path)
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to execute 'quota list' on node %s. "
                    "Hence failed to get the quota list.", mnode)
        return None

    try:
        root = etree.XML(out)
    except etree.ParseError:
        g.log.error("Failed to parse the gluster quota list xml output.")
        return None

    quotalist = {}
    for path in root.findall("volQuota/limit"):
        for elem in path.getchildren():
            if elem.tag == "path":
                path = elem.text
                quotalist[path] = {}
            else:
                quotalist[path][elem.tag] = elem.text
    return quotalist


def quota_set_alert_time(mnode, volname, time):
    """Sets quota alert time

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        time (str): quota alert time in seconds

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Examples:
        >>> quota_set_alert_time("abc.com", "testvol", <alert time>)

    """

    cmd = ("gluster volume quota %s alert-time %s --mode=script"
           % (volname, time))
    return g.run(mnode, cmd)


def quota_set_soft_timeout(mnode, volname, timeout):
    """Sets quota soft timeout

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name
        timeout (str): quota soft limit timeout value

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Examples:
        >>> quota_set_soft_timeout("abc.com", "testvol", <timeout-value>)

    """

    cmd = ("gluster volume quota %s soft-timeout %s --mode=script"
           % (volname, timeout))
    return g.run(mnode, cmd)


def quota_set_hard_timeout(mnode, volname, timeout):
    """Sets quota hard timeout

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name
        timeout (str): quota hard limit timeout value

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Examples:
        >>> quota_set_hard_timeout("abc.com", "testvol", <timeout-value>)

    """

    cmd = ("gluster volume quota %s hard-timeout %s --mode=script"
           % (volname, timeout))
    return g.run(mnode, cmd)


def quota_set_default_soft_limit(mnode, volname, timeout):
    """Sets quota default soft limit

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name
        timeout (str): quota soft limit timeout value

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Examples:
        >>> quota_set_default_soft_limit("abc.com", "testvol",
                                         <timeout-value>)

    """

    cmd = ("gluster volume quota %s default-soft-limit %s --mode=script"
           % (volname, timeout))
    return g.run(mnode, cmd)


def quota_remove(mnode, volname, path):
    """Removes quota for the given path

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name
        path (str): path to which quota limit usage is set.
            Defaults to /.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Examples:
        >>> quota_remove("abc.com", "testvol", <path>)

    """

    cmd = "gluster volume quota %s remove %s --mode=script" % (volname, path)
    return g.run(mnode, cmd)


def quota_remove_objects(mnode, volname, path):
    """Removes quota objects for the given path

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name
        path (str): path to which quota limit usage is set.
            Defaults to /.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Examples:
        >>> quota_remove_objects("abc.com", "testvol", <path>)

    """

    cmd = ("gluster volume quota %s remove-objects %s --mode=script"
           % (volname, path))
    return g.run(mnode, cmd)
