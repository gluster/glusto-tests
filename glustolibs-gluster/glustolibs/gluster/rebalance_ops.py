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
    Description: Library for gluster rebalance operations.
"""

import time
from glusto.core import Glusto as g

try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree


def rebalance_start(mnode, volname, fix_layout=False, force=False):
    """Starts rebalance on the given volume.

    Example:
        rebalance_start("abc.com", testvol)

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        fix_layout (bool) : If this option is set to True, then rebalance
            start will get execute with fix-layout option. If set to False,
            then rebalance start will get executed without fix-layout option
        force (bool): If this option is set to True, then rebalance
            start will get execute with force option. If it is set to False,
            then rebalance start will get executed without force option

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """

    flayout = ''
    if fix_layout:
        flayout = "fix-layout"

    frce = ''
    if force:
        frce = 'force'

    if fix_layout and force:
        g.log.warning("Both fix-layout and force option is specified."
                      "Ignoring force option")
        frce = ''

    cmd = "gluster volume rebalance %s %s start %s" % (volname, flayout, frce)
    ret = g.run(mnode, cmd)
    return ret


def rebalance_stop(mnode, volname):
    """Stops rebalance on the given volume.

    Example:
        rebalance_stop("abc.com", testvol)

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
    """

    cmd = "gluster volume rebalance %s stop" % volname
    ret = g.run(mnode, cmd)
    return ret


def rebalance_status(mnode, volname):
    """Executes rebalance status on the given volume.

    Example:
        rebalance_status("abc.com", testvol)

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
    """

    cmd = "gluster volume rebalance %s status" % volname
    ret = g.run(mnode, cmd)
    return ret


def get_rebalance_status(mnode, volname):
    """Parse the output of 'gluster vol rebalance status' command
       for the given volume

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name

    Returns:
        NoneType: None if command execution fails, parse errors.
        dict: dict on success. rebalance status will be
            in dict format

    Examples:
        >>> get_rebalance_status('abc.lab.eng.xyz.com', testvol)
        {'node': [{'files': '0', 'status': '3', 'lookups': '0', 'skipped': '0',
        'nodeName': 'localhost', 'failures': '0', 'runtime': '0.00', 'id':
        '11336017-9561-4e88-9ac3-a94d4b403340', 'statusStr': 'completed',
        'size': '0'}, {'files': '0', 'status': '1', 'lookups': '0', 'skipped':
        '0', 'nodeName': '10.70.47.16', 'failures': '0', 'runtime': '0.00',
        'id': 'a2b88b10-eba2-4f97-add2-8dc37df08b27', 'statusStr':
        'in progress', 'size': '0'}, {'files': '0', 'status': '3',
        'lookups': '0', 'skipped': '0', 'nodeName': '10.70.47.152',
        'failures': '0', 'runtime': '0.00', 'id':
        'b15b8337-9f8e-4ec3-8bdb-200d6a67ae12', 'statusStr': 'completed',
        'size': '0'}, {'files': '0', 'status': '3', 'lookups': '0', 'skipped':
        '0', 'nodeName': '10.70.46.52', 'failures': '0', 'runtime': '0.00',
        'id': '77dc299a-32f7-43d8-9977-7345a344c398', 'statusStr': 'completed',
        'size': '0'}], 'task-id': 'a16f99d1-e165-40e7-9960-30508506529b',
        'aggregate': {'files': '0', 'status': '1', 'lookups': '0', 'skipped':
        '0', 'failures': '0', 'runtime': '0.00', 'statusStr': 'in progress',
        'size': '0'}, 'nodeCount': '4', 'op': '3'}
    """

    cmd = "gluster volume rebalance %s status --xml" % volname
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to execute 'rebalance status' on node %s. "
                    "Hence failed to get the rebalance status.", mnode)
        return None

    try:
        root = etree.XML(out)
    except etree.ParseError:
        g.log.error("Failed to parse the gluster rebalance status "
                    "xml output.")
        return None

    rebal_status = {}
    rebal_status["node"] = []
    for info in root.findall("volRebalance"):
        for element in info.getchildren():
            if element.tag == "node":
                status_info = {}
                for elmt in element.getchildren():
                    status_info[elmt.tag] = elmt.text
                rebal_status[element.tag].append(status_info)
            elif element.tag == "aggregate":
                status_info = {}
                for elmt in element.getchildren():
                    status_info[elmt.tag] = elmt.text
                rebal_status[element.tag] = status_info
            else:
                rebal_status[element.tag] = element.text
    return rebal_status


def rebalance_stop_and_get_status(mnode, volname):
    """Parse the output of 'gluster vol rebalance stop' command
       for the given volume

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name

    Returns:
        NoneType: None if command execution fails, parse errors.
        dict: dict on success. rebalance status will be
            in dict format

    Examples:
        >>> rebalance_stop_and_get_status('abc.xyz.com', testvol)
        {'node': [{'files': '0', 'status': '3', 'lookups': '0', 'skipped': '0',
        'nodeName': 'localhost', 'failures': '0', 'runtime': '0.00', 'id':
        '11336017-9561-4e88-9ac3-a94d4b403340', 'statusStr': 'completed',
        'size': '0'}, {'files': '0', 'status': '1', 'lookups': '0', 'skipped':
        '0', 'nodeName': '10.70.47.16', 'failures': '0', 'runtime': '0.00',
        'id': 'a2b88b10-eba2-4f97-add2-8dc37df08b27', 'statusStr':
        'in progress', 'size': '0'}, {'files': '0', 'status': '3',
        'lookups': '0', 'skipped': '0', 'nodeName': '10.70.47.152',
        'failures': '0', 'runtime': '0.00', 'id':
        'b15b8337-9f8e-4ec3-8bdb-200d6a67ae12', 'statusStr': 'completed',
        'size': '0'}, {'files': '0', 'status': '3', 'lookups': '0', 'skipped':
        '0', 'nodeName': '10.70.46.52', 'failures': '0', 'runtime': '0.00',
        'id': '77dc299a-32f7-43d8-9977-7345a344c398', 'statusStr': 'completed',
        'size': '0'}], 'task-id': 'a16f99d1-e165-40e7-9960-30508506529b',
        'aggregate': {'files': '0', 'status': '1', 'lookups': '0', 'skipped':
        '0', 'failures': '0', 'runtime': '0.00', 'statusStr': 'in progress',
        'size': '0'}, 'nodeCount': '4', 'op': '3'}
    """

    cmd = "gluster volume rebalance %s stop --xml" % volname
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to execute 'rebalance stop' on node %s. "
                    "Hence failed to parse the rebalance status.", mnode)
        return None

    try:
        root = etree.XML(out)
    except etree.ParseError:
        g.log.error("Failed to parse gluster rebalance stop xml output.")
        return None

    rebal_status = {}
    rebal_status["node"] = []
    for info in root.findall("volRebalance"):
        for element in info.getchildren():
            if element.tag == "node":
                status_info = {}
                for elmt in element.getchildren():
                    status_info[elmt.tag] = elmt.text
                rebal_status[element.tag].append(status_info)
            elif element.tag == "aggregate":
                status_info = {}
                for elmt in element.getchildren():
                    status_info[elmt.tag] = elmt.text
                rebal_status[element.tag] = status_info
            else:
                rebal_status[element.tag] = element.text
    return rebal_status


def wait_for_fix_layout_to_complete(mnode, volname, timeout=300):
    """Waits for the fix-layout to complete

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name

    Kwargs:
        timeout (int): timeout value in seconds to wait for rebalance
            to complete

    Returns:
        True on success, False otherwise

    Examples:
        >>> wait_for_fix_layout_to_complete("abc.com", "testvol")
    """

    count = 0
    while count < timeout:
        status_info = get_rebalance_status(mnode, volname)
        if status_info is None:
            return False

        status = status_info['aggregate']['statusStr']
        if status == 'fix-layout completed':
            g.log.info("Fix-layout is successfully completed")
            return True
        if status == 'fix-layout failed':
            g.log.error("Fix-layout failed on one or more nodes."
                        "Check rebalance status for more details")
            return False

        time.sleep(10)
        count = count + 10
    g.log.error("Fix layout has not completed. Wait timeout.")
    return False


def wait_for_rebalance_to_complete(mnode, volname, timeout=300):
    """Waits for the rebalance to complete

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name

    Kwargs:
        timeout (int): timeout value in seconds to wait for rebalance
            to complete

    Returns:
        True on success, False otherwise

    Examples:
        >>> wait_for_rebalance_to_complete("abc.com", "testvol")
    """

    count = 0
    while count < timeout:
        status_info = get_rebalance_status(mnode, volname)
        if status_info is None:
            return False

        status = status_info['aggregate']['statusStr']
        if status == 'completed':
            g.log.info("Rebalance is successfully completed")
            return True
        if status == 'failed':
            g.log.error(" Rebalance failed on one or more nodes."
                        "Check rebalance status for more details")
            return False

        time.sleep(10)
        count = count + 10
    g.log.error("Rebalance operation has not completed. Wait timeout.")
    return False


def get_remove_brick_status(mnode, volname, bricks_list):
    """Parse the output of 'gluster vol remove-brick status' command
       for the given volume

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name
        bricks_list (list): List of bricks participating in
        remove-brick operation

    Returns:
        NoneType: None if command execution fails, parse errors.
        dict: dict on success. rebalance status will be
            in dict format

    Examples:
        >>> get_remove_brick_status('abc.lab.eng.xyz.com', testvol, bricklist)
        {'node': [{'files': '0', 'status': '3', 'lookups': '0', 'skipped': '0'
            , 'nodeName': 'localhost', 'failures': '0', 'runtime': '0.00','id'
            : '6662bdcd-4602-4f2b-ac1a-75e6c85e780c', 'statusStr':
            'completed', 'size': '0'}], 'task-id': '6a135147-b202-4e69-
            b48c-b1c6408b9d24', 'aggregate': {'files': '0', 'status': '3',
                'lookups': '0', 'skipped': '0', 'failures': '0', 'runtime':
                '0.00', 'statusStr': 'completed', 'size': '0'}, 'nodeCount'
            : '3'}

    """

    cmd = ("gluster volume remove-brick %s %s status --xml" %
           (volname, ' '.join(bricks_list)))
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to execute 'remove-brick status' on node %s",
                    mnode)
        return None

    try:
        root = etree.XML(out)
    except etree.ParseError:
        g.log.error("Failed to parse the remove-brick status"
                    "xml output on volume %s", volname)
        return None

    remove_brick_status = {}
    remove_brick_status["node"] = []
    for info in root.findall("volRemoveBrick"):
        for element in info.getchildren():
            if element.tag == "node":
                status_info = {}
                for elmt in element.getchildren():
                    status_info[elmt.tag] = elmt.text
                remove_brick_status[element.tag].append(status_info)
            elif element.tag == "aggregate":
                status_info = {}
                for elmt in element.getchildren():
                    status_info[elmt.tag] = elmt.text
                remove_brick_status[element.tag] = status_info
            else:
                remove_brick_status[element.tag] = element.text
    return remove_brick_status


def wait_for_remove_brick_to_complete(mnode, volname, bricks_list,
                                      timeout=1200):
    """Waits for the remove brick to complete

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): volume name
        bricks_list (str): List of bricks participating in
        remove-brick operation

    Kwargs:
        timeout (int): timeout value in seconds to wait for remove brick
            to complete

    Returns:
        True on success, False otherwise

    Examples:
        >>> wait_for_remove_brick_to_complete("abc.com", "testvol")
    """

    count = 0
    while count < timeout:
        status_info = get_remove_brick_status(mnode, volname, bricks_list)
        if status_info is None:
            return False
        status = status_info['aggregate']['statusStr']
        if status == 'completed':
            g.log.info("Remove brick is successfully completed in %s sec",
                       count)
            return True
        elif status == 'failed':
            g.log.error(" Remove brick failed on one or more nodes. "
                        "Check remove brick status for more details")
            return False
        else:
            time.sleep(10)
            count += 10
            g.log.error("Remove brick operation has not completed. "
                        "Wait timeout is %s" % count)
    return False


def set_rebalance_throttle(mnode, volname, throttle_type='normal'):
    """Sets rebalance throttle

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        throttle_type (str): throttling type (lazy|normal|aggressive)
            Defaults to 'normal'

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        set_rebalance_throttle(mnode, testvol, throttle_type='aggressive')
    """
    cmd = ("gluster volume set {} rebal-throttle {}".format
           (volname, throttle_type))
    return g.run(mnode, cmd)
