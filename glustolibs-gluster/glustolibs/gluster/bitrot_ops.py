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
    Description: Library for gluster bitrot operations.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_options, get_volume_status
from glustolibs.gluster.lib_utils import (get_pathinfo,
                                          calculate_checksum,
                                          get_extended_attributes_info)
import time
import re


# Global variables
SCRUBBER_TIMEOUT = 100


def enable_bitrot(mnode, volname):
    """Enables bitrot for given volume

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
        enable_bitrot("abc.com", testvol)
    """

    cmd = "gluster volume bitrot %s enable" % volname
    return g.run(mnode, cmd)


def disable_bitrot(mnode, volname):
    """Disables bitrot for given volume

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
        disable_bitrot("abc.com", testvol)
    """

    cmd = "gluster volume bitrot %s disable" % volname
    return g.run(mnode, cmd)


def is_bitrot_enabled(mnode, volname):
    """Checks if bitrot is enabled on given volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        True on success, False otherwise

    Example:
        is_bitrot_enabled("abc.com", testvol)
    """

    output = get_volume_options(mnode, volname, "features.bitrot")
    if output is None:
        return False

    g.log.info("Bitrot Status in volume %s: %s"
               % (volname, output["features.bitrot"]))
    if output["features.bitrot"] != 'on':
        return False

    return True


def is_file_signed(mnode, filename, volname, expected_file_version=None):
    """Verifies if the given file is signed

    Args:
        mnode (str): Node on which cmd has to be executed.
        filename (str): relative path of filename to be verified
        volname (str): volume name

    Kwargs:
        expected_file_version (str): file version to check with getfattr output
            If this option is set, this function
            will check file versioning as part of signing verification.
            If this option is set to None, function will not check
            for file versioning. Defaults to None.

    Returns:
        True on success, False otherwise

    Example:
        is_file_signed("abc.com", 'file1', "testvol",
                       expected_file_version='2')
    """

    # Getting file path in the rhs node
    file_location = get_pathinfo(mnode, filename, volname)
    if file_location is None:
        g.log.error("Failed to get backend file path in is_file_signed()")
        return False

    path_info = file_location[0].split(':')

    expected_file_signature = (calculate_checksum(path_info[0],
                                                  [path_info[1]])
                               [path_info[1]])

    attr_info = get_extended_attributes_info(path_info[0],
                                             [path_info[1]])
    if attr_info is None:
        g.log.error("Failed to get attribute info in is_file_signed()")
        return False

    if 'trusted.bit-rot.signature' in attr_info[path_info[1]]:
        file_signature = attr_info[path_info[1]]['trusted.bit-rot.signature']
    else:
        g.log.error("trusted.bit-rot.signature attribute not present "
                    " for file %s" % filename)
        return False

    if expected_file_version is not None:
        expected_file_version = ('{0:02d}'.format(int(
                                 expected_file_version))).ljust(16, '0')
        actual_signature_file_version = re.findall('.{16}',
                                                   file_signature[4:]).pop(0)

        # Verifying file version after signing
        if actual_signature_file_version != expected_file_version:
            g.log.error("File version mismatch in signature.Filename: %s ."
                        "Expected file version: %s.Actual file version: %s"
                        % (filename, expected_file_version,
                           actual_signature_file_version))
            return False

    actual_file_signature = ''.join(re.findall('.{16}',
                                               file_signature[4:])[1:])

    # Verifying file signature
    if actual_file_signature != expected_file_signature:
        g.log.error("File signature mismatch. File name: %s . Expected "
                    "file signature: %s. Actual file signature: %s"
                    % (filename, expected_file_signature,
                       actual_file_signature))
        return False
    return True


def is_file_bad(mnode, filename):
    """Verifies if scrubber identifies bad file
    Args:
        filename (str): absolute path of the file in mnode
        mnode (str): Node on which cmd has to be executed.

    Returns:
        True on success, False otherwise

    Example:
        is_file_bad("abc.xyz.com", "/bricks/file1")
    """
    ret = True
    count = 0
    flag = 0
    while (count < SCRUBBER_TIMEOUT):
        attr_info = get_extended_attributes_info(mnode, [filename])
        if attr_info is None:
            ret = False

        if 'trusted.bit-rot.bad-file' in attr_info[filename]:
            flag = 1
            break

        time.sleep(10)
        count = count + 10
    if not flag:
        g.log.error("Scrubber failed to identify bad file")
        ret = False

    return ret


def bring_down_bitd(mnode):
    """Brings down bitd process
    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        True on success, False otherwise

    Example:
        bring_down_bitd("abc.xyz.com")
    """

    kill_cmd = ("pid=`cat /var/lib/glusterd/bitd/run/bitd.pid` && "
                "kill -15 $pid || kill -9 $pid")
    ret, _, _ = g.run(mnode, kill_cmd)
    if ret != 0:
        g.log.error("Unable to kill the bitd for %s"
                    % mnode)
        return False
    else:
        return True


def bring_down_scrub_process(mnode):
    """Brings down scrub process
    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        True on success, False otherwise

    Example:
        bring_down_scrub_process("abc.xyz.com")
    """

    kill_cmd = ("pid=`cat /var/lib/glusterd/scrub/run/scrub.pid` && "
                "kill -15 $pid || kill -9 $pid")

    ret, _, _ = g.run(mnode, kill_cmd)
    if ret != 0:
        g.log.error("Unable to kill the scrub process for %s"
                    % mnode)
        return False
    else:
        return True


def set_scrub_throttle(mnode, volname, throttle_type='lazy'):
    """Sets scrub throttle

    Args:
        volname (str): volume name
        mnode (str): Node on which cmd has to be executed.

    Kwargs:
        throttle_type (str): throttling type (lazy|normal|aggressive)
            Defaults to 'lazy'

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        set_scrub_throttle(mnode, testvol)
    """

    cmd = ("gluster volume bitrot %s scrub-throttle %s"
           % (volname, throttle_type))
    return g.run(mnode, cmd)


def set_scrub_frequency(mnode, volname, frequency_type='biweekly'):
    """Sets scrub frequency

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        frequency_type (str): frequency type (hourly|daily|weekly|biweekly|
            monthly). Defaults to 'biweekly'

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        set_scrub_frequency("abc.com", testvol)
    """

    cmd = ("gluster volume bitrot %s scrub-frequency %s"
           % (volname, frequency_type))
    return g.run(mnode, cmd)


def pause_scrub(mnode, volname):
    """Pauses scrub

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
        pause_scrub("abc.com", testvol)
    """

    cmd = "gluster volume bitrot %s scrub pause" % volname
    return g.run(mnode, cmd)


def resume_scrub(mnode, volname):
    """Resumes scrub

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
        resume_scrub("abc.com", testvol)
    """

    cmd = "gluster volume bitrot %s scrub resume" % volname
    return g.run(mnode, cmd)


def get_bitd_pid(mnode):
    """Gets bitd process id for the given node
    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        str: pid of the bitd process on success
        NoneType: None if command execution fails, errors.

    Example:
        get_bitd_pid("abc.com")
    """

    cmd = ("cat /var/lib/glusterd/bitd/run/bitd.pid")
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Unable to get bitd pid for %s"
                    % mnode)
        return None

    return out.strip("\n")


def get_scrub_process_pid(mnode):
    """Gets scrub process id for the given node
    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        str: pid of the scrub process on success
        NoneType: None if command execution fails, errors.

    Example:
        get_scrub_process_pid("abc.com")
    """

    cmd = ("cat /var/lib/glusterd/scrub/run/scrub.pid")
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Unable to get scrub pid for %s"
                    % mnode)
        return None

    return out.strip("\n")


def is_bitd_running(mnode, volname):
    """Checks if bitd is running on the given node

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        True on success, False otherwise

    Example:
        is_bitd_running("abc.com", "testvol")
    """

    vol_status = get_volume_status(mnode, volname=volname)
    if vol_status is None:
        g.log.error("Failed to get volume status in is_bitd_running()")
        return False

    if 'Bitrot Daemon' not in vol_status[volname][mnode]:
        g.log.error("Bitrot is not enabled in volume %s"
                    % volname)
        return False

    bitd_status = vol_status[volname][mnode]['Bitrot Daemon']['status']
    if bitd_status != '1':
        g.log.error("Bitrot Daemon is not running in node %s"
                    % mnode)
        return False
    return True


def is_scrub_process_running(mnode, volname):
    """Checks if scrub process is running on the given node

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        True on success, False otherwise

    Example:
        is_scrub_process_running("abc.com", "testvol")
    """

    vol_status = get_volume_status(mnode, volname=volname)
    if vol_status is None:
        g.log.error("Failed to get volume status in "
                    "is_scrub_process_running()")
        return False

    if 'Scrubber Daemon' not in vol_status[volname][mnode]:
        g.log.error("Bitrot is not enabled in volume %s"
                    % volname)
        return False

    bitd_status = vol_status[volname][mnode]['Scrubber Daemon']['status']
    if bitd_status != '1':
        g.log.error("Scrubber Daemon is not running in node %s"
                    % mnode)
        return False
    return True


def scrub_status(mnode, volname):
    """Executes gluster bitrot scrub status command

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
        scrub_status("abc.com", testvol)
    """

    cmd = "gluster volume bitrot %s scrub status" % volname
    return g.run(mnode, cmd)


def get_scrub_status(mnode, volname):
    """Parse the output of gluster bitrot scrub status command

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        dict: scrub status in dict format
        NoneType: None if command execution fails, errors.

    Example:
        >>>get_scrub_status("abc.com", testvol)
        {'State of scrub': 'Active', 'Bitrot error log location':
        '/var/log/glusterfs/bitd.log', 'Scrub impact': 'aggressive',
        'Scrub frequency': 'hourly', 'status_info': {'localhost':
        {'Duration of last scrub (D:M:H:M:S)': '0:0:0:0', 'corrupted_gfid':
        ['475ca13f-577f-460c-a5d7-ea18bb0e7779'], 'Error count': '1',
        'Last completed scrub time': '2016-06-21 12:46:19',
        'Number of Skipped files': '0', 'Number of Scrubbed files': '0'},
        '10.70.47.118': {'Duration of last scrub (D:M:H:M:S)': '0:0:0:1',
        'corrupted_gfid': ['19e62b26-5942-4867-a2f6-e354cd166da9',
        'fab55c36-0580-4d11-9ac0-d8e4e51f39a0'], 'Error count': '2',
        'Last completed scrub time': '2016-06-21 12:46:03',
        'Number of Skipped files': '0', 'Number of Scrubbed files': '2'}},
        'Volume name': 'testvol', 'Scrubber error log location':
        '/var/log/glusterfs/scrub.log'}
    """

    cmd = "gluster volume bitrot %s scrub status" % volname
    ret, out, err = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Unable to get scrub status for volume %s"
                    % volname)
        return None

    match = re.search('(.*?)(==.*==.*)', out, re.S)
    if match is None:
        g.log.error("Mismatch in regex. Scrub status raw output is not"
                    " in expected format")
        return None
    info = match.group(2).replace('\n\n', '\n')

    if "Corrupted object's [GFID]" in info:
        info = info.replace("Corrupted object's [GFID]:\n",
                            "Corrupted object's [GFID]:")
        regex = 'Node(?:(?!Node).)*?Corrupted object.*?:.*?\n+='
        temp_list = re.findall(regex, info, re.S)
        corrupt_list = []
        for node in temp_list:
            tmp_reg = ('Node: (\S+)\n.*Error count.*'
                       'Corrupted object.*?:(.*)\n=.*')
            m = re.search(tmp_reg, node, re.S)
            if m is None:
                g.log.error("Mismatch in cli output when bad file"
                            "is identified")
                return None
            corrupt_list.append(m.groups())
    else:
        corrupt_list = []
    info_list = re.findall('Node:.*?\n.*:.*\n.*:.*\n.*:.*\n.*:.*\n.*:.*\n+',
                           info)
    temp_list = []
    for item in info_list:
        item = item.replace('\n\n', '')
        temp_list.append(item)

    tmp_dict1 = {}
    for item in temp_list:
        tmp = item.split('\n')
        tmp_0 = tmp[0].split(':')
        tmp.pop(0)
        tmp_dict = {}
        for tmp_item in tmp[:-1]:
            tmp_1 = tmp_item.split(': ')
            tmp_dict[tmp_1[0].strip(' ')] = tmp_1[1].strip(' ')
        tmp_dict1[tmp_0[1].strip(' ')] = tmp_dict
    status_dict = {}
    for item in match.group(1).split('\n\n')[:-1]:
        elmt = item.split(':')
        tmp_elmt = elmt[1].strip(' ').strip('\n')
        status_dict[elmt[0].strip(' ').strip('\n')] = tmp_elmt

    status_dict['status_info'] = tmp_dict1
    for elmt in corrupt_list:
        if elmt[0].strip(' ') in status_dict['status_info'].keys():
            val = elmt[1].split('\n')
            val = filter(None, val)
            gfid = "corrupted_gfid"
            status_dict['status_info'][elmt[0].strip(' ')][gfid] = val
    return status_dict
