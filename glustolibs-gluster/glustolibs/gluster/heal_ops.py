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
    Description: Module for gluster heal operations.
"""

from glusto.core import Glusto as g
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree


def trigger_heal(mnode, volname):
    """Triggers heal on the volume.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        bool : True if heal is triggered successfully. False otherwise.
    """
    cmd = "gluster volume heal %s" % volname
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        return False

    return True


def trigger_heal_full(mnode, volname):
    """Triggers heal 'full' on the volume.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        bool : True if heal is triggered successfully. False otherwise.
    """
    cmd = "gluster volume heal %s full" % volname
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        return False

    return True


def enable_heal(mnode, volname):
    """Enable heal by executing 'gluster volume heal enable'
        for the specified volume.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        bool : True if heal is enabled on the volume.
            False otherwise.
    """
    cmd = "gluster volume heal %s enable" % volname
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        return False

    return True


def disable_heal(mnode, volname):
    """Disable heal by executing 'gluster volume heal disable'
        for the specified volume.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        bool : True if heal is disabled on the volume.
            False otherwise.
    """
    cmd = "gluster volume heal %s disable" % volname
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        return False

    return True


def enable_self_heal_daemon(mnode, volname):
    """Enables self-heal-daemon on a volume by setting volume option
        'self-heal-daemon' to value 'on'

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        bool : True if setting self_heal_daemon option to 'on' is successful.
            False otherwise.
    """
    cmd = "gluster volume set %s self-heal-daemon on" % volname
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        return False

    return True


def disable_self_heal_daemon(mnode, volname):
    """Disables self-heal-daemon on a volume by setting volume option
        'self-heal-daemon' to value 'off'

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        bool : True if setting self_heal_daemon option to 'off' is successful.
            False otherwise.
    """
    cmd = "gluster volume set %s self-heal-daemon off" % volname
    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        return False

    return True


def heal_info(mnode, volname):
    """Get heal info for the volume by executing:
        'gluster volume heal <volname> info'

    Args:
        mnode : Node on which commands are executed.
        volname : Name of the volume

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    cmd = "gluster volume heal %s info" % volname
    return g.run(mnode, cmd)


def heal_info_summary(mnode, volname):
    """Get heal info summary i.e Bricks and it's corresponding number of
        entries, status.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    cmd = (r"gluster volume heal %s info | grep 'entries\|Brick\|Status'" %
           volname)
    return g.run(mnode, cmd)


def heal_info_healed(mnode, volname):
    """Get healed entries information for the volume by executing:
        'gluster volume heal <volname> info healed'

    Args:
        mnode : Node on which commands are executed.
        volname : Name of the volume

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    cmd = "gluster volume heal %s info healed" % volname
    return g.run(mnode, cmd)


def heal_info_heal_failed(mnode, volname):
    """Get entries on which heal failed for the volume by executing:
        'gluster volume heal <volname> info heal-failed'

    Args:
        mnode : Node on which commands are executed.
        volname : Name of the volume

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    cmd = "gluster volume heal %s info heal-failed" % volname
    return g.run(mnode, cmd)


def heal_info_split_brain(mnode, volname):
    """Get entries that are in split-brain state for the volume by executing:
        'gluster volume heal <volname> info split-brain'

    Args:
        mnode : Node on which commands are executed.
        volname : Name of the volume

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    cmd = "gluster volume heal %s info split-brain" % volname
    return g.run(mnode, cmd)


def get_heal_info(mnode, volname):
    """From the xml output of heal info command get the heal info data.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        NoneType: None if parse errors.
        list: list of dictionaries. Each element in the list is the
            heal_info data per brick.
    """
    cmd = "gluster volume heal %s info --xml" % volname
    ret, out, _ = g.run(mnode, cmd, log_level='DEBUG')
    if ret != 0:
        g.log.error("Failed to get the heal info xml output for the volume %s."
                    "Hence failed to get the heal info summary." % volname)
        return None

    try:
        root = etree.XML(out)
    except etree.ParseError:
        g.log.error("Failed to parse the gluster heal info xml output.")
        return None

    heal_info_data = []
    for brick in root.findall("healInfo/bricks/brick"):
        brick_heal_info = {}
        brick_files_to_heal = []
        file_to_heal_exist = False
        for element in brick:
            if element.tag == "file":
                file_to_heal_exist = True
                file_info = {}
                file_info[element.attrib['gfid']] = element.text
                brick_files_to_heal.append(file_info)

            else:
                brick_heal_info[element.tag] = element.text
        if file_to_heal_exist:
            brick_heal_info['file'] = brick_files_to_heal
        heal_info_data.append(brick_heal_info)
    return heal_info_data


def get_heal_info_summary(mnode, volname):
    """From the xml output of heal info command  get heal info summary
        i.e Bricks and it's corresponding number of entries, status.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        NoneType: None if parse errors.
        dict: dict of dictionaries. brick names are the keys of the dict with
            each key having brick's status, numberOfEntries info as dict.
            Example:
                heal_info_summary_data = {
                    'ijk.lab.eng.xyz.com': {
                        'status': 'Connected'
                        'numberOfEntries': '11'
                        },
                    'def.lab.eng.xyz.com': {
                        'status': 'Transport endpoint is not connected',
                        'numberOfEntries': '-'
                        }
                    }

    """
    heal_info_data = get_heal_info(mnode, volname)
    if heal_info_data is None:
        g.log.error("Unable to get heal info summary for the volume %s" %
                    volname)
        return None

    heal_info_summary_data = {}
    for info_data in heal_info_data:
        heal_info_summary_data[info_data['name']] = {
            'status': info_data['status'],
            'numberOfEntries': info_data['numberOfEntries']
        }
    return heal_info_summary_data


def get_heal_info_split_brain(mnode, volname):
    """From the xml output of heal info split-brain command get the
        heal info split-brain data.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        NoneType: None if parse errors.
        list: list of dictionaries. Each element in the list is the
            heal_info_split_brain data per brick.
    """
    cmd = "gluster volume heal %s info split-brain --xml" % volname
    ret, out, _ = g.run(mnode, cmd, log_level='DEBUG')
    if ret != 0:
        g.log.error("Failed to get the heal info xml output for the volume %s."
                    "Hence failed to get the heal info summary." % volname)
        return None

    try:
        root = etree.XML(out)
    except etree.ParseError:
        g.log.error("Failed to parse the gluster heal info xml output.")
        return None

    heal_info_split_brain_data = []
    for brick in root.findall("healInfo/bricks/brick"):
        brick_heal_info_split_brain = {}
        brick_files_in_split_brain = []
        is_file_in_split_brain = False
        for element in brick:
            if element.tag == "file":
                is_file_in_split_brain = True
                file_info = {}
                file_info[element.attrib['gfid']] = element.text
                brick_files_in_split_brain.append(file_info)

            else:
                brick_heal_info_split_brain[element.tag] = element.text
        if is_file_in_split_brain:
            brick_heal_info_split_brain['file'] = brick_files_in_split_brain
        heal_info_split_brain_data.append(brick_heal_info_split_brain)
    return heal_info_split_brain_data


def get_heal_info_split_brain_summary(mnode, volname):
    """Get heal info split_brain summary i.e Bricks and it's
        corresponding number of split-brain entries, status.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        NoneType: None if parse errors.
        dict: dict of dictionaries. brick names are the keys of the dict with
            each key having brick's status, numberOfEntries info as dict.
            Example:
                heal_info_split_brain_summary_data = {
                    'ijk.lab.eng.xyz.com': {
                        'status': 'Connected'
                        'numberOfEntries': '11'
                        },
                    'def.lab.eng.xyz.com': {
                        'status': 'Connected',
                        'numberOfEntries': '11'
                        }
                    }

    """
    heal_info_split_brain_data = get_heal_info_split_brain(mnode, volname)
    if heal_info_split_brain_data is None:
        g.log.error("Unable to get heal info summary for the volume %s" %
                    volname)
        return None

    heal_info_split_brain_summary_data = {}
    for info_data in heal_info_split_brain_data:
        heal_info_split_brain_summary_data[info_data['name']] = {
            'status': info_data['status'],
            'numberOfEntries': info_data['numberOfEntries']
        }
    return heal_info_split_brain_summary_data
