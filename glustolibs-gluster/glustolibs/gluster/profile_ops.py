#!/usr/bin/env python
#  Copyright (C) 2019 Red Hat, Inc. <http://www.redhat.com>
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
    Description: Library for volume profile operations.
"""
from glusto.core import Glusto as g
from pprint import pformat
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree


def profile_start(mnode, volname):
    """Start profile on the specified volume.

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): Volume on which profile has to started.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        profile_start(mnode, "testvol")
    """
    cmd = "gluster volume profile %s start" % volname
    return g.run(mnode, cmd)


def profile_info(mnode, volname, options=''):
    """Run profile info on the specified volume.

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): Volume for which profile info has to be retrived.

    Kwargs:
        options (str): Options can be
        [peek|incremental [peek]|cumulative|clear].If not given the
        function returns the output of gluster volume profile <volname> info.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
        None: If invalid option is given.

    Example:
        profile_info(mnode, "testvol")
    """
    if not check_profile_options(options):
        return None
    cmd = "gluster volume profile %s info %s" % (volname, options)
    return g.run(mnode, cmd)


def profile_stop(mnode, volname):
    """Stop profile on the specified volume.

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): Volume on which profile has to be stopped.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        profile_stop(mnode, "testvol")
    """
    cmd = "gluster volume profile %s stop" % volname
    return g.run(mnode, cmd)


def check_profile_options(options):
    """Helper function to valid if profile options.

    Args:
        options (str): Options can be nothing or
        [peek|incremental [peek]|cumulative|clear].

    Returns:
        True: If valid option is given.
        False: If invalid option is given
    """

    list_of_options = ['peek', 'incremental', 'incremental peek',
                       'cumulative', 'clear', '']
    if options not in list_of_options:
        g.log.error("Invalid profile info option given.")
        return False
    return True


def get_profile_info(mnode, volname, options=''):
    """Fetches the volume profile information as displayed in the volume
        profile info.
        Uses xml output of volume profile info and parses the
        into to a dict

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): Volume for which profile info has to be retrived.

    Kwargs:
        options (str): Options can be
        [peek|incremental [peek]|cumulative|clear].If not given the
        function returns the output of gluster volume profile <volname> info.

    Returns:
        NoneType: If there are errors.
        dict: Volume profile info in dict of dicts

    Example:
        get_profile_info(mnode, "testvol")
    """

    if not check_profile_options(options):
        return None

    cmd = "gluster volume profile %s info %s --xml" % (volname, options)
    ret, out, err = g.run(mnode, cmd, log_level='DEBUG')
    if ret:
        g.log.error("Profile not running on volume.")
        return None

    # Iterating through the XML and creating dict
    root = etree.XML(out)
    volprofileinfo = {}
    volume = root.find("volProfile")
    brick_counter = 0
    for elem in volume.getchildren():
        if elem.tag == "volname":
            volname = elem.text
            volprofileinfo[volname] = {}
        elif elem.tag == "brick":
            brick_counter += 1
            volprofileinfo[volname][elem.tag+str(brick_counter)] = {}
            brick_dict = volprofileinfo[volname][elem.tag+str(brick_counter)]
            for brick_tag in elem.getchildren():
                if 'cumulativeStats' == brick_tag.tag:
                    brick_dict["cumulativeStats"] = {}
                    for el in brick_tag.getchildren():
                        if el.tag == 'duration':
                            brick_dict["cumulativeStats"][el.tag] = el.text
                        elif el.tag == 'totalWrite' or el.tag == 'totalRead':
                            brick_dict["cumulativeStats"][el.tag] = el.text
                        elif el.tag == 'blockStats':
                            brick_dict["cumulativeStats"][el.tag] = {}
                            block_dict = brick_dict["cumulativeStats"][el.tag]
                            counter = 0
                            for block in el.getchildren():
                                counter += 1
                                block_dict[block.tag+str(counter)] = {}
                                elm_dict = block_dict[block.tag+str(counter)]
                                for block_elm in block.getchildren():
                                    elm_dict[block_elm.tag] = block_elm.text
                        elif el.tag == 'fopStats':
                            brick_dict["cumulativeStats"][el.tag] = {}
                            fop_dict = brick_dict["cumulativeStats"][el.tag]
                            fop_count = 0
                            for fops in el.getchildren():
                                fop_dict['fop'+str(fop_count)] = {}
                                fop_param = fop_dict['fop'+str(fop_count)]
                                for fop in fops.getchildren():
                                    fop_param[fop.tag] = fop.text
                                fop_count += 1
                elif 'intervalStats' == brick_tag.tag:
                    brick_dict["intervalStats"] = {}
                    for el in brick_tag.getchildren():
                        if el.tag == 'duration':
                            brick_dict["intervalStats"][el.tag] = el.text
                        elif el.tag == 'totalWrite' or el.tag == 'totalRead':
                            brick_dict["intervalStats"][el.tag] = el.text
                        elif el.tag == 'blockStats':
                            brick_dict["intervalStats"][el.tag] = {}
                            block_dict = brick_dict["intervalStats"][el.tag]
                            counter = 0
                            for block in el.getchildren():
                                counter += 1
                                block_dict[block.tag+str(counter)] = {}
                                elm_dict = block_dict[block.tag+str(counter)]
                                for block_elm in block.getchildren():
                                    elm_dict[block_elm.tag] = block_elm.text
                        elif el.tag == 'fopStats':
                            brick_dict["intervalStats"][el.tag] = {}
                            fop_dict = brick_dict["intervalStats"][el.tag]
                            fop_count = 0
                            for fops in el.getchildren():
                                fop_dict['fop'+str(fop_count)] = {}
                                fop_param = fop_dict['fop'+str(fop_count)]
                                for fop in fops.getchildren():
                                    fop_param[fop.tag] = fop.text
                                fop_count += 1
                else:
                    brick_dict[brick_tag.tag] = brick_tag.text
        else:
            volprofileinfo[elem.tag] = elem.text

    g.log.debug("Volume profile info output: %s"
                % pformat(volprofileinfo, indent=10))
    return volprofileinfo


def profile_info_clear(mnode, volname):
    """Run profile info clear on the specified volume.

    Args:
        mnode (str): Node on which command has to be executed.
        volname (str): Volume for which profile info has to be retrived.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).

            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        profile_info_clear(mnode, "testvol")
    """
    cmd = "gluster volume profile %s info clear" % (volname)
    return g.run(mnode, cmd)
