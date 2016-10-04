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


import re
import time
from glusto.core import Glusto as g
from pprint import pformat
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree
from glustolibs.gluster.mount_ops import mount_volume
from glustolibs.gluster.gluster_init import env_setup_servers, start_glusterd
from glustolibs.gluster.peer_ops import (peer_probe_servers,
                                         nodes_from_pool_list)

"""
    This file contains the gluster volume operations like create volume,
    start/stop volume etc
"""


def volume_create(mnode, volname, bricks_list, force=False, **kwargs):
    """Create the gluster volume with specified configuration

    Args:
        mnode(str): server on which command has to be executed
        volname(str): volume name that has to be created
        bricks_list (list): List of bricks to use for creating volume.
            Example:
                from glustolibs.gluster.lib_utils import form_bricks_list
                bricks_list = form_bricks_list(mnode, volname, num_of_bricks,
                                               servers, servers_info)
    Kwargs:
        force (bool): If this option is set to True, then create volume
            will get executed with force option. If it is set to False,
            then create volume will get executed without force option

        **kwargs
            The keys, values in kwargs are:
                - replica_count : (int)|None
                - arbiter_count : (int)|None
                - stripe_count : (int)|None
                - disperse_count : (int)|None
                - disperse_data_count : (int)|None
                - redundancy_count : (int)|None
                - transport_type : tcp|rdma|tcp,rdma|None
                - ...

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

           (-1, '', ''): If not enough bricks are available to create volume.
           (ret, out, err): As returned by volume create command execution.

    Example:
        volume_create(mnode, volname, bricks_list)
    """
    replica_count = arbiter_count = stripe_count = None
    disperse_count = disperse_data_count = redundancy_count =  None
    transport_type = None

    if 'replica_count' in kwargs:
        replica_count = int(kwargs['replica_count'])

    if 'arbiter_count' in kwargs:
        arbiter_count = int(kwargs['arbiter_count'])

    if 'stripe_count' in kwargs:
        stripe_count = int(kwargs['stripe_count'])

    if 'disperse_count' in kwargs:
        disperse_count = int(kwargs['disperse_count'])

    if 'disperse_data_count' in kwargs:
        disperse_data_count = int(kwargs['disperse_data_count'])

    if 'redundancy_count' in kwargs:
        redundancy_count = int(kwargs['redundancy_count'])

    if 'transport_type' in kwargs:
        transport_type = kwargs['transport_type']

    replica = arbiter = stripe = disperse = disperse_data = redundancy = ''
    transport = ''
    if replica_count is not None:
        replica = "replica %d" % replica_count

    if arbiter_count is not None:
        arbiter = "arbiter %d" % arbiter_count

    if stripe_count is not None:
        stripe = "stripe %d" % stripe_count

    if disperse_count is not None:
        disperse = "disperse %d" % disperse_count

    if disperse_data_count is not None:
        disperse_data = "disperse-data %d" % disperse_data_count

    if redundancy_count is not None:
        redundancy = "redundancy %d" % redundancy_count

    if transport_type is not None:
        transport = "transport %s" % transport_type

    cmd = ("gluster volume create %s %s %s %s %s %s %s %s %s "
           "--mode=script" % (volname, replica, arbiter, stripe,
                              disperse, disperse_data, redundancy,
                              transport, ' '.join(bricks_list)))

    if force:
        cmd = cmd + " force"

    return g.run(mnode, cmd)

def volume_start(mnode, volname, force=False):
    """Starts the gluster volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        force (bool): If this option is set to True, then start volume
            will get executed with force option. If it is set to False,
            then start volume will get executed without force option

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        volume_start("testvol")
    """
    if force:
        cmd = "gluster volume start %s force --mode=script" % volname
    else:
        cmd = "gluster volume start %s --mode=script" % volname
    return g.run(mnode, cmd)


def volume_stop(mnode, volname, force=False):
    """Stops the gluster volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        force (bool): If this option is set to True, then stop volume
            will get executed with force option. If it is set to False,
            then stop volume will get executed without force option

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        volume_stop(mnode, "testvol")
    """
    if force:
        cmd = "gluster volume stop %s force --mode=script" % volname
    else:
        cmd = "gluster volume stop %s --mode=script" % volname
    return g.run(mnode, cmd)


def volume_delete(mnode, volname):
    """Deletes the gluster volume if given volume exists in
       gluster and deletes the directories in the bricks
       associated with the given volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        bool: True, if volume is deleted
              False, otherwise

    Example:
        volume_delete("abc.xyz.com", "testvol")
    """

    volinfo = get_volume_info(mnode, volname)
    if volinfo is None or volname not in volinfo:
        g.log.info("Volume %s does not exist in %s" % (volname, mnode))
        return True

    if volinfo[volname]['typeStr'] == 'Tier':
        tmp_hot_brick = volinfo[volname]["bricks"]["hotBricks"]["brick"]
        hot_bricks = [x["name"] for x in tmp_hot_brick if "name" in x]
        tmp_cold_brick = volinfo[volname]["bricks"]["coldBricks"]["brick"]
        cold_bricks = [x["name"] for x in tmp_cold_brick if "name" in x]
        bricks = hot_bricks + cold_bricks
    else:
        bricks = [x["name"] for x in volinfo[volname]["bricks"]["brick"]
                  if "name" in x]
    ret, _, _ = g.run(mnode, "gluster volume delete %s --mode=script"
                       % volname)
    if ret != 0:
        return False

    for brick in bricks:
        node, vol_dir = brick.split(":")
        ret = g.run(node, "rm -rf %s" % vol_dir)

    return True


def volume_reset(mnode, volname, force=False):
    """Resets the gluster volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        force (bool): If this option is set to True, then reset volume
            will get executed with force option. If it is set to False,
            then reset volume will get executed without force option

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        volume_reset("abc.xyz.com", "testvol")
    """
    if force:
        cmd = "gluster volume reset %s force --mode=script" % volname
    else:
        cmd = "gluster volume reset %s --mode=script" % volname
    return g.run(mnode, cmd)


def volume_status(mnode, volname='all', service='', options=''):
    """Executes gluster volume status cli command

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        volname (str): volume name. Defaults to 'all'
        service (str): name of the service to get status.
            serivce can be, [nfs|shd|<BRICK>|quotad]], If not given,
            the function returns all the services
        options (str): options can be,
            [detail|clients|mem|inode|fd|callpool|tasks]. If not given,
            the function returns the output of gluster volume status

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        volume_status("abc.xyz.com")
    """
    cmd = "gluster vol status %s %s %s" % (volname, service, options)
    return g.run(mnode, cmd)


def _parse_volume_status_xml(root_xml):
    """
    Helper module for get_volume_status. It takes root xml object as input,
    parses and returns the 'volume' tag xml object.
    """

    for element in root_xml:
        if element.findall("volume"):
            return element.findall("volume")
        root_vol = _parse_volume_status_xml(element)
        if root_vol is not None:
            return root_vol


def parse_xml(tag_obj):
    """
    This helper module takes any xml element object and parses all the child
    nodes and returns the parsed data in dictionary format
    """
    node_dict = {}
    for tag in tag_obj:
        if re.search(r'\n\s+', tag.text) is not None:
            port_dict = {}
            port_dict = parse_xml(tag)
            node_dict[tag.tag] = port_dict
        else:
            node_dict[tag.tag] = tag.text
    return node_dict


def get_volume_status(mnode, volname='all', service='', options=''):
    """This module gets the status of all or specified volume(s)/brick

    Args:
        mnode (str): Node on which cmd has to be executed.

    Kwargs:
        volname (str): volume name. Defaults to 'all'
        service (str): name of the service to get status.
            serivce can be, [nfs|shd|<BRICK>|quotad]], If not given,
            the function returns all the services
        options (str): options can be,
            [detail|clients|mem|inode|fd|callpool|tasks]. If not given,
            the function returns the output of gluster volume status
    Returns:
        dict: volume status in dict of dictionary format, on success
        NoneType: on failure

    Example:
        get_volume_status("10.70.47.89", volname="testvol")
        >>>{'testvol': {'10.70.47.89': {'/bricks/brick1/a11': {'status': '1',
        'pid': '28963', 'bricktype': 'cold', 'port': '49163', 'peerid':
        '7fc9015e-8134-4753-b837-54cbc6030c98', 'ports': {'rdma': 'N/A',
        'tcp': '49163'}}, '/bricks/brick2/a31': {'status': '1', 'pid':
        '28982', 'bricktype': 'cold', 'port': '49164', 'peerid':
        '7fc9015e-8134-4753-b837-54cbc6030c98', 'ports': {'rdma': 'N/A',
        'tcp': '49164'}}, 'NFS Server': {'status': '1', 'pid': '30525',
        'port': '2049', 'peerid': '7fc9015e-8134-4753-b837-54cbc6030c98',
        'ports': {'rdma': 'N/A', 'tcp': '2049'}}, '/bricks/brick1/a12':
        {'status': '1', 'pid': '30505', 'bricktype': 'hot', 'port': '49165',
        'peerid': '7fc9015e-8134-4753-b837-54cbc6030c98', 'ports': {'rdma':
        'N/A', 'tcp': '49165'}}}, '10.70.47.118': {'/bricks/brick1/a21':
        {'status': '1', 'pid': '5427', 'bricktype': 'cold', 'port': '49162',
        'peerid': '5397d8f5-2986-453a-b0b5-5c40a9bb87ff', 'ports': {'rdma':
        'N/A', 'tcp': '49162'}}, '/bricks/brick2/a41': {'status': '1', 'pid':
        '5446', 'bricktype': 'cold', 'port': '49163', 'peerid':
        '5397d8f5-2986-453a-b0b5-5c40a9bb87ff', 'ports': {'rdma': 'N/A',
        'tcp': '49163'}}, 'NFS Server': {'status': '1', 'pid': '6397', 'port':
        '2049', 'peerid': '5397d8f5-2986-453a-b0b5-5c40a9bb87ff', 'ports':
        {'rdma': 'N/A', 'tcp': '2049'}}}}}
    """

    cmd = "gluster vol status %s %s %s --xml" % (volname, service, options)

    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to execute gluster volume status command")
        return None

    root = etree.XML(out)
    volume_list = _parse_volume_status_xml(root)
    if volume_list is None:
        g.log.error("Failed to parse the XML output of volume status for "
                    "volume %s" % volname)
        return None

    vol_status = {}
    for volume in volume_list:
        tmp_dict1 = {}
        tmp_dict2 = {}
        hot_bricks = []
        cold_bricks = []
        vol_name = [vol.text for vol in volume if vol.tag == "volName"]

        # parsing volume status xml output
        if options == 'tasks':
            tasks = volume.findall("tasks")
            for each_task in tasks:
                tmp_dict3 = parse_xml(each_task)
                node_name = 'task_status'
                if 'task' in tmp_dict3.keys():
                    if node_name in tmp_dict2.keys():
                        tmp_dict2[node_name].append(tmp_dict3['task'])
                    else:
                        tmp_dict2[node_name] = [tmp_dict3['task']]
                else:
                    tmp_dict2[node_name] = [tmp_dict3]
        else:
            elem_tag = []
            for elem in volume.getchildren():
                elem_tag.append(elem.tag)
            if ('hotBricks' in elem_tag) or ('coldBricks' in elem_tag):
                for elem in volume.getchildren():
                    if (elem.tag == 'hotBricks'):
                        nodes = elem.findall("node")
                        hot_bricks = [node.find('path').text
                                      for node in nodes
                                      if (
                                       node.find('path').text.startswith('/'))]
                    if (elem.tag == 'coldBricks'):
                        for n in elem.findall("node"):
                            nodes.append(n)
                        cold_bricks = [node.find('path').text
                                       for node in nodes
                                       if (
                                        (node.find('path').
                                         text.startswith('/')))]
            else:
                nodes = volume.findall("node")

            for each_node in nodes:
                if each_node.find('path').text.startswith('/'):
                    node_name = each_node.find('hostname').text
                elif each_node.find('path').text == 'localhost':
                    node_name = mnode
                else:
                    node_name = each_node.find('path').text
                node_dict = parse_xml(each_node)
                tmp_dict3 = {}
                if "hostname" in node_dict.keys():
                    if node_dict['path'].startswith('/'):
                        if node_dict['path'] in hot_bricks:
                            node_dict["bricktype"] = 'hot'
                        elif node_dict['path'] in cold_bricks:
                            node_dict["bricktype"] = 'cold'
                        else:
                            node_dict["bricktype"] = 'None'
                        tmp = node_dict["path"]
                        tmp_dict3[node_dict["path"]] = node_dict
                    else:
                        tmp_dict3[node_dict["hostname"]] = node_dict
                        tmp = node_dict["hostname"]
                    del tmp_dict3[tmp]["path"]
                    del tmp_dict3[tmp]["hostname"]
                if node_name in tmp_dict1.keys():
                    tmp_dict1[node_name].append(tmp_dict3)
                else:
                    tmp_dict1[node_name] = [tmp_dict3]

                tmp_dict4 = {}
                for item in tmp_dict1[node_name]:
                    for key, val in item.items():
                        tmp_dict4[key] = val
                tmp_dict2[node_name] = tmp_dict4

        vol_status[vol_name[0]] = tmp_dict2
    g.log.debug("Volume status output: %s"
                % pformat(vol_status, indent=10))
    return vol_status


def get_volume_options(mnode, volname, option='all'):
    """gets the option values for the given volume.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Kwargs:
        option (str): volume option to get status.
                    If not given, the function returns all the options for
                    the given volume

    Returns:
        dict: value for the given volume option in dict format, on success
        NoneType: on failure

    Example:
        get_volume_options(mnode, "testvol")
    """

    cmd = "gluster volume get %s %s" % (volname, option)
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to execute gluster volume get command"
                    "for volume %s" % volname)
        return None

    volume_option = {}
    raw_output = out.split("\n")
    for line in raw_output[2:-1]:
        match = re.search(r'^(\S+)(.*)', line.strip())
        if match is None:
            g.log.error("gluster get volume output is not in "
                        "expected format")
            return None

        volume_option[match.group(1)] = match.group(2).strip()

    return volume_option


def set_volume_options(mnode, volname, options):
    """Sets the option values for the given volume.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        options (dict): volume options in key
            value format

    Returns:
        bool: True, if the volume option is set
              False, on failure

    Example:
        options = {"user.cifs":"enable","user.smb":"enable"}
        set_volume_option("abc.com", "testvol", options)
    """
    _rc = True
    for option in options:
        cmd = ("gluster volume set %s %s %s"
               % (volname, option, options[option]))
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            g.log.error("Unable to set value %s for option %s"
                        % (options[option], option))
            _rc = False
    return _rc


def volume_info(mnode, volname='all'):
    """Executes gluster volume info cli command

    Args:
        mnode (str): Node on which cmd has to be executed.

    Kwargs:
        volname (str): volume name. Defaults to 'all'

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        volume_status("abc.com")
    """

    cmd = "gluster volume info %s" % volname
    return g.run(mnode, cmd)


def get_volume_info(mnode, volname='all'):
    """Fetches the volume information as displayed in the volume info.
        Uses xml output of volume info and parses the into to a dict

    Args:
        mnode (str): Node on which cmd has to be executed.

    Kwargs:
        volname (str): volume name. Defaults to 'all'

    Returns:
        NoneType: If there are errors
        dict: volume info in dict of dicts

    Example:
        get_volume_info("abc.com", volname="testvol")
        >>>{'testvol': {'status': '1', 'xlators': None, 'disperseCount': '0',
        'bricks': {'coldBricks': {'colddisperseCount': '0',
        'coldarbiterCount': '0', 'coldBrickType': 'Distribute',
        'coldbrickCount': '4', 'numberOfBricks': '4', 'brick':
        [{'isArbiter': '0', 'name': '10.70.47.89:/bricks/brick1/a11',
        'hostUuid': '7fc9015e-8134-4753-b837-54cbc6030c98'}, {'isArbiter':
        '0', 'name': '10.70.47.118:/bricks/brick1/a21', 'hostUuid':
        '7fc9015e-8134-4753-b837-54cbc6030c98'}, {'isArbiter': '0', 'name':
        '10.70.47.89:/bricks/brick2/a31', 'hostUuid':
        '7fc9015e-8134-4753-b837-54cbc6030c98'}, {'isArbiter': '0',
        'name': '10.70.47.118:/bricks/brick2/a41', 'hostUuid':
        '7fc9015e-8134-4753-b837-54cbc6030c98'}], 'coldreplicaCount': '1'},
        'hotBricks': {'hotBrickType': 'Distribute', 'numberOfBricks': '1',
        'brick': [{'name': '10.70.47.89:/bricks/brick1/a12', 'hostUuid':
        '7fc9015e-8134-4753-b837-54cbc6030c98'}], 'hotbrickCount': '1',
        'hotreplicaCount': '1'}}, 'type': '5', 'distCount': '1',
        'replicaCount': '1', 'brickCount': '5', 'options':
        {'cluster.tier-mode': 'cache', 'performance.readdir-ahead': 'on',
        'features.ctr-enabled': 'on'}, 'redundancyCount': '0', 'transport':
        '0', 'typeStr': 'Tier', 'stripeCount': '1', 'arbiterCount': '0',
        'id': 'ffa8a8d1-546f-4ebf-8e82-fcc96c7e4e05', 'statusStr': 'Started',
        'optCount': '3'}}
    """

    cmd = "gluster volume info %s --xml" % volname
    ret, out, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("volume info returned error")
        return None
    root = etree.XML(out)
    volinfo = {}
    for volume in root.findall("volInfo/volumes/volume"):
        for elem in volume.getchildren():
            if elem.tag == "name":
                volname = elem.text
                volinfo[volname] = {}
            elif elem.tag == "bricks":
                volinfo[volname]["bricks"] = {}
                tag_list = [x.tag for x in elem.getchildren() if x]
                if 'brick' in tag_list:
                    volinfo[volname]["bricks"]["brick"] = []
                for el in elem.getchildren():
                    if el.tag == 'brick':
                        brick_info_dict = {}
                        for elmt in el.getchildren():
                            brick_info_dict[elmt.tag] = elmt.text
                        (volinfo[volname]["bricks"]["brick"].
                         append(brick_info_dict))

                    if el.tag == "hotBricks" or el.tag == "coldBricks":
                        volinfo[volname]["bricks"][el.tag] = {}
                        volinfo[volname]["bricks"][el.tag]["brick"] = []
                        for elmt in el.getchildren():
                            if elmt.tag == 'brick':
                                brick_info_dict = {}
                                for el_brk in elmt.getchildren():
                                    brick_info_dict[el_brk.tag] = el_brk.text
                                (volinfo[volname]["bricks"][el.tag]["brick"].
                                 append(brick_info_dict))
                            else:
                                volinfo[volname]["bricks"][el.tag][elmt.tag] = elmt.text
            elif elem.tag == "options":
                volinfo[volname]["options"] = {}
                for option in elem.findall("option"):
                    for el in option.getchildren():
                        if el.tag == "name":
                            opt = el.text
                        if el.tag == "value":
                            volinfo[volname]["options"][opt] = el.text
            else:
                volinfo[volname][elem.tag] = elem.text

    g.log.debug("Volume info output: %s"
                % pformat(volinfo, indent=10))

    return volinfo


def volume_sync(mnode, hostname, volname="all"):
    """syncs the volume to the specified host

    Args:
        mnode (str): Node on which cmd has to be executed.
        hostname (str): host name to which volume has to be sync'ed

    Kwargs:
        volname (str): volume name. Defaults to 'all'.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        volume_sync("abc.xyz.com",volname="testvol")
    """

    cmd = "gluster volume sync %s %s --mode=script" % (hostname, volname)
    return g.run(mnode, cmd)
