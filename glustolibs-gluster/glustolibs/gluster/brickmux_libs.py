#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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
    Description: Module for Brick multiplexing realted helper functions.
"""

from itertools import cycle
try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest

from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_list
from glustolibs.gluster.lib_utils import get_servers_bricks_dict


def get_all_bricks_from_servers_multivol(servers, servers_info):
    """
    Form list of all the bricks to create/add-brick from the given
    servers and servers_info

    Args:
        servers (list): List of servers in the storage pool.
        servers_info (dict): Information about all servers.

    Returns:
        brickCount (int): Number of bricks available from the servers.
        bricks_list (list): List of all bricks from the servers provided.

    example :
            servers_info = {
                'abc.lab.eng.xyz.com': {
                    'host': 'abc.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    },
                'def.lab.eng.xyz.com':{
                    'host': 'def.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    }
                }
    """
    if not isinstance(servers, list):
        servers = [servers]

    brickCount, bricks_list = 0, []

    servers_bricks = get_servers_bricks_dict(servers, servers_info)
    server_ip = cycle(servers_bricks.keys())

    for item in list(zip_longest(*list(servers_bricks.values()))):
        for brick in item:
            server = server_ip.next()
            if brick:
                bricks_list.append(server + ":" + brick)
                brickCount += 1
    return brickCount, bricks_list


def get_current_brick_index(mnode):
    """
    Get the brick current index from the node of the cluster.

    Args:
        mnode (str): Node on which commands has to be executed.

    Returns:
        NoneType: If there are any errors
        int: Count of the bricks in the cluster.
    """
    ret, brick_index, err = g.run(mnode, "gluster volume info | egrep "
                                  "\"^Brick[0-9]+\" | grep -v \"ss_brick\"")
    if ret:
        g.log.error("Error in getting bricklist using gluster v info %s" % err)
        return None

    g.log.info("brick_index is ", brick_index)
    return len(brick_index.splitlines())


def form_bricks_for_multivol(mnode, volname, number_of_bricks, servers,
                             servers_info):
    """
    Forms brics list for volume create/add-brick given the number_of_bricks
    servers, servers_info, for multiple volume cluster and for brick multiplex
    enabled cluster.

    Args:
        mnode (str): Node on which commands has to be executed.
        volname (str): Volume name for which we require brick-list
        number_of_bricks (int): The number of bricks for which brick list
                                has to be created.
        servers (str|list): A server|List of servers from which the bricks
                            needs to be selected for creating the brick list.
        servers_info (dict): Dict of server info of each servers.

    Returns:
        list: List of bricks to use with volume create.
        Nonetype: If unable to fetch the brick list

    """
    if not isinstance(servers, list):
        servers = [servers]

    brick_index, brick_list_for_volume = 0, []

    # Importing get_all_bricks() from bricks_libs to avoid cyclic imports
    from glustolibs.gluster.brick_libs import get_all_bricks

    # Get all volume list present in the cluster from mnode
    current_vol_list = get_volume_list(mnode)
    for volume in current_vol_list:
        brick_index = brick_index + len(get_all_bricks(mnode, volume))
    g.log.info("current brick_index %s" % brick_index)

    # Get all bricks_count and bricks_list
    all_brick_count, bricks_list = get_all_bricks_from_servers_multivol(
        servers, servers_info)
    if not (all_brick_count > 1):
        g.log.error("Unable to get the bricks present in the specified"
                    "servers")
        return None

    for num in range(number_of_bricks):
        brick = brick_index % all_brick_count
        brick_list_for_volume.append("%s/%s_brick%d" % (bricks_list[brick],
                                                        volname, brick_index))
        brick_index += 1

    return brick_list_for_volume
