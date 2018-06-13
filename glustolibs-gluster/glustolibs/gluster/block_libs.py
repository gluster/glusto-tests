#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
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

""" Description: Module for gluster block related helper functions. """

from glusto.core import Glusto as g
from glustolibs.gluster.block_ops import block_create, block_info, block_list


def if_block_exists(mnode, volname, blockname):
    """Check if block already exists

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): Name of the volume.
        blockname(str): block name

    Returns:
        bool : True if block exists. False Otherwise
    """
    # Get block list for the volname
    ret, out, err = block_list(mnode, volname)
    if ret != 0:
        g.log.error("Failed to get block list for the volume %s: %s",
                    volname, err)
        return False

    # Check if block exists for the volname
    block_list_dict = g.load_json_string(out)
    if blockname in block_list_dict['blocks']:
        return True
    else:
        g.log.error("Block '%s' doesn't exists on volume %s",
                    blockname, volname)
        return False


def setup_block(mnode, volname, blockname, servers, size, **block_args_info):
    """Create the gluster block with specified configuration

    Args:
        mnode(str): server on which command has to be executed
        volname(str): volume name that has to be created
        blockname(str): block name that has to be created
        servers(list): List of servers IP's to be used to create block
        size(str): Size of the block

    block_args_info:
        **block_args_info
            The keys, values in block_args_info are:
                - ha : (int)|None
                - auth : (str)|None (enable|disable)
                - prealloc : (str)|None (full|no)
                - ...

    Returns:
        bool : True on successful setup. False Otherwise

    Example:
        setup_block(mnode, volname, blockname, servers, size,
        **block_args_info)
    """
    # Create the Block Device
    ret, _, err = block_create(mnode, volname, blockname, servers, size,
                               **block_args_info)
    if ret != 0:
        g.log.error("Failed to create block:%s\n%s", err, block_args_info)
        return False

    return True


def get_block_info(mnode, volname, blockname):
    """Get Block Info

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): Name of the volume.
        blockname(str): block name

    Returns:
        dict if successful in getting block info, None if block doesn't exists
    """
    ret, out, err = block_info(mnode, volname, blockname)
    if ret != 0:
        g.log.error("Failed to get block info of block: %s/%s : %s",
                    volname, blockname, err)
        return None

    return g.load_json_string(out)


def get_block_list(mnode, volname):
    """ Get list of all blocks for the volume

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): Name of the volume.

    Returns:
        list of block names if successful in getting block list, None if
        block list command errors
    """
    # get block list for the volname
    ret, out, err = block_list(mnode, volname)
    if ret != 0:
        g.log.error("Failed to get block list for the volume %s: %s",
                    volname, err)
        return None

    block_list_dict = g.load_json_string(out)
    blocknames = block_list_dict.get('blocks', None)
    if blocknames is None:
        g.log.error("No blocks present on volume %s", volname)

    return blocknames


def get_block_gbid(mnode, volname, blockname):
    """Get Block IQN

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): Name of the volume.
        blockname(str): block name

    Returns:
        gbid if successful, None if block doesn't exists
    """
    block_info_dict = get_block_info(mnode, volname, blockname)
    if not block_info_dict:
        return None

    block_gbid = block_info_dict.get('GBID')
    return block_gbid


def get_block_password(mnode, volname, blockname):
    """Get Block Password

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): Name of the volume.
        blockname(str): block name

    Returns:
        Password if auth enable, None otherwise
    """
    block_info_dict = get_block_info(mnode, volname, blockname)
    if not block_info_dict:
        return None

    block_password = block_info_dict.get('PASSWORD')
    if block_password == '':
        g.log.error("Block authentication is disabled")
        return None
    else:
        return block_password


def get_volume_blocks_gbid(mnode, volname):
    """Get Block GBID for all the blocks present in the volume

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): Name of the volume.

    Returns:
        dict of block:gbid if successful,
        None if volume doesn't have any blocks or volume doesn't exists
    """
    blocknames = get_block_list(mnode, volname)
    if not blocknames:
        return None

    blocks_gbid = {}

    # get each block info
    for blockname in blocknames:
        ret = get_block_gbid(mnode, volname, blockname)
        blocks_gbid[blockname] = ret

    return blocks_gbid


def validate_block_info(mnode, volname, blockname, servers, size,
                        **block_args_info):
    """Validate the output of gluster-block info command with the params
       passed

    Args:
        mnode(str): server on which command has to be executed
        volname(str): volume name that has to be validated
        blockname(str): block name that has to be validated
        servers(list): List of servers IP's to be used to create block
        size(str): Size of the block

    block_args_info:
        **block_args_info
            The keys, values in block_args_info are:
                - ha : (int)|None

    Returns:
        bool : True if validation is successful. False Otherwise

    Example:
        validate_block_info(mnode, volname, blockname, servers, size,
        **block_args_info)
    """
    block_info_dict = get_block_info(mnode, volname, blockname)
    if not block_info_dict:
        g.log.error("Failed to get block info of block: %s/%s",
                    volname, blockname)
        return False

    # Check volname
    if volname != block_info_dict.get('VOLUME'):
        g.log.error("Volume name validation is unsuccessful")
        return False

    if blockname != block_info_dict.get("NAME"):
        g.log.error("Block name validation is unsuccessful")
        return False

    if (block_args_info.get('ha')) != block_info_dict.get("HA"):
        g.log.error("HA parameter validation is unsuccessful")
        return False

    if set(servers) != set(block_info_dict.get("EXPORTED ON")):
        g.log.error("Server information validation is unsuccessful")
        return False

    if size != block_info_dict.get("SIZE"):
        g.log.error("Size validation is unsuccessful")
        return False

    g.log.info("Information in config file matches with block"
               "information on server")
    return True


def check_device_logged_in(client, block_iqn):
    """Check if the block is logged in on the client

    Args:
        client: Client to check on
        block_iqn: IQN of block for which a check is done

    Returns:
        bool : True if validation is successful. False Otherwise

    """

    cmd = 'iscsiadm -m session | grep -F -m 1 %s' % block_iqn
    ret, out, err = g.run(client, cmd)
    if ret != 0:
        g.log.error("Failed to get device login details: %s", err)
        return False

    return True
