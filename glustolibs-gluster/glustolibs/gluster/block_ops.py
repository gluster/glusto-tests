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

""" Description: Module for gluster Block related helper functions. """

from glusto.core import Glusto as g


def block_create(mnode, volname, blockname, servers, size=None,
                 **block_args_info):
    """Create the gluster block with specified configuration

    Args:
        mnode(str): server on which command has to be executed
        volname(str): volume name that has to be created
        blockname(str): block name that has to be created
        servers(list): List of servers IP's to be used to create block
        size(str|None): Size of the block, none if storage link passed

    block_args_info:
        **block_args_info
            The keys, values in block_args_info are:
                - ha : (int)|None
                - auth : (str)|None (enable|disable)
                - prealloc : (str)|None (full|no)
                - storage : (str)|None
                - ring-buffer: (int)

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

           (ret, out, err): As returned by block create command execution.

    Example:
        block_create(mnode, volname, blockname, servers, size,
        **block_args_info)
    """
    if isinstance(servers, str):
        servers = [servers]

    ha = auth = prealloc = storage = ring_buffer = ''

    if block_args_info.get('ha'):
        ha = "ha %d" % int(block_args_info['ha'])

    if block_args_info.get('auth'):
        auth = "auth %s" % block_args_info['auth']

    if block_args_info.get('prealloc'):
        prealloc = "prealloc %s" % block_args_info['prealloc']

    if block_args_info.get('ring-buffer'):
        ring_buffer = "ring-buffer %d" % int(block_args_info['ring-buffer'])

    if block_args_info.get('storage'):
        storage = "storage %s" % block_args_info['storage']
        size = ''

    cmd = ("gluster-block create %s/%s %s %s %s %s %s %s %s " %
           (volname, blockname, ha, auth, ring_buffer, storage,
            prealloc, ','.join(servers), size))

    return g.run(mnode, cmd)


def block_modify(mnode, volname, blockname, auth=None, size=None, force=False):
    """modify the block device by either enabling the auth or changing the
    block size.

    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): Name of volume
        blockname (str): Name of the block
        auth (str): enable/disable
        size (str): New block size

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

           (ret, out, err): As returned by block create command execution.
    """
    force_block_modify = ''
    if force is True:
        force_block_modify = "force"

    if size is None:
        cmd = ("gluster-block modify %s/%s auth %s %s --json-pretty" %
               (volname, blockname, auth, force_block_modify))
    else:
        cmd = ("gluster-block modify %s/%s size %s %s --json-pretty " %
               (volname, blockname, size, force_block_modify))

    return g.run(mnode, cmd)


def block_list(mnode, volname):
    """list available block devices for the volume

    Args:
        mnode(str): server on which command has to be executed
        volname(str): volume name for which blocks has to be listed

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

           (ret, out, err): As returned by block create command execution.
    """
    cmd = "gluster-block list %s --json-pretty" % volname
    return g.run(mnode, cmd)


def block_info(mnode, volname, blockname):
    """Get info of block device

    Args:
        mnode(str): server on which command has to be executed
        volname(str): volume name for which block info has got
        blockname(str): block name for which info has to be got

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

           (ret, out, err): As returned by block create command execution.

    """
    cmd = "gluster-block info %s/%s --json-pretty" % (volname, blockname)
    return g.run(mnode, cmd)


def block_delete(mnode, volname, blockname, unlink_storage="yes", force=False):
    """Delete the block from volname

    Args:
        mnode(str): server on which command has to be executed
        volname(str): volume name from which block has to be delete
        blockname(str): block name to be deleted

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

           (ret, out, err): As returned by block create command execution.

    """
    force_delete_block = ''
    if force is True:
        force_delete_block = "force"

    if unlink_storage == "yes":
        cmd = "gluster-block delete %s/%s %s" % (volname, blockname,
                                                 force_delete_block)
    else:
        cmd = ("gluster-block delete %s/%s unlink-storage no %s"
               % (volname, blockname, force_delete_block))
    return g.run(mnode, cmd)
