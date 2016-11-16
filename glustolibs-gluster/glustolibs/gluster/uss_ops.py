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
    Description: Module for gluster uss operations
"""

from glusto.core import Glusto as g


def enable_uss(mnode, volname):
    """Enables uss on the specified volume

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
    cmd = "gluster volume set %s features.uss enable" % volname
    return g.run(mnode, cmd)


def disable_uss(mnode, volname):
    """Disables uss on the specified volume

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
    cmd = "gluster volume set %s features.uss disable" % volname
    return g.run(mnode, cmd)


def is_uss_enabled(mnode, volname):
    """Check if uss is Enabled on the specified volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        bool : True if successfully enabled uss on the volume. False otherwise.
    """
    from glustolibs.gluster.volume_ops import get_volume_options
    option_dict = get_volume_options(mnode=mnode, volname=volname,
                                     option="uss")
    if option_dict is None:
        g.log.error("USS is not set on the volume %s" % volname)
        return False

    if ('features.uss' in option_dict and
            option_dict['features.uss'] == 'enable'):
        return True
    else:
        return False


def is_uss_disabled(mnode, volname):
    """Check if uss is disabled on the specified volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name

    Returns:
        bool : True if successfully disabled uss on the volume.
            False otherwise.
    """
    from glustolibs.gluster.volume_ops import get_volume_options
    option_dict = get_volume_options(mnode=mnode, volname=volname,
                                     option="uss")
    if option_dict is None:
        g.log.error("USS is not set on the volume %s" % volname)
        return False

    if ('features.uss' in option_dict and
            option_dict['features.uss'] == 'disable'):
        return True
    else:
        return False
