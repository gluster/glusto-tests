#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY :or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

""" Description: Module for gluster quota related helper functions. """


from glusto.core import Glusto as g
from glustolibs.gluster.quota_ops import (quota_fetch_list)


def quota_validate(mnode, volname, path, **kwargs):
    """ Validate if the hard limit, soft limit, usage match the expected values.
        If any of the arguments are None, they are not verified.

    Args:
        mnode (str)             : Node on which command has to be executed.
        volname (str)           : volume name.
        path (str)              : Path to be verified.
        kwargs
        hard_limit(int)         : hard limit is verified with this value.
        soft_limit_percent(int) : soft limit (in %) is verified with this value
        used_space(int)         : if set, usage as displayed in quota list is
                                  verified with expected value.
        avail_space(int)         : if set, usage as displayed in quota list is
                                  verified with expected value.
        sl_exceeded(bool)       : expected value of soft limit flag.
        hl_exceeded(bool)       : expected value of hard limit flag.

    """

    if kwargs is None:
        g.log.error("No arguments given for validation")
        return False

    quotalist = quota_fetch_list(mnode, volname, path)

    if path not in quotalist:
        g.log.error("Path not found (script issue)")
        return False
    else:
        listinfo = quotalist[path]

    ret = True
    for key, value in kwargs.iteritems():
        if key and listinfo[key] != value:
            g.log.error("%s = %s does not match with expected value %s",
                        key, str(listinfo[key]), str(value))
            ret = False

    return ret
