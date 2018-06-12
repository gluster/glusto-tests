#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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
Description : Modules for enabling and disabling
shared storoge
"""

import time
from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import set_volume_options


def enable_shared_storage(mnode):
    """
    Enables the shared storage

    Args:
        mnode (str) : Node on which command is to be executed

    Returns:
        bool : True if successfully enabled shared storage.
               False otherwise.
    """
    option = {"cluster.enable-shared-storage": "enable"}
    ret = set_volume_options(mnode, "all", option)
    if not ret:
        g.log.error("Failed to enable shared storage")
        return False
    g.log.info("Successfully enabled shared storage option")
    return True


def disable_shared_storage(mnode):
    """
    Enables the shared storage

    Args:
        mnode (str) : Node on which command is to be executed

    Returns:
        bool : True if successfully disabled shared storage.
               False otherwise.
    """
    option = {"cluster.enable-shared-storage": "disable"}
    ret = set_volume_options(mnode, "all", option)
    if not ret:
        g.log.error("Failed to disable shared storage")
        return False
    g.log.info("Successfully disabled shared storage option")
    return True


def is_shared_volume_mounted(mnode):
    """
    Checks shared volume mounted after enabling it

    Args:
        mnode (str) : Node on which command is to be executed

    Returns:
        bool : True if successfully mounted shared volume.
               False otherwise.
    """
    halt = 20
    counter = 0
    path = "/run/gluster/shared_storage"
    while counter < halt:
        _, out, _ = g.run(mnode, "df -h")
        if path in out:
            g.log.info("Shared volume mounted successfully")
            return True
        else:
            time.sleep(2)
            counter = counter + 2
    g.log.error("Shared volume not mounted")
    return False


def is_shared_volume_unmounted(mnode):
    """
    Checks shared volume unmounted after disabling it

    Args:
        mnode (str) : Node on which command is to be executed

    Returns:
        bool : True if successfully unmounted shared volume.
               False otherwise.
    """
    halt = 20
    counter = 0
    path = "/run/gluster/shared_storage"
    while counter < halt:
        _, out, _ = g.run(mnode, "df -h")
        if path not in out:
            g.log.info("Shared volume unmounted successfully")
            return True
        else:
            time.sleep(2)
            counter = counter + 2
    g.log.error("Shared volume not unmounted")
    return False
