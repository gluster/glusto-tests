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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import (set_volume_options,
                                           get_volume_list)


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
    Disables the shared storage

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
    g.log.info("Successfully disabled shared storage")
    return True


def is_shared_volume_mounted(mnode):
    """
    Checks if shared storage volume is mounted

    Args:
        mnode (str) : Node on which command is to be executed

    Returns:
        bool : True if shared storage volume is mounted.
               False otherwise.
    """
    halt = 20
    counter = 0
    path = "/run/gluster/shared_storage"
    while counter < halt:
        _, out, _ = g.run(mnode, "df -h")
        if path in out:
            g.log.info("Shared storage volume is mounted")
            return True
        else:
            sleep(2)
            counter = counter + 2
    g.log.info("Shared storage volume not mounted")
    return False


def check_gluster_shared_volume(mnode, present=True):
    """
    Check gluster shared volume present or absent.

    Args:
        mnode (str) : Node on which command is to be executed
        present (bool) : True if you want to check presence
                         False if you want to check absence.

    Returns:
        bool : True if shared volume is present or absent.
               False otherwise.
    """
    if present:
        halt = 20
        counter = 0
        g.log.info("Wait for some seconds to create "
                   "gluster_shared_storage volume.")

        while counter < halt:
            vol_list = get_volume_list(mnode)
            if "gluster_shared_storage" in vol_list:
                return True
            else:
                g.log.info("Wait for some seconds, since it takes "
                           "time to create gluster_shared_storage "
                           "volume.")
                sleep(2)
                counter = counter + 2

        return False

    else:
        halt = 20
        counter = 0
        g.log.info("Wait for some seconds to delete "
                   "gluster_shared_storage volume.")

        while counter < halt:
            vol_list = get_volume_list(mnode)
            if "gluster_shared_storage" not in vol_list:
                return True
            else:
                g.log.info("Wait for some seconds, since it takes "
                           "time to delete gluster_shared_storage "
                           "volume.")
            sleep(2)
            counter = counter + 2

        return False
