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
   Description: Library for gluster geo-replication operations
"""

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_init import restart_glusterd
from glustolibs.gluster.geo_rep_ops import (create_shared_storage,
                                            georep_groupadd,
                                            georep_geoaccount,
                                            georep_mountbroker_setup,
                                            georep_mountbroker_adduser,
                                            georep_mountbroker_status)


def setup_mountbroker_prerequisites(mnode, snodes, group, user, mntbroker_dir,
                                    slavevol):
    """ Setup pre-requisites for mountbroker setup

    Args:
        mnode (str) : Master node on which cmd is to be executed
        snodes (list): List of slave nodes
        group (str): Specifies a group name
        user (str): Specifies a user name
        mntbroker_dir: Mountbroker mount directory
        slavevol (str) The name of the slave volume
    Returns:
        bool: True if all pre-requisite are successful else False

    """
    g.log.debug("Enable shared-storage")
    ret, _, err = create_shared_storage(mnode)
    if ret:
        if "already exists" not in err:
            g.log.error("Failed to enable shared storage on %s", mnode)
            return False

    g.log.debug("Create new group: %s on all slave nodes", group)
    if not georep_groupadd(snodes, group):
        g.log.error("Creating group: %s on all slave nodes failed", group)
        return False

    g.log.debug("Create user: %s in group: %s on all slave nodes", user, group)
    if not georep_geoaccount(snodes, group, user):
        g.log.error("Creating user: %s in group: %s on all slave nodes "
                    "failed", user, group)
        return False

    g.log.debug("Setting up mount broker root directory: %s node: %s",
                mntbroker_dir, snodes[0])
    ret, _, _ = georep_mountbroker_setup(snodes[0], group, mntbroker_dir)
    if ret:
        g.log.error("Setting up of mount broker directory failed: %s node: %s",
                    mntbroker_dir, snodes[0])
        return False

    g.log.debug("Add volume: %s and user: %s to mountbroker service",
                slavevol, user)
    ret, _, _ = georep_mountbroker_adduser(snodes[0], slavevol, user)
    if ret:
        g.log.error("Add volume: %s and user: %s to mountbroker "
                    "service failed", slavevol, user)
        return False

    g.log.debug("Checking mountbroker status")
    ret, out, _ = georep_mountbroker_status(snodes[0])
    if not ret:
        if "not ok" in out:
            g.log.error("Mountbroker status not ok")
            return False
    else:
        g.log.error("Mountbroker status command failed")
        return False

    g.log.debug("Restart glusterd on all slave nodes")
    if not restart_glusterd(snodes):
        g.log.error("Restarting glusterd failed")
        return False

    return True

    # TODO setup passwdless SSH between one of master nodes to one of slave
    # nodes
