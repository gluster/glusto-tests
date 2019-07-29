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
from glustolibs.gluster.geo_rep_ops import (georep_groupadd,
                                            georep_geoaccount,
                                            georep_mountbroker_setup,
                                            georep_mountbroker_adduser,
                                            georep_mountbroker_status,
                                            georep_geoaccount_setpasswd,
                                            georep_ssh_keygen,
                                            georep_ssh_copyid,
                                            georep_createpem, georep_create,
                                            georep_set_pemkeys,
                                            georep_config_set)
from glustolibs.gluster.shared_storage_ops import enable_shared_storage


def georep_root_prerequisites(mnode, snode, user, passwd):
    """Takes care of the pre-requisites required for
       setting up a successful geo-rep
       session which includes:
       1. Check if shared_storage has been successfully created
       2. Check if shared_storage has been mounted
       3. Generating the common pem pub file on all the nodes
          on the master
       4. Establishing passwordless ssh connection to the slave
    Args:
        mnode (str) : The primary master node where the commands are executed
        snode (str) : The primary slave node where the commande are executed
        user (str) : User to be used to setup the geo-rep session
        passwd (str): Password of the specified user.
    Returns:
        bool : True if all the steps are successful, false if there are
              any failures in the middle

    """
    g.log.debug("Enable shared-storage")
    ret = enable_shared_storage(mnode)
    if not ret:
        g.log.error("Failed to create gluster shared storage on "
                    "the master cluster")
        return False

    g.log.debug("Creating a common pem pub file on all the nodes in "
                "the master to establish passwordless ssh connection "
                "to the slave ")
    ret = georep_ssh_keygen(mnode)
    if not ret:
        g.log.error("Failed to create a common pem pub file")
        return False

    g.log.debug("Establishing passwordless ssh between master and slave")
    ret = georep_ssh_copyid(mnode, snode, user, passwd)
    if not ret:
        g.log.error("Failed to establish ssh connection")
        return False

    g.log.info("Shared storage has been created. "
               "Passwordless ssh between the master and the "
               "slave has been successful!")
    return True


def georep_create_root_session(mnode, snode, mastervol, slavevol,
                               user=None, force=False):
    """ Create a geo-replication session between the master and
        the slave
    Args:
        mnode (str) : The primary master node where the commands are executed
        snode (str) : The primary slave node where the commande are executed
        mastervol (str) : The name of the master volume
        slavevol (str): The name of the slave volume
    Kwargs:
        user (str): Since it's a root session, user is root or None
        force (bool) : Set to true if session needs to be created with force
            else it remains false as the default option

    Returns:
        bool : True if all the steps are successful, false if there are
              any failures in the middle
    """
    g.log.debug("Creating a common pem file on %s", mnode)
    ret, _, _ = georep_createpem(mnode)
    if not ret:
        g.log.error("Failed to create a common pem file on all the nodes "
                    "belonging to the cluster %s ", mnode)
        return False

    g.log.debug("Create geo-rep session from %s to %s", mnode, snode)
    ret, _, _ = georep_create(mnode, mastervol, snode,
                              slavevol, user, force)
    if not ret:
        g.log.error("Failed to create geo-rep session")
        return False

    g.log.debug("Setting up meta-volume on %s", mnode)
    ret, _, _ = georep_config_set(mnode, mastervol, snode, slavevol,
                                  "use_meta_volume", "True")
    if not ret:
        g.log.error("Failed to set up meta-volume")
        return False

    g.log.info("Pem file has been created and the keys have "
               "been pushed to all the slave nodes. The meta-volume "
               "has been successfully configured as well ")
    return True


def georep_nonroot_prerequisites(mnode, snodes, group, user, mntbroker_dir,
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
    ret = enable_shared_storage(mnode)
    if ret:
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

    g.log.debug("Set passwd for user account on slave")
    if not georep_geoaccount_setpasswd(snodes, group, user, "geopasswd"):
        g.log.error("Setting password failed on slaves")
        return False

    g.log.debug("Setup passwordless SSH between %s and %s", mnode, snodes[0])
    if not georep_ssh_keygen(mnode):
        g.log.error("ssh keygen is failed on %s", mnode)
        return False

    if not georep_ssh_copyid(mnode, snodes[0], user, "geopasswd"):
        g.log.error("ssh copy-id is failed from %s to %s", mnode, snodes[0])
        return False

    return True


def georep_create_nonroot_session(mnode, mastervol, snode, slavevol, user,
                                  force=False):
    """ Create mountbroker/non-root geo-rep session

    Args:
        mnode (str) : Master node for session creation
        mastervol (str) The name of the master volume
        snode (str): Slave node  for session creation
        slavevol (str) The name of the slave volume
        user (str): Specifies a user name
    Kwargs:
        force (bool): if master and slave are not of similar
        size
    Returns:
        bool: True if geo-rep session is created successfully
              Else False

    """

    g.log.debug("Create geo-rep pem keys")
    ret, _, _ = georep_createpem(mnode)
    if ret:
        g.log.error("Failed to create pem keys")
        return False

    g.log.debug("Create geo-rep session")
    ret, _, _ = georep_create(mnode, mastervol, snode, slavevol,
                              user, force)
    if ret:
        g.log.error("Failed to create geo-rep session")
        return False

    g.log.debug("Copy geo-rep pem keys onto all slave nodes")
    ret, _, _ = georep_set_pemkeys(snode, user, mastervol, slavevol)
    if ret:
        g.log.error("Failed to copy geo-rep pem keys onto all slave nodes")
        return False

    g.log.debug("Enable meta-volume")
    ret, _, _ = georep_config_set(mnode, mastervol, snode, slavevol,
                                  "use_meta_volume", "true")
    if ret:
        g.log.error("Failed to set meta-volume")
        return False

    return True
