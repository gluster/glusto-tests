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
   Description: Library for gluster geo-replication operations.
"""

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_init import restart_glusterd
from glustolibs.gluster.peer_ops import is_peer_connected
from glustolibs.gluster.geo_rep_ops import (georep_mountbroker_setup,
                                            georep_mountbroker_add_user,
                                            georep_create_pem,
                                            georep_create,
                                            georep_set_pem_keys,
                                            georep_config_set)
from glustolibs.gluster.shared_storage_ops import (enable_shared_storage,
                                                   is_shared_volume_mounted,
                                                   check_gluster_shared_volume)
from glustolibs.gluster.lib_utils import (group_add, ssh_copy_id,
                                          ssh_keygen, add_user, set_passwd,
                                          is_group_exists, is_user_exists,
                                          is_passwordless_ssh_configured)
from glustolibs.gluster.glusterdir import get_dir_contents


def georep_prerequisites(mnode, snode, passwd, user="root", group=None,
                         mntbroker_dir="/var/mountbroker-root",
                         nonrootpass=None):
    """
    Sets up all the prerequisites for geo-rep.

    Args:
        mnode(str): The primary master node where the commands are executed
        snode(str|list): slave nodes on which setup has to be completed.
        passwd(str): Password of the root user.

    Kwargs:
        user(str): User to be used to setup the geo-rep session.
                   (Default: root)
        mntbroker_dir(str): Mountbroker mount directory.
                            (Default: /var/mountbroker-root)
        group(str): Group under which geo-rep useraccount is setup.
                    (Default: None)
        nonrootpass(str): Password of the non-root user.(Default: None)

    Returns:
        bool : True if all the steps are successful, false if there are
              any failures in the middle

    """
    # Converting snode to list if string.
    if not isinstance(snode, list):
        snode = [snode]

    # Checking for blank username.
    if not user.strip():
        g.log.error("Blank username isn't possible.")
        return False

    # Checking if non-root user is given without userpassword.
    if user != "root" and nonrootpass is None:
        g.log.error("Non-root user specified without password.")
        return False

    # Checking and enabling shared storage on master cluster.
    ret = is_shared_volume_mounted(mnode)
    if not ret:

        ret = enable_shared_storage(mnode)
        if not ret:
            g.log.error("Failed to set cluster"
                        ".enable-shared-storage to enable.")
            return False

        # Check volume list to confirm gluster_shared_storage is created
        ret = check_gluster_shared_volume(mnode)
        if not ret:
            g.log.error("gluster_shared_storage volume not"
                        " created even after enabling it.")
            return False

    # Running prerequisites for non-root user.
    if user != "root" and group is not None:

        if len(snode) < 2:
            g.log.error("A list of all slave nodes is needed for non-root"
                        " setup as every slave node will have a non-root"
                        " user.")
            return False

        # Checking and creating a  group on all slave nodes.
        if not is_group_exists(snode, group):
            ret = group_add(snode, group)
            if not ret:
                g.log.error("Creating group: %s on all slave nodes failed.",
                            group)
                return False

        # Checking and creating a non-root user on all the nodes.
        if not is_user_exists(snode, user):
            ret = add_user(snode, user, group)
            if not ret:
                g.log.error("Creating user: %s in group: %s on all slave nodes"
                            " failed,", user, group)
                return False

            # Setting password for user on all the nodes.
            ret = set_passwd(snode, user, nonrootpass)
            if not ret:
                g.log.error("Setting password failed on slaves")
                return False

            # Setting up mount broker on first slave node.
            ret, _, _ = georep_mountbroker_setup(snode[0], group,
                                                 mntbroker_dir)
            if ret:
                g.log.error("Setting up of mount broker directory"
                            " failed on node: %s", snode[0])
                return False

    # Checking if ssh keys are present.
    ret = get_dir_contents(mnode, "~/.ssh/")
    if "id_rsa" not in ret or "id_rsa.pub" not in ret:
        ret = ssh_keygen(mnode)
        if not ret:
            g.log.error("Failed to create a common pem pub file.")
            return False

    # Setting up passwordless ssh to primary slave node.
    if not is_passwordless_ssh_configured(mnode, snode[0], user):
        if user != "root":
            ret = ssh_copy_id(mnode, snode[0], nonrootpass, user)
        else:
            ret = ssh_copy_id(mnode, snode[0], passwd, user)
        if not ret:
            g.log.error("Failed to setup passwordless ssh.")
            return False

    # Checking if pem files else running gsec_create.
    ret = get_dir_contents(mnode, "/var/lib/glusterd/geo-replication/")
    list_of_pem_files = [
        "common_secret.pem.pub", "secret.pem",
        "tar_ssh.pem", "gsyncd_template.conf",
        "secret.pem.pub", "tar_ssh.pem.pub"
        ]
    if ret != list_of_pem_files:
        ret, _, _ = georep_create_pem(mnode)
        if ret:
            g.log.error("Failed exeucte gluster system:: execute gsec_create.")
            return False
    return True


def georep_create_session(mnode, snode, mastervol, slavevol,
                          user="root", force=False, sync="rsync"):
    """ Create a geo-replication session between the master and
        the slave.

    Args:
        mnode(str): The primary master node where the commands are executed
        snode(str|list): slave node where the commande are executed
        mastervol(str): The name of the master volume
        slavevol(str): The name of the slave volume

    Kwargs:
        user (str): User to be used to create geo-rep session.(Default: root)
        force (bool) : Set to true if session needs to be created with force
                       else it remains false as the default option.
                       (Default: False)
        sync (str): Sync method to be used for geo-rep session.(Default:rsync)
    Returns:
        bool : True if all the steps are successful, false if there are
              any failures in the middle
    """
    # Converting snode to list if string.
    if not isinstance(snode, list):
        snode = [snode]

    # Checking for blank username.
    if not user.strip():
        g.log.error("Blank username isn't possible.")
        return False

    if sync not in ["rsync", "tarssh"]:
        g.log.error("Invalid sync method used. "
                    "%s is not a valid sync method.", sync)
        return False

    # Setting up root geo-rep session.
    if user == "root":
        g.log.debug("Creating root geo-rep session.")
        ret, _, _ = georep_create(mnode, mastervol, snode[0],
                                  slavevol, user, force)
        if ret:
            g.log.error("Failed to create geo-rep session")
            return False

        g.log.debug("Enabling meta-volume for master volume.")
        ret, _, _ = georep_config_set(mnode, mastervol, snode[0],
                                      slavevol, "use_meta_volume", "True")
        if ret:
            g.log.error("Failed to set up meta-volume for root "
                        "geo-rep session from %s to %s",
                        (mastervol, slavevol))
            return False

        # Setting up sync method if not rsync.
        g.log.debug("Enabling tarssh for master volume.")
        if sync == "tarssh":
            ret, _, _ = georep_config_set(mnode, mastervol, snode[0],
                                          slavevol, "sync_method", "tarssh")
            if ret:
                g.log.error("Failed to set sync method to tarssh for root "
                            "geo-rep session from %s to %s",
                            (mastervol, slavevol))
                return False
        return True

    # Setting up non-root geo-rep session.
    else:
        # Glusterd has to be restarted on all the slave nodes.
        if len(snode) < 2:
            g.log.error("A list of all slave nodes is needed for non-root"
                        " session to restart glusterd on all slaves after"
                        " adding it to mountbroker.")
            return False

        # Adding volume to mountbroker.
        g.log.debug("Creating a non-root geo-rep session.")
        ret, _, _ = georep_mountbroker_add_user(snode[0], slavevol, user)
        if ret:
            g.log.error("Failed to setup mountbroker.")
            return False

        # Restarting glusterd on all nodes.
        ret = restart_glusterd(snode)
        if not ret:
            g.log.error("Restarting glusterd failed.")
            return False

        # Checking if peers are in connected state or not.
        ret = is_peer_connected(snode[0], snode)
        if not ret:

            counter = 20
            while counter > 0:

                ret = is_peer_connected(snode[0], snode)
                if ret:
                    break
                sleep(3)
                counter += 1

        # Creating a geo-rep session.
        ret, _, _ = georep_create(mnode, mastervol, snode[0], slavevol,
                                  user, force)
        if ret:
            g.log.error("Failed to create geo-rep session.")
            return False

        # Setting up pem keys between master and slave node.
        g.log.debug("Copy geo-rep pem keys onto all slave nodes.")
        ret, _, _ = georep_set_pem_keys(snode[0], user, mastervol, slavevol)
        if ret:
            g.log.error("Failed to copy geo-rep pem keys onto all slave nodes")
            return False

        # Setting use_meta_volume to true.
        g.log.debug("Setting use_meta_volume to true.")
        ret, _, _ = georep_config_set(mnode, mastervol, snode[0], slavevol,
                                      "use_meta_volume", "true", user)
        if ret:
            g.log.error("Failed to set up meta-volume for %s "
                        "geo-rep session from %s to %s.",
                        (user, mastervol, slavevol))
            return False

        # Setting up sync method if not rsync.
        g.log.debug("Setting sync method to tarssh.")
        if sync == "tarssh":
            ret, _, _ = georep_config_set(mnode, mastervol, snode[0],
                                          slavevol, "sync_method", "tarssh",
                                          user)
            if ret:
                g.log.error("Failed to set sync method to tarssh for %s "
                            "geo-rep session from %s to %s",
                            (user, mastervol, slavevol))
                return False
        return True
