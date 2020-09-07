#  Copyright (C) 2019 Red Hat, Inc. <http://www.redhat.com>
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


def georep_create_pem(mnode):
    """ Creates a common pem pub file on all the nodes in the master and
        is used to implement the passwordless SSH connection
    Args:
        mnode (str): Node on which cmd is to be executed
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    cmd = "gluster system:: execute gsec_create"
    return g.run(mnode, cmd)


def georep_set_pem_keys(mnode, useraccount, mastervol, slavevol):
    """ Sets geo-rep pem keys

    Args:
        mnode (str): Node on which command is to be executed
        useraccount (str) : User with which geo-rep is to be set up
        mastervol (str) : The master volume
        slavevol (str): The slave volume

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    cmd = ("/usr/libexec/glusterfs/set_geo_rep_pem_keys.sh %s %s %s" %
           (useraccount, mastervol, slavevol))
    return g.run(mnode, cmd)


def georep_mountbroker_setup(mnode, groupname, directory):
    """ Sets up mountbroker root directory and group

    Args:
        mnode (str): Node on which command is to be executed
        groupname (str) : Specifies the groupname used
        directory (str) : Specifies mountbroker root directory

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    cmd = "gluster-mountbroker setup %s %s" % (directory, groupname)
    return g.run(mnode, cmd)


def georep_mountbroker_add_user(mnode, slavevol, useraccount):
    """ Adds the volume and user to the mountbroker

    Args:
        mnode (str): Node on which command is to be executed
        slavevol (str) : The slave volume name
        useraccount (str): The user with which geo-rep is to be setup

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    cmd = "gluster-mountbroker add %s %s" % (slavevol, useraccount)
    return g.run(mnode, cmd)


def georep_mountbroker_status(mnode):
    """ Displays the status of every peer node in the slave cluster

    Args:
        mnode (str): Node on which command is to be executed


    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    cmd = "gluster-mountbroker status"
    return g.run(mnode, cmd)


def georep_mountbroker_remove_user(mnode, slavevol, useraccount):
    """ Remove the volume and user from the mountbroker

    Args:
        mnode (str): Node on which command is to be executed
        slavevol (str) : The slave volume name
        useraccount (str): The user with which geo-rep is to be setup

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    cmd = ("gluster-mountbroker remove --volume %s --user %s"
           % (slavevol, useraccount))
    return g.run(mnode, cmd)


def georep_status(mnode, mastervol, slaveip, slavevol, user=None):
    """Shows the status of the geo-replication session
    Args:
        mnode (str): Node on which cmd is to be executed
        mastervol (str):The name of the master volume
        slaveip (str): SlaveIP
        slavevol(str): The name of the slave volume
    Kwargs:
        user (str): If not set, the default is a root-user
            If specified, non-root user participates in geo-rep
            session
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.
            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.
            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    if user:
        cmd = ("gluster volume geo-replication %s %s@%s::%s status" %
               (mastervol, user, slaveip, slavevol))
    else:
        cmd = ("gluster volume geo-replication %s %s::%s status" %
               (mastervol, slaveip, slavevol))
    return g.run(mnode, cmd)


def georep_create(mnode, mastervol, slaveip, slavevol, user=None, force=False):
    """Pushes the keys to all the slave nodes and creates a geo-rep session
    Args:
        mnode (str) : Node on which cmd is to be executed
        mastervol (str) : The name of the mastervol
        slaveip (str): SlaveIP
        slavevol (str) The name of the slave volume
    kwargs:
        force (bool): If this option is set to True, then create geo-rep
            session will be executed with the force option.
            If it is set to False, then the geo-rep session is created
            without the force option
        user (str): If not set, the default is a root-user
            If specified, non-root user participates in the geo-rep
            session

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    if user:
        cmd = ("gluster volume geo-replication %s %s@%s::%s create "
               "push-pem " % (mastervol, user, slaveip, slavevol))
    else:
        cmd = ("gluster volume geo-replication %s %s::%s create push-pem" %
               (mastervol, slaveip, slavevol))
    if force:
        cmd = (cmd + " force")
    return g.run(mnode, cmd)


def georep_config_get(mnode, mastervol, slaveip, slavevol, config_key,
                      user=None):
    """ All the available configurable geo-rep options can be got
        using the config_key and seeing what it has been set to

    Args:
        mnode (str) : Node on which cmd is to be executed
        mastervol (str) : The name of the mastervol
        slaveip (str): SlaveIP
        slavevol (str) The name of the slave volume
        config_key (str): The configurable options available in geo-replication
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution. In this case, it contains value of
            config.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    if user:
        cmd = ("gluster volume geo-replication %s %s@%s::%s config %s" %
               (mastervol, user, slaveip, slavevol, config_key))
    else:
        cmd = ("gluster volume geo-replication %s %s::%s config %s" %
               (mastervol, slaveip, slavevol, config_key))
    return g.run(mnode, cmd)


def georep_config_set(mnode, mastervol, slaveip, slavevol, config, value,
                      user=None):
    """ All the available configurable geo-rep options can be set with a
        specific command if required or
        just with the config parameter
    Args:
        mnode (str) : Node on which cmd is to be executed
        mastervol (str) : The name of the mastervol
        slaveip (str): SlaveIP
        slavevol (str) The name of the slave volume
        config (str): The configurable options available in geo-replication
    Kwargs:
        value (str): The value for the geo-rep config
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    if user:
        cmd = ("gluster volume geo-replication %s %s@%s::%s config %s %s" %
               (mastervol, user, slaveip, slavevol, config, value))
    else:
        cmd = ("gluster volume geo-replication %s %s::%s config %s %s" %
               (mastervol, slaveip, slavevol, config, value))
    return g.run(mnode, cmd)


def georep_start(mnode, mastervol, slaveip, slavevol, user=None, force=False):
    """Starts the Geo-replication session
    Args:
        mnode (str): Node on which cmd is to be executed
        mastervol (str):The name of the master volume
        slaveip (str): SlaveIP
        slavevol (str): The name of the slave volume

    kwargs:
        force (bool): If this option is set to True, then the geo-rep
            session will be started with the force option.
            If it is set to False, then the session will be started
            without the force option -- which is the default option
        user (str): If not set, the default is a root-user
            If specified, non-root user participates in the geo-rep
            session

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    if user:
        cmd = ("gluster volume geo-replication %s %s@%s::%s start " %
               (mastervol, user, slaveip, slavevol))
    else:
        cmd = ("gluster volume geo-replication %s %s::%s start " %
               (mastervol, slaveip, slavevol))
    if force:
        cmd = (cmd + "force")
    return g.run(mnode, cmd)


def georep_stop(mnode, mastervol, slaveip, slavevol, user=None, force=False):
    """Stops a geo-repication session

    Args:
        mnode (str): Node on which cmd is to be executed
        mastervol (str):The name of the master volume
        slaveip (str): SlaveIP
        slavevol (str): The name of the slave volume
    kwargs:
        force (bool): If this option is set to True, then the geo-rep
            session will be stopped with the force option.
            If it is set to False, then the session will be stopped
            without the force option --which is the default option
        user (str): If not set, the default is a root-user
            If specified, non-root user participates in the geo-rep
            session

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    if user:
        cmd = ("gluster volume geo-replication %s %s@%s::%s stop " %
               (mastervol, user, slaveip, slavevol))
    else:
        cmd = ("gluster volume geo-replication %s %s::%s stop " %
               (mastervol, slaveip, slavevol))
    if force:
        cmd = (cmd + "force")
    return g.run(mnode, cmd)


def georep_pause(mnode, mastervol, slaveip, slavevol, user=None):
    """Pauses the geo-replication session
    Args:
        mnode (str): Node on which cmd is to be executed
        mastervol (str):The name of the master volume
        slaveip (str): SlaveIP
        slavevol (str): The name of the slave volume
    Kwargs:
        user (str): If not set, the default is a root-user
        If specified, non-root user participates in geo-rep
        session
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    if user:
        cmd = ("gluster volume geo-replication %s %s@%s::%s pause" %
               (mastervol, user, slaveip, slavevol))
    else:
        cmd = ("gluster volume geo-replication %s %s::%s pause" %
               (mastervol, slaveip, slavevol))
    return g.run(mnode, cmd)


def georep_resume(mnode, mastervol, slaveip, slavevol, user=None):
    """Resumes the geo-replication session
    Args:
        mnode (str): Node on which cmd is to be executed
        mastervol (str):The name of the master volume
        slaveip (str): SlaveIP
        slavevol (str): The name of the slave volume
    Kwargs:
        user (str): If not set, the default is a root-user
        If specified, non-root user participates in geo-rep
        session
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    if user:
        cmd = ("gluster volume geo-replication %s %s@%s::%s resume" %
               (mastervol, user, slaveip, slavevol))
    else:
        cmd = ("gluster volume geo-replication %s %s::%s resume" %
               (mastervol, slaveip, slavevol))
    return g.run(mnode, cmd)


def georep_delete(mnode, mastervol, slaveip, slavevol, user=None):
    """Deletes the geo-replication session
    Args:
        mnode (str): Node on which cmd is to be executed
        mastervol (str):The name of the master volume
        slaveip (str): SlaveIP
        slavevol (str): The name of the slave volume
    Kwargs:
        user (str): If not set, the default is a root-user
            If specified, non-root user participates in geo-rep
            session
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    if user:
        cmd = ("gluster volume geo-replication %s %s@%s::%s delete" %
               (mastervol, user, slaveip, slavevol))
    else:
        cmd = ("gluster volume geo-replication %s %s::%s delete" %
               (mastervol, slaveip, slavevol))
    return g.run(mnode, cmd)
