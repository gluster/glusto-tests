#!/usr/bin/env python
#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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

# pylint: disable=too-many-lines
"""
    Description: Library for nfs ganesha operations.
    Pre-requisite: Please install gdeploy package on the glusto-tests
    management node.
"""

import os
from glusto.core import Glusto as g
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.lib_utils import add_services_to_firewall
from glustolibs.gluster.shared_storage_ops import enable_shared_storage

GDEPLOY_CONF_DIR = "/usr/share/glustolibs/gdeploy_configs/"


def teardown_nfs_ganesha_cluster(servers, force=False):
    """
    Teardown nfs ganesha cluster

    Args:
        servers (list): Nodes in nfs-ganesha cluster to teardown entire
            cluster
        force (bool): if this option is set to True, then nfs ganesha cluster
            is teardown using force cleanup

    Returns:
        bool : True on successfully teardown nfs-ganesha cluster.
            False otherwise

    Example:
        teardown_nfs_ganesha_cluster(servers)
    """
    if force:
        g.log.info("Executing force cleanup...")
        for server in servers:
            cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --teardown "
                   "/var/run/gluster/shared_storage/nfs-ganesha")
            _, _, _ = g.run(server, cmd)
            cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --cleanup /var/run/"
                   "gluster/shared_storage/nfs-ganesha")
            _, _, _ = g.run(server, cmd)
            _, _, _ = stop_nfs_ganesha_service(server)
        return True
    ret, _, _ = disable_nfs_ganesha(servers[0])
    if ret != 0:
        g.log.error("Nfs-ganesha disable failed")
        return False
    return True


def add_node_to_nfs_ganesha_cluster(servers, node_to_add, vip):
    """Adds a node to nfs ganesha cluster using gdeploy

    Args:
        servers (list): Nodes of existing nfs-ganesha cluster.
        node_to_add (str): Node to add in existing nfs-ganesha cluster.
        vip (str): virtual IP of the node mentioned in 'node_to_add'
            param.

    Returns:
        bool : True on successfully adding node to nfs-ganesha cluster.
            False otherwise

    Example:
        add_node_to_nfs_ganesha_cluster(servers, node_to_add, vip)
    """

    conf_file = "add_node_to_nfs_ganesha_cluster.jinja"
    gdeploy_config_file = GDEPLOY_CONF_DIR + conf_file
    tmp_gdeploy_config_file = ("/tmp/" + os.path.splitext(conf_file)[0] +
                               ".conf")
    cluster_nodes = servers
    hosts = servers + [node_to_add]

    values_to_substitute_in_template = {'servers': hosts,
                                        'node_to_add': node_to_add,
                                        'cluster_nodes': cluster_nodes,
                                        'vip': vip}

    ret = g.render_template(gdeploy_config_file,
                            values_to_substitute_in_template,
                            tmp_gdeploy_config_file)
    if not ret:
        g.log.error("Failed to substitute values in %s file"
                    % tmp_gdeploy_config_file)
        return False

    cmd = "gdeploy -c " + tmp_gdeploy_config_file
    retcode, stdout, stderr = g.run_local(cmd)
    if retcode != 0:
        g.log.error("Failed to execute gdeploy cmd %s for adding node "
                    "in existing nfs ganesha cluster" % cmd)
        g.log.error("gdeploy console output for adding node in "
                    "existing nfs-ganesha cluster: %s" % stderr)

        return False

    g.log.info("gdeploy output for adding node in existing "
               "nfs-ganesha cluster: %s" % stdout)

    # pcs status output
    _, _, _ = g.run(servers[0], "pcs status")

    # Removing gdeploy conf file from /tmp
    os.remove(tmp_gdeploy_config_file)
    return True


def delete_node_from_nfs_ganesha_cluster(servers, node_to_delete):
    """Deletes a node from existing nfs ganesha cluster using gdeploy

    Args:
        servers (list): Nodes of existing nfs-ganesha cluster.
        node_to_delete (str): Node to delete from existing nfs-ganesha cluster

    Returns:
        bool : True on successfully creating nfs-ganesha cluster.
            False otherwise

    Example:
        delete_node_from_nfs_ganesha_cluster(servers, node_to_delete)
    """

    conf_file = "delete_node_from_nfs_ganesha_cluster.jinja"
    gdeploy_config_file = GDEPLOY_CONF_DIR + conf_file
    tmp_gdeploy_config_file = ("/tmp/" + os.path.splitext(conf_file)[0] +
                               ".conf")

    values_to_substitute_in_template = {'servers': servers,
                                        'node_to_delete': node_to_delete}

    ret = g.render_template(gdeploy_config_file,
                            values_to_substitute_in_template,
                            tmp_gdeploy_config_file)
    if not ret:
        g.log.error("Failed to substitute values in %s file"
                    % tmp_gdeploy_config_file)
        return False

    cmd = "gdeploy -c " + tmp_gdeploy_config_file
    retcode, stdout, stderr = g.run_local(cmd)
    if retcode != 0:
        g.log.error("Failed to execute gdeploy cmd %s for deleting node "
                    "from existing nfs ganesha cluster" % cmd)
        g.log.error("gdeploy console output for deleting node from "
                    "existing nfs-ganesha cluster: %s" % stderr)

        return False

    g.log.info("gdeploy output for deleting node from existing "
               "nfs-ganesha cluster: %s" % stdout)

    # pcs status output
    _, _, _ = g.run(servers[0], "pcs status")

    # Removing gdeploy conf file from /tmp
    os.remove(tmp_gdeploy_config_file)
    return True


def enable_nfs_ganesha(mnode):
    """Enables nfs-ganesha cluster in the storage pool.
       All the pre-requisites to create nfs-ganesha cluster
       has to be done prior to use this module.

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        enable_nfs_ganesha("abc.com")
    """

    cmd = "gluster nfs-ganesha enable --mode=script"
    return g.run(mnode, cmd)


def disable_nfs_ganesha(mnode):
    """Disables nfs-ganesha cluster in the storage pool.

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        disable_nfs_ganesha("abc.com")
    """

    cmd = "gluster nfs-ganesha disable --mode=script"
    return g.run(mnode, cmd)


def export_nfs_ganesha_volume(mnode, volname):
    """Exports nfs-ganesha volume.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): Volume name

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        export_nfs_ganesha_volume("abc.com", volname)
    """

    cmd = "gluster volume set %s ganesha.enable on" % volname
    return g.run(mnode, cmd)


def unexport_nfs_ganesha_volume(mnode, volname):
    """Unexport nfs-ganesha volume.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): Volume name

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        unexport_nfs_ganesha_volume("abc.com", volname)
    """

    cmd = "gluster volume set %s ganesha.enable off" % volname
    return g.run(mnode, cmd)


def run_refresh_config(mnode, volname):
    """Runs refresh config on nfs ganesha volume.

    Args:
        mnode (str): Node in which refresh config command will
            be executed.
        volname (str): volume name

    Returns:
        bool : True on successfully running refresh config on
            nfs-ganesha volume. False otherwise

    Example:
        run_refresh_config("abc.com", volname)
    """

    conf_file = "nfs_ganesha_refresh_config.jinja"
    gdeploy_config_file = GDEPLOY_CONF_DIR + conf_file

    tmp_gdeploy_config_file = ("/tmp/" + os.path.splitext(conf_file)[0] +
                               ".conf")

    values_to_substitute_in_template = {'server': mnode,
                                        'volname': volname}

    ret = g.render_template(gdeploy_config_file,
                            values_to_substitute_in_template,
                            tmp_gdeploy_config_file)
    if not ret:
        g.log.error("Failed to substitute values in %s file"
                    % tmp_gdeploy_config_file)
        return False

    cmd = "gdeploy -c " + tmp_gdeploy_config_file
    retcode, stdout, stderr = g.run_local(cmd)
    if retcode != 0:
        g.log.error("Failed to execute gdeploy cmd %s for running "
                    "refresh config on nfs ganesha volume" % cmd)
        g.log.error("gdeploy console output for running refresh config "
                    "on nfs ganesha volume: %s" % stderr)

        return False

    g.log.info("gdeploy output for running refresh config "
               "on nfs ganesha volume: %s" % stdout)

    # Removing the gdeploy conf file from /tmp
    os.remove(tmp_gdeploy_config_file)
    return True


def update_volume_export_configuration(mnode, volname, config_to_update):
    """Updates volume export configuration and runs
       refresh config for the volume.

    Args:
        mnode (str): Node in which refresh config command will
            be executed.
        volname (str): volume name
        config_to_update (str): config lines to update in volume
            export configuration file.

    Returns:
        bool : True on successfully updating export config for
            nfs-ganesha volume. False otherwise

    Example:
        update_volume_export_configuration(mnode, volname, config_to_update)
    """

    conf_file = "nfs_ganesha_update_export_file.jinja"
    gdeploy_config_file = GDEPLOY_CONF_DIR + conf_file
    tmp_gdeploy_config_file = ("/tmp/" + os.path.splitext(conf_file)[0] +
                               ".conf")

    values_to_substitute_in_template = {'server': mnode,
                                        'volname': volname,
                                        'config_to_update': config_to_update}

    ret = g.render_template(gdeploy_config_file,
                            values_to_substitute_in_template,
                            tmp_gdeploy_config_file)
    if not ret:
        g.log.error("Failed to substitute values in %s file"
                    % tmp_gdeploy_config_file)
        return False

    cmd = "gdeploy -c " + tmp_gdeploy_config_file
    retcode, stdout, stderr = g.run_local(cmd)
    if retcode != 0:
        g.log.error("Failed to execute gdeploy cmd %s to update export "
                    "configuration on nfs ganesha volume" % cmd)
        g.log.error("gdeploy console output to update export "
                    "configuration on nfs ganesha volume: %s" % stderr)

        return False

    g.log.info("gdeploy output to update export configuration "
               "on nfs ganesha volume: %s" % stdout)

    # Removing the gdeploy conf file from /tmp
    os.remove(tmp_gdeploy_config_file)
    return True


def is_nfs_ganesha_cluster_in_healthy_state(mnode):
    """
       Checks whether nfs ganesha cluster is in healthy state.

    Args:
        mnode (str): Node in which cmd command will
            be executed.

    Returns:
        bool : True if nfs ganesha cluster is in healthy state.
            False otherwise

    Example:
        is_nfs_ganesha_cluster_in_healthy_state(mnode)
    """

    cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --status " +
           "/run/gluster/shared_storage/nfs-ganesha/ | grep " +
           " 'Cluster HA Status' | cut -d ' ' -f 4 ")

    retcode, stdout, _ = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to check "
                    "if cluster is in healthy state")
        return False

    if stdout.strip('\n') != "HEALTHY":
        g.log.error("nfs-ganesha cluster is not in healthy state. Current "
                    "cluster state: %s " % stdout)
        return False

    cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --status " +
           "/run/gluster/shared_storage/nfs-ganesha/ | grep -v" +
           " 'Online' | grep -v 'Cluster' | cut -d ' ' -f 1 | " +
           "sed s/'-cluster_ip-1'//g")

    retcode, stdout, _ = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to parse "
                    "for the cluster resources")
        return False

    cluster_list = stdout.split("\n")
    cluster_list = list(filter(None, cluster_list))

    cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --status " +
           "/run/gluster/shared_storage/nfs-ganesha/ | grep -v" +
           " 'Online' | grep -v 'Cluster' | cut -d ' ' -f 1 | " +
           "sed s/'-cluster_ip-1'//g")

    retcode, stdout, _ = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to parse "
                    "for the hostnames in cluster")
        return False

    host_list = stdout.split("\n")
    host_list = list(filter(None, host_list))

    if (cluster_list != []) and (cluster_list == host_list):
        g.log.info("nfs ganesha cluster is in HEALTHY state")
        return True

    g.log.error("nfs ganesha cluster is not in HEALTHY state")
    return False


def is_nfs_ganesha_cluster_in_failover_state(mnode, failed_nodes):
    """
       Checks whether nfs ganesha cluster is in failover state.

    Args:
        mnode (str): Node in which cmd command will
            be executed.
        failed_nodes (list): Nodes in which nfs-ganesha process
            are down.

    Returns:
        bool : True if nfs ganesha cluster is in failover state.
            False otherwise

    Example:
        is_nfs_ganesha_cluster_in_failover_state(mnode, failed_nodes)
    """

    cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --status " +
           "/run/gluster/shared_storage/nfs-ganesha/ | grep " +
           " 'Cluster HA Status' | cut -d ' ' -f 4 ")

    retcode, stdout, _ = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to check "
                    "if cluster is in failover state")
        return False

    if stdout.strip('\n') != "FAILOVER":
        g.log.error("nfs-ganesha cluster is not in failover state. Current "
                    "cluster state: %s " % stdout)
        return False

    cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --status " +
           "/run/gluster/shared_storage/nfs-ganesha/ | grep -v" +
           " 'Online' | grep -v 'Cluster' | cut -d ' ' -f 1 | " +
           "sed s/'-cluster_ip-1'//g")

    retcode, stdout, _ = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to parse "
                    "for the cluster resources")
        return False

    cluster_list = stdout.split("\n")
    cluster_list = list(filter(None, cluster_list))

    cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --status " +
           "/run/gluster/shared_storage/nfs-ganesha/ | grep -v" +
           " 'Online' | grep -v 'Cluster' | cut -d ' ' -f 2 | " +
           "sed s/'-cluster_ip-1'//g")

    retcode, stdout, _ = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to parse "
                    "for the hostnames in cluster")
        return False

    host_list = stdout.split("\n")
    host_list = list(filter(None, host_list))

    ret = True
    for cluster_node, host_node in zip(cluster_list, host_list):
        if cluster_node in failed_nodes:
            if cluster_node == host_node:
                g.log.error("failover status: failed node %s isn't taken over"
                            " by other node in nfs-ganesha cluster" %
                            cluster_node)
                ret = False
            else:
                g.log.info("failover status: failed node %s is successfully "
                           "failovered to node %s" %
                           (cluster_node, host_node))
        else:
            if cluster_node != host_node:
                g.log.error("Unexpected. Other nodes are in failover state. "
                            "Node %s is takenover by node %s in nfs-ganesha "
                            "cluster" % (cluster_node, host_node))
                ret = False
    return ret


def is_nfs_ganesha_cluster_in_bad_state(mnode):
    """
       Checks whether nfs ganesha cluster is in bad state.

    Args:
        mnode (str): Node in which cmd command will
            be executed.

    Returns:
        bool : True if nfs ganesha cluster is in bad state.
            False otherwise

    Example:
        is_nfs_ganesha_cluster_in_bad_state(mnode)
    """

    cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --status " +
           "/run/gluster/shared_storage/nfs-ganesha/ | grep " +
           " 'Cluster HA Status' | cut -d ' ' -f 4 ")

    retcode, stdout, _ = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to check "
                    "if cluster is in bad state")
        return False

    if stdout.strip('\n') != "BAD":
        g.log.error("nfs-ganesha cluster is not in bad state. Current "
                    "cluster state: %s " % stdout)
        return False


def is_nfs_ganesha_cluster_exists(mnode):
    """
       Checks whether nfs ganesha cluster exists.

    Args:
        mnode (str): Node in which cmd command will
            be executed.

    Returns:
        bool : True if nfs ganesha cluster exists.
            False otherwise

    Example:
        is_nfs_ganesha_cluster_exists(mnode)
    """

    cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --status " +
           "/run/gluster/shared_storage/nfs-ganesha/ | grep -v" +
           " 'Online' | grep -v 'Cluster' | cut -d ' ' -f 1 | " +
           "sed s/'-cluster_ip-1'//g")

    retcode, stdout, _ = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to parse "
                    "for the cluster resources")
        return False

    cluster_list = stdout.split("\n")
    cluster_list = list(filter(None, cluster_list))

    if cluster_list != []:
        g.log.info("nfs ganesha cluster exists")
        return True

    g.log.error("nfs ganesha cluster not exists")
    return False


def stop_nfs_ganesha_service(mnode):
    """Stops nfs-ganesha service in given node.

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        stop_nfs_ganesha_service(mnode)
    """

    cmd = "systemctl stop nfs-ganesha"
    return g.run(mnode, cmd)


def start_nfs_ganesha_service(mnode):
    """Starts nfs-ganesha service in given node.

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        start_nfs_ganesha_service(mnode)
    """

    cmd = "systemctl start nfs-ganesha"
    return g.run(mnode, cmd)


def kill_nfs_ganesha_service(mnode):
    """Kills nfs-ganesha service in given node.

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        kill_nfs_ganesha_service(mnode)
    """

    cmd = "kill -9 $(pgrep ganesha.nfsd)"
    return g.run(mnode, cmd)


def start_pacemaker_service(mnode):
    """Starts pacemaker service in given node.

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        start_pacemaker_service(mnode)
    """

    cmd = "systemctl start pacemaker"
    return g.run(mnode, cmd)


def create_nfs_ganesha_cluster(servers, vips):
    """
    Creating a ganesha HA cluster

    Args:
        servers(list): Hostname of ganesha nodes
        vips(list): VIPs that has to be assigned for each nodes
    Returns:
        True(bool): If configuration of ganesha cluster is success
        False(bool): If failed to configure ganesha cluster
    """
    # pylint: disable=too-many-return-statements
    ganesha_mnode = servers[0]

    # Configure ports in ganesha servers
    g.log.info("Defining statd service ports")
    ret = configure_ports_on_servers(servers)
    if not ret:
        g.log.error("Failed to set statd service ports on nodes.")
        return False

    # Firewall settings for nfs-ganesha
    ret = ganesha_server_firewall_settings(servers)
    if not ret:
        g.log.error("Firewall settings for nfs ganesha has failed.")
        return False
    g.log.info("Firewall settings for nfs ganesha was success.")

    # Enable shared storage if not present
    ret, _, _ = g.run(ganesha_mnode,
                      "gluster v list | grep 'gluster_shared_storage'")
    if ret != 0:
        if not enable_shared_storage(ganesha_mnode):
            g.log.error("Failed to enable shared storage")
            return False
        g.log.info("Enabled gluster shared storage.")
    else:
        g.log.info("Shared storage is already enabled.")

    # Enable the glusterfssharedstorage.service and nfs-ganesha service
    for server in servers:
        cmd = "systemctl enable glusterfssharedstorage.service"
        ret, _, _ = g.run(server, cmd)
        if ret != 0:
            g.log.error("Failed to enable glusterfssharedstorage.service "
                        "on %s", server)
            return False

        ret, _, _ = g.run(server, "systemctl enable nfs-ganesha")
        if ret != 0:
            g.log.error("Failed to enable nfs-ganesha service on %s", server)
            return False

    # Password less ssh for nfs
    ret = create_nfs_passwordless_ssh(ganesha_mnode, servers)
    if not ret:
        g.log.error("Password less ssh between nodes failed.")
        return False
    g.log.info("Password less ssh between nodes successful.")

    # Create ganesha-ha.conf file
    tmp_ha_conf = "/tmp/ganesha-ha.conf"
    create_ganesha_ha_conf(servers, vips, tmp_ha_conf)

    # Check whether ganesha-ha.conf file is created
    if not os.path.isfile(tmp_ha_conf):
        g.log.error("Failed to create ganesha-ha.conf")
        return False

    # Cluster auth setup
    ret = cluster_auth_setup(servers)
    if not ret:
        g.log.error("Failed to configure cluster services")
        return False

    # Create nfs-ganesha directory in shared storage
    dpath = '/var/run/gluster/shared_storage/nfs-ganesha'
    mkdir(ganesha_mnode, dpath)

    # Copy the config files to shared storage
    cmd = 'cp -p /etc/ganesha/ganesha.conf %s/' % dpath
    ret, _, _ = g.run(ganesha_mnode, cmd)
    if ret != 0:
        g.log.error("Failed to copy ganesha.conf to %s/", dpath)
        return False

    g.upload(ganesha_mnode, tmp_ha_conf, '%s/' % dpath)

    # Create backup of ganesha-ha.conf file in ganesha_mnode
    g.upload(ganesha_mnode, tmp_ha_conf, '/etc/ganesha/')

    # Enabling ganesha
    g.log.info("Enable nfs-ganesha")
    ret, _, _ = enable_nfs_ganesha(ganesha_mnode)

    if ret != 0:
        g.log.error("Failed to enable ganesha")
        return False

    g.log.info("Successfully created ganesha cluster")

    # pcs status output
    _, _, _ = g.run(ganesha_mnode, "pcs status")

    return True


def ganesha_server_firewall_settings(servers):
    """
    Do firewall settings for ganesha

    Args:
        servers(list): Hostname of ganesha nodes
    Returns:
        True(bool): If successfully set the firewall settings
        False(bool): If failed to do firewall settings
    """
    services = ['nfs', 'rpc-bind', 'high-availability', 'nlm', 'mountd',
                'rquota']

    ret = add_services_to_firewall(servers, services, True)
    if not ret:
        g.log.error("Failed to set firewall zone permanently on ganesha nodes")
        return False

    for server in servers:
        ret, _, _ = g.run(server, "firewall-cmd --add-port=662/tcp "
                          "--add-port=662/udp")
        if ret != 0:
            g.log.error("Failed to add firewall port in %s", server)
            return False
        ret, _, _ = g.run(server, "firewall-cmd --add-port=662/tcp "
                          "--add-port=662/udp --permanent")
        if ret != 0:
            g.log.error("Failed to add firewall port permanently in %s",
                        server)
            return False
    return True


def ganesha_client_firewall_settings(clients):
    """
    Do firewall settings in clients

    Args:
        clients(list): List of clients
    Returns:
        True(bool): If successfully set the firewall settings
        False(bool): If failed to do firewall settings
    """
    for client in clients:
        _, zone_name, _ = g.run(client,
                                "firewall-cmd --get-active-zones | head -n 1")
        if not zone_name:
            g.log.error("Failed to get active zone name in %s", client)
            return False

        zone_name = zone_name.strip()
        ret, _, _ = g.run(client, "firewall-cmd --zone=%s "
                          "--add-port=662/tcp --add-port=662/udp "
                          "--add-port=32803/tcp --add-port=32769/udp "
                          "--add-port=2049/udp" % zone_name)
        if ret != 0:
            g.log.error("Failed to set firewall ports in %s", client)
            return False

        ret, _, _ = g.run(client, "firewall-cmd --zone=%s "
                          "--add-port=662/tcp --add-port=662/udp "
                          "--add-port=32803/tcp --add-port=32769/udp "
                          "--add-port=2049/udp"
                          " --permanent" % zone_name)
        if ret != 0:
            g.log.error("Failed to set firewall ports permanently in %s",
                        client)
            return False
    return True


def create_nfs_passwordless_ssh(mnode, gnodes, guser='root'):
    """
    Enable key-based SSH authentication without password on all the HA nodes

    Args:
        mnode(str): Hostname of ganesha maintenance node.
        snodes(list): Hostname of all ganesha nodes including maintenance node
        guser(str): User for setting password less ssh
    Returns:
        True(bool): On success
        False(bool): On failure
    """
    loc = '/var/lib/glusterd/nfs'
    # Generate key on one node if not already present
    ret, _, _ = g.run(mnode, "test -e %s/secret.pem" % loc)
    if ret != 0:
        cmd = "yes n | ssh-keygen -f %s/secret.pem -t rsa -N ''" % loc
        g.log.info("Generating public key on %s", mnode)
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            g.log.error("Failed to generate ssh key")
            return False

    # Deploy the generated public key from mnode to all the nodes
    # (including mnode)
    g.log.info("Deploying the generated public key from %s to all the nodes",
               mnode)
    for node in gnodes:
        cmd = "ssh-copy-id -i %s/secret.pem.pub %s@%s" % (loc, guser, node)
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            g.log.error("Failed to deploy the public key from %s to %s",
                        mnode, node)
            return False

    # Copy the ssh key pair from mnode to all the nodes in the Ganesha-HA
    # cluster
    g.log.info("Copy the ssh key pair from %s to other nodes in the "
               "Ganesha-HA cluster" % mnode)
    for node in gnodes:
        if node != mnode:
            cmd = ("scp -i %s/secret.pem %s/secret.* %s@%s:%s/"
                   % (loc, loc, guser, node, loc))
            ret, _, _ = g.run(mnode, cmd)
            if ret != 0:
                g.log.error("Failed to copy the ssh key pair from %s to %s",
                            mnode, node)
                return False

    return True


def create_ganesha_ha_conf(hostnames, vips, temp_ha_file):
    """
    Create temporary ganesha-ha.conf file

    Args:
        hostnames(list): Hostname of ganesha nodes
        vips(list): VIPs that has to be assigned for each nodes
        temp_ha_file(str): temporary local file to create ganesha-ha config
    """
    hosts = ','.join(hostnames)

    with open(temp_ha_file, 'wb') as fhand:
        fhand.write('HA_NAME="ganesha-ha-360"\n')
        fhand.write('HA_CLUSTER_NODES="%s"\n' % hosts)
        for (hostname, vip) in zip(hostnames, vips):
            fhand.write('VIP_%s="%s"\n' % (hostname, vip))


def cluster_auth_setup(servers):
    """
    Configuring the Cluster Services

    Args:
        servers(list): Hostname of ganesha nodes
    Returns:
        True(bool): If configuration of cluster services is success
        False(bool): If failed to configure cluster services
    """
    result = True
    for node in servers:
        # Enable pacemaker.service
        ret, _, _ = g.run(node, "systemctl enable pacemaker.service")
        if ret != 0:
            g.log.error("Failed to enable pacemaker service in %s", node)

        # Start pcsd
        ret, _, _ = g.run(node, "systemctl start pcsd")
        if ret != 0:
            g.log.error("failed to start pcsd on %s", node)
            return False

        # Enable pcsd on the system
        ret, _, _ = g.run(node, "systemctl enable pcsd")
        if ret != 0:
            g.log.error("Failed to enable pcsd in %s", node)

        # Set a password for the user ‘hacluster’ on all the nodes
        ret, _, _ = g.run(node, "echo hacluster | passwd --stdin hacluster")
        if ret != 0:
            g.log.error("unable to set password for hacluster on %s", node)
            return False

    # Perform cluster authentication between the nodes
    for node in servers:
        ret, _, _ = g.run(node, "pcs cluster auth %s -u hacluster -p "
                                "hacluster" % ' '.join(servers))
        if ret != 0:
            g.log.error("pcs cluster auth command failed on %s", node)
            result = False
    return result


def configure_ports_on_servers(servers):
    """
    Define ports for statd service

    Args:
        servers(list): List of nodes where the port has to be set
    Returns:
        True(bool): On success
        False(bool): On failure
    """
    cmd = "sed -i '/STATD_PORT/s/^#//' /etc/sysconfig/nfs"
    for server in servers:
        ret, _, _ = g.run(server, cmd)
        if ret != 0:
            g.log.error("Failed to set statd service port in %s", server)
            return False

        ret, _, _ = g.run(server, "systemctl restart nfs-config")
        if ret != 0:
            g.log.error("Failed to restart nfs-config in %s", server)
            return False

        ret, _, _ = g.run(server, "systemctl restart rpc-statd")
        if ret != 0:
            g.log.error("Failed to restart rpc-statd in %s", server)
            return False
    return True


def configure_ports_on_clients(clients):
    """
    Define ports for statd service

    Args:
        clients(list): List of clients where the port has to be set
    Returns:
        True(bool): On success
        False(bool): On failure
    """
    for client in clients:
        # Configure ports
        cmd = ("sed -i -e '/STATD_PORT/s/^#//' -e '/LOCKD_TCPPORT/s/^#//' "
               "-e '/LOCKD_UDPPORT/s/^#//' /etc/sysconfig/nfs")
        ret, _, _ = g.run(client, cmd)
        if ret != 0:
            g.log.error("Failed to edit /etc/sysconfig/nfs file in %s",
                        client)
            return False

        ret, _, _ = g.run(client, "systemctl restart nfs-config")
        if ret != 0:
            g.log.error("Failed to restart nfs-config in %s", client)
            return False

        ret, _, _ = g.run(client, "systemctl restart rpc-statd")
        if ret != 0:
            g.log.error("Failed to restart rpc-statd in %s", client)
            return False

        ret, _, _ = g.run(client, "systemctl restart nfslock")
        if ret != 0:
            g.log.error("Failed to restart nfslock in %s", client)
            return False
    return True


def refresh_config(mnode, volname):
    """
    Run refresh-config for exported volume

    Args:
        mnode(str): Ip/hostname of one node in the cluster
        volname(str): Volume name for which refresh-config has to be done
    Returns:
        True(bool): On success
        False(bool): On failure
    """
    cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --refresh-config /var/run/"
           "gluster/shared_storage/nfs-ganesha %s" % volname)

    ret, _, _ = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Error in running the refresh-config script for %s"
                    % volname)
        return False
    g.log.info("refresh-config script successfully ran for %s " % volname)
    return True


def set_root_squash(mnode, volname, squash=True, do_refresh_config=True):
    """
    Modify volume export file to enable or disable root squash

    Args:
        mnode(str): Ip/hostname of one node in the cluster
        volname(str): Volume name for which refresh-config has to be done
        squash(bool): 'True' to enable and 'False' to disable root squash
        do_refresh_config(bool): Value to decide refresh-config has to be
            executed or not after modifying export file
    Returns:
        True(bool): On success
        False(bool): On failure
    """
    if squash:
        cmd = ("sed -i  s/'Squash=.*'/'Squash=\"Root_squash\";'/g /var/run/"
               "gluster/shared_storage/nfs-ganesha/exports/export.%s.conf"
               % volname)
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            g.log.error("Error in editing the export file of %s" % volname)
            return False
        g.log.info("Edited the export file of volume %s successfully to "
                   "enable root squash" % volname)
    else:
        cmd = ("sed -i  s/'Squash=.*'/'Squash=\"No_root_squash\";'/g /var/"
               "run/gluster/shared_storage/nfs-ganesha/exports/"
               "export.%s.conf" % volname)
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            g.log.error("Error in editing the export file of %s" % volname)
            return False
        g.log.info("Edited the export file of volume %s successfully to "
                   "enable root squash" % volname)

    if do_refresh_config:
        return refresh_config(mnode, volname)
    return True


def set_acl(mnode, volname, acl=True, do_refresh_config=True):
    """
    Modify volume export file to enable or disable ACL

    Args:
        mnode(str): Ip/hostname of one node in the cluster
        volname(str): Volume name for which refresh-config has to be done
        acl(bool): 'True' to enable and 'False' to disable ACL
        do_refresh_config(bool): Value to decide refresh-config has to be
            executed or not after modifying export file
    Returns:
        True(bool): On success
        False(bool): On failure
    """
    if acl:
        cmd = ("sed -i s/'Disable_ACL = .*'/'Disable_ACL = false;'/g /var"
               "/run/gluster/shared_storage/nfs-ganesha/exports/"
               "export.%s.conf" % volname)
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            g.log.error("Error in editing the export file of %s" % volname)
            return False
        g.log.info("Edited the export file of volume %s successfully to "
                   "enable acl " % volname)
    else:
        cmd = ("sed -i s/'Disable_ACL = .*'/'Disable_ACL = true;'/g /var/"
               "run/gluster/shared_storage/nfs-ganesha/exports/"
               "export.%s.conf" % volname)
        ret, _, _ = g.run(mnode, cmd)
        if ret != 0:
            g.log.error("Error in editing the export file of %s" % volname)
            return False
        g.log.info("Edited the export file of volume %s successfully to "
                   "disable acl " % volname)

    if do_refresh_config:
        return refresh_config(mnode, volname)
    return True
