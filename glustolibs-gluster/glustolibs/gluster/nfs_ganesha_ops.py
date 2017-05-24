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

"""
    Description: Library for nfs ganesha operations.
    Pre-requisite: Please install gdeploy package on the glusto-tests
    management node.
"""

from glusto.core import Glusto as g
import os

GDEPLOY_CONF_DIR = "/usr/share/glustolibs/gdeploy_configs/"


def create_nfs_ganesha_cluster(servers, vips):
    """Creates nfs ganesha cluster using gdeploy

    Args:
        servers (list): Nodes in which nfs-ganesha cluster will be created.
        vips (list): virtual IPs of each servers mentioned in 'servers'
            param.

    Returns:
        bool : True on successfully creating nfs-ganesha cluster.
            False otherwise

    Example:
        create_nfs_ganesha_cluster(servers, vips)
    """

    conf_file = "create_nfs_ganesha_cluster.jinja"
    gdeploy_config_file = GDEPLOY_CONF_DIR + conf_file
    tmp_gdeploy_config_file = ("/tmp/" + os.path.splitext(conf_file)[0] +
                               ".conf")

    values_to_substitute_in_template = {'servers': servers,
                                        'vips': vips}

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
        g.log.error("Failed to execute gdeploy cmd %s for creating nfs "
                    "ganesha cluster" % cmd)
        g.log.error("gdeploy console output for creating nfs-ganesha "
                    "cluster: %s" % stderr)

        return False

    g.log.info("gdeploy output for creating nfs-ganesha cluster: %s"
               % stdout)

    # pcs status output
    _, _, _ = g.run(servers[0], "pcs status")

    # Removing the gdeploy conf file from /tmp
    os.remove(tmp_gdeploy_config_file)
    return True


def teardown_nfs_ganesha_cluster(servers, force=False):
    """Teardown nfs ganesha cluster using gdeploy

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

    conf_file = "teardown_nfs_ganesha_cluster.jinja"
    gdeploy_config_file = GDEPLOY_CONF_DIR + conf_file
    tmp_gdeploy_config_file = ("/tmp/" + os.path.splitext(conf_file)[0] +
                               ".conf")

    values_to_substitute_in_template = {'servers': servers}

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
        g.log.error("Failed to execute gdeploy cmd %s for teardown nfs "
                    "ganesha cluster" % cmd)
        g.log.error("gdeploy console output for teardown nfs-ganesha "
                    "cluster: %s" % stderr)

        return False

    g.log.info("gdeploy output for teardown nfs-ganesha cluster: %s"
               % stdout)

    # Removing gdeploy conf file from /tmp
    os.remove(tmp_gdeploy_config_file)

    if force:
        g.log.info("Executing force cleanup...")
        for server in servers:
            cmd = ("/usr/libexec/ganesha/ganesha-ha.sh --teardown "
                   "/var/run/gluster/shared_storage/nfs-ganesha")
            _, _, _ = g.run(server, cmd)
            _, _, _ = stop_nfs_ganesha_service(server)

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
        node_to_delete (str): Node to delete from existing nfs-ganesha cluster.

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


def enable_root_squash(mnode, volname):
    """
       Enable root squash for the given volume.

    Args:
        mnode (str): Node in which cmd command will
            be executed.
        volname (str): volume name

    Returns:
        bool : True on successfully enabling root squash on
            nfs-ganesha volume. False otherwise

    Example:
        enable_root_squash(mnode, volname)
    """

    config_to_update = "Squash=\"Root_squash\";"
    return update_volume_export_configuration(mnode, volname, config_to_update)


def disable_root_squash(mnode, volname):
    """
       Disable root squash for the given volume.

    Args:
        mnode (str): Node in which cmd command will
            be executed.
        volname (str): volume name

    Returns:
        bool : True on successfully disabling root squash on
            nfs-ganesha volume. False otherwise

    Example:
        disable_root_squash(mnode, volname)
    """

    config_to_update = "Squash=\"No_root_squash\";"
    return update_volume_export_configuration(mnode, volname, config_to_update)


def enable_acl(mnode, volname):
    """
       Enable acl for the given volume.

    Args:
        mnode (str): Node in which cmd command will
            be executed.
        volname (str): volume name

    Returns:
        bool : True on successfully enabling acl on
            nfs-ganesha volume. False otherwise

    Example:
        enable_acl(mnode, volname)
    """

    config_to_update = "Disable_ACL = false;"
    return update_volume_export_configuration(mnode, volname, config_to_update)


def disable_acl(mnode, volname):
    """
       Disable acl for the given volume.

    Args:
        mnode (str): Node in which cmd command will
            be executed.
        volname (str): volume name

    Returns:
        bool : True on successfully disabling acl on
            nfs-ganesha volume. False otherwise

    Example:
        disable_acl(mnode, volname)
    """

    config_to_update = "Disable_ACL = true;"
    return update_volume_export_configuration(mnode, volname, config_to_update)


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

    retcode, stdout, stderr = g.run(mnode, cmd)
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

    retcode, stdout, stderr = g.run(mnode, cmd)
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

    retcode, stdout, stderr = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to parse "
                    "for the hostnames in cluster")
        return False

    host_list = stdout.split("\n")
    host_list = list(filter(None, host_list))

    if ((cluster_list != []) and (cluster_list == host_list)):
        g.log.info("nfs ganesha cluster is in HEALTHY state")
        return True
    else:
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

    retcode, stdout, stderr = g.run(mnode, cmd)
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

    retcode, stdout, stderr = g.run(mnode, cmd)
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

    retcode, stdout, stderr = g.run(mnode, cmd)
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
                g.log.error("failover status: failed node %s is not takenover "
                            "by other node in nfs-ganesha cluster"
                            % (cluster_node))
                ret = False
            else:
                g.log.info("failover status: failed node %s is successfully "
                           "failovered to node %s" % (cluster_node, host_node))
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

    retcode, stdout, stderr = g.run(mnode, cmd)
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

    retcode, stdout, stderr = g.run(mnode, cmd)
    if retcode != 0:
        g.log.error("Failed to execute nfs-ganesha status command to parse "
                    "for the cluster resources")
        return False

    cluster_list = stdout.split("\n")
    cluster_list = list(filter(None, cluster_list))

    if cluster_list != []:
        g.log.info("nfs ganesha cluster exists")
        return True
    else:
        g.log.error("nfs ganesha cluster not exists")
        return False


def set_nfs_ganesha_client_configuration(client_nodes):
    """Sets pre-requisites in the client machines to
       mount with nfs-ganesha.

    Args:
        client_nodes (list): Client nodes in which the prerequisite
            are done to do nfs-ganesha mount.

    Returns:
        bool : True on successfully creating nfs-ganesha cluster.
            False otherwise

    Example:
        set_nfs_ganesha_client_configuration(client_nodes)
    """

    conf_file = "nfs_ganesha_client_configuration.jinja"
    gdeploy_config_file = GDEPLOY_CONF_DIR + conf_file
    tmp_gdeploy_config_file = ("/tmp/" + os.path.splitext(conf_file)[0] +
                               ".conf")

    values_to_substitute_in_template = {'servers': client_nodes}

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
        g.log.error("Failed to execute gdeploy cmd %s for setting nfs "
                    "ganesha client configuration" % cmd)
        g.log.error("gdeploy console output for setting nfs-ganesha "
                    "client configuration: %s" % stderr)

        return False

    g.log.info("gdeploy output for setting nfs-ganesha client "
               "configuration: %s" % stdout)

    # Removing the gdeploy conf file from /tmp
    os.remove(tmp_gdeploy_config_file)
    return True


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
