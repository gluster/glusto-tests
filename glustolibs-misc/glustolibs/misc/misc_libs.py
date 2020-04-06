#  Copyright (C) 2015-2020  Red Hat, Inc. <http://www.redhat.com>
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

""" Description: Helper module for misc libs. """

import os
import sys
import time

from glusto.core import Glusto as g
from glustolibs.gluster.lib_utils import is_rhel6


def create_dirs(list_of_nodes, list_of_dir_paths):
    """Create directories on nodes.

    Args:
        list_of_nodes (list): Nodes on which dirs has to be created.
        list_of_dir_paths (list): List of dirs abs path.

    Returns:
        bool: True of creation of all dirs on all nodes is successful.
            False otherwise.
    """
    if not isinstance(list_of_nodes, list):
        list_of_nodes = [list_of_nodes]

    if isinstance(list_of_dir_paths, list):
        list_of_dir_paths = ' '.join(list_of_dir_paths)

    _rc = True
    # Create upload dir on each node
    for node in list_of_nodes:
        ret, _, err = g.run(node, "mkdir -p %s" % list_of_dir_paths)
        if ret != 0:
            g.log.error("Failed to create the dirs: %s on node: %s - %s" %
                        (list_of_dir_paths.split(" "), node, err))
            _rc = False
    if _rc:
        g.log.info("Successfully created dirs: %s on nodes:%s" %
                   (list_of_dir_paths.split(" "), list_of_nodes))
    return _rc


def path_exists(list_of_nodes, list_of_paths):
    """Check if paths exist on nodes.

    Args:
        list_of_nodes (list): List of nodes.
        list_of_paths (list): List of abs paths to verify if path exist.

    Returns:
        bool: True if all paths exists on all nodes. False otherwise.
    """
    if not isinstance(list_of_nodes, list):
        list_of_nodes = [list_of_nodes]

    if not isinstance(list_of_paths, list):
        list_of_paths = (list_of_paths.split(" "))

    _rc = True
    for node in list_of_nodes:
        for path in list_of_paths:
            cmd = "ls -l %s" % path
            ret, _, err = g.run(node, cmd)
            if ret != 0:
                g.log.error("Path: %s not found on node: %s - %s" %
                            (path, node, err))
                _rc = False

    if _rc:
        g.log.info("Paths: %s exists on nodes: %s" %
                   (list_of_paths, list_of_nodes))
    return _rc


def upload_scripts(list_of_nodes, list_of_scripts_abs_path,
                   upload_dir="/usr/share/glustolibs/io/scripts/", user=None):
    """Upload specified scripts to all the nodes.

    Args:
        list_of_nodes (list): Nodes on which scripts have to be uploaded.
        list_of_scripts_abs_path (list): List of absolute path of all
            scripts that are to be uploaded from local node.
        upload_dir (optional[str]): Name of the dir under which
            scripts will be uploaded on remote node.
        user (optional[str]): The user to use for the remote connection.

    Returns:
        bool: True if uploading scripts is successful on all nodes.
            False otherwise.
    """
    if not isinstance(list_of_nodes, list):
        list_of_nodes = [list_of_nodes]

    if not isinstance(list_of_scripts_abs_path, list):
        list_of_scripts_abs_path = (
            list_of_scripts_abs_path.split(" "))

    g.log.info("Scripts to upload: %s" % list_of_scripts_abs_path)
    g.log.info("Script upload dir: %s" % upload_dir)

    # Create upload dir on each node
    if not create_dirs(list_of_nodes, upload_dir):
        return False

    # Upload scrpts
    _rc = True
    for script_local_abs_path in list_of_scripts_abs_path:
        if not os.path.exists(script_local_abs_path):
            g.log.error("Script: %s doesn't exists" % script_local_abs_path)
            _rc = False
            break
        for node in list_of_nodes:
            script_name = os.path.basename(script_local_abs_path)
            script_upload_path = os.path.join(upload_dir, script_name)
            g.upload(node, script_local_abs_path, script_upload_path, user)
    if not _rc:
        g.log.error("Failed to upload scripts")
        return False

    # Recursively provide execute permissions to all scripts
    for node in list_of_nodes:
        ret, _, _ = g.run(node, "chmod -R +x %s" % upload_dir)
        if ret != 0:
            g.log.error("Unable to provide execute permissions to upload dir "
                        "'%s' on %s" % (upload_dir, node))
            return False
        else:
            g.log.info("Successfully provided execute permissions to upload "
                       "dir '%s' on %s" % (upload_dir, node))

        ret, out, err = g.run(node, "ls -l %s" % upload_dir)
        if ret != 0:
            g.log.error("Failed to list the dir: %s on node: %s - %s" %
                        (upload_dir, node, err))
        else:
            g.log.info("Listing dir: %s on node: %s - \n%s" %
                       (upload_dir, node, out))

    return True


def yum_add_repos(list_of_nodes, list_of_yum_repos):
    """Add yum repo files on all the nodes.

    Args:
        list_of_nodes (list): Nodes on which yum repo files have to be added.
        list_of_yum_repos (list): List of yum repos

    Returns:
        bool: True if adding yum repo files is successful on all nodes.
            False otherwise.
    """
    if not isinstance(list_of_nodes, list):
        list_of_nodes = [list_of_nodes]

    if not isinstance(list_of_yum_repos, list):
        list_of_yum_repos = list_of_yum_repos.split(" ")

    _rc = True
    for node in list_of_nodes:
        for yum_repo in list_of_yum_repos:
            out_file = os.path.basename(yum_repo)
            cmd = ("wget %s -O /etc/yum.repos.d/%s" % (yum_repo, out_file))
            ret, _, err = g.run(node, cmd)
            if ret != 0:
                g.log.error("Unable to add repo file: %s on node: %s - %s" %
                            (yum_repo, node, err))
                _rc = False
    if _rc:
        g.log.info("Successfully added yum repo files: %s on nodes: %s" %
                   (list_of_yum_repos, list_of_nodes))
    return _rc


def yum_install_packages(list_of_nodes, yum_packages):
    """Install the specified yum packages on all nodes.

    Args:
        list_of_nodes (list): Nodes on which yum packages have to be installed.
        yum_packages (list): List of yum packages.

    Returns:
        bool: True if installation of packages is successful on all nodes.
            False otherwise.
    """
    if not isinstance(list_of_nodes, list):
        list_of_nodes = [list_of_nodes]

    if isinstance(yum_packages, list):
        yum_packages = ' '.join(yum_packages)

    _rc = True
    for node in list_of_nodes:
        cmd = "yum -y install %s" % yum_packages
        ret, _, err = g.run(node, cmd)
        if ret != 0:
            g.log.error("Unable to install yum packages: %s on node: %s - %s" %
                        (yum_packages, node, err))
            _rc = False
    if _rc:
        g.log.info("Successfully installed yum packages: %s on nodes: %s" %
                   (yum_packages, list_of_nodes))
        return _rc


def yum_remove_packages(list_of_nodes, yum_packages):
    """Remove the specified yum packages on all nodes.

    Args:
        list_of_nodes (list): Nodes on which yum packages have to be removed.
        yum_packages (list): List of yum packages.

    Returns:
        bool: True if removing packages is successful on all nodes.
            False otherwise.
    """
    if not isinstance(list_of_nodes, list):
        list_of_nodes = [list_of_nodes]

    if isinstance(yum_packages, list):
        yum_packages = ' '.join(yum_packages)

    _rc = True
    for node in list_of_nodes:
        cmd = "yum -y remove %s" % yum_packages
        ret, _, err = g.run(node, cmd)
        if ret != 0:
            g.log.error("Unable to remove yum packages: %s on node: %s - %s" %
                        (yum_packages, node, err))
            _rc = False
    if _rc:
        g.log.info("Successfully removed yum packages: %s on nodes: %s" %
                   (yum_packages, list_of_nodes))
    return _rc


def pip_install_packages(list_of_nodes, python_packages):
    """Install the specified python packages on all the specified nodes.

    Args:
        list_of_nodes (list): Nodes on which python packages have to be
            installed.
        python_packages (list): List of python packages.

    Returns:
        bool: True if installation of packages is successful on all nodes.
            False otherwise.
    """
    if not isinstance(list_of_nodes, list):
        list_of_nodes = [list_of_nodes]

    if isinstance(python_packages, list):
        python_packages = ' '.join(python_packages)

    _rc = True
    for node in list_of_nodes:
        cmd = "pip install %s" % python_packages
        ret, _, err = g.run(node, cmd)
        if ret != 0:
            g.log.error("Unable to install python packages: %s on node: %s - "
                        "%s" % (python_packages, node, err))
            _rc = False
    if _rc:
        g.log.info("Successfully installed python packages: %s on nodes: %s" %
                   (python_packages, list_of_nodes))
    return _rc


def install_testing_tools(list_of_nodes, testing_tools):
    """Install the specified testing tools on all nodes.

    Args:
        list_of_nodes (list): Nodes on which testing tools has to be
            installed.
        testing_tools(list): List of testing tools to install.
            Available tools:
                - arequal

    Returns:
        bool: True if installation of all testing tools is successful on
            all nodes. False otherwise.
    """
    if not isinstance(list_of_nodes, list):
        list_of_nodes = [list_of_nodes]

    if not isinstance(testing_tools, list):
        testing_tools = testing_tools.split(" ")

    _rc = True
    this_module = sys.modules[__name__]
    for testing_tool in testing_tools:
        func_name = 'install_' + testing_tool
        if hasattr(this_module, func_name):
            func = getattr(this_module, func_name)
            ret = func(list_of_nodes)
            if not ret:
                _rc = False
            else:
                g.log.info("Successfully installed tool: %s on nodes: [%s]" %
                           (testing_tool, ', '.join(list_of_nodes)))
        else:
            g.log.error("Unable to find the helper function to install %s" %
                        testing_tool)
            _rc = False
    return _rc


def install_arequal(list_of_nodes):
    """Install arequal on nodes in /usr/bin/.

    Args:
        nodes(list): List of nodes on which arequal-checksum needs to be
            installed.

    Returns:
        bool: Returns True on successful installation of arequal-checksum
            on all nodes. False Otherwise.

    Note: The arequal repo can be specified in the config file:
        dependencies:
            testing_tools:
                arequal:
                    repo: "https://abc.def.com/tools/arequal/arequal.repo"
    """
    if not isinstance(list_of_nodes, list):
        list_of_nodes = [list_of_nodes]

    try:
        arequal_repo = (g.config['dependencies']['testing_tools']['arequal']
                        ['repo'])
    except KeyError:
        arequal_repo = ("https://copr.fedorainfracloud.org/coprs/nigelbabu/"
                        "arequal/repo/epel-7/nigelbabu-arequal-epel-7.repo")

    arequal_install_path = "/usr/bin/arequal-checksum"
    _rc = True
    for node in list_of_nodes:
        # check if arequal-checksum is installed
        if not path_exists(node, arequal_install_path):
            g.log.info("arequal-checksum not installed. Installling...")

            # get arequal repo file
            if not yum_add_repos(node, arequal_repo):
                return False

            # Install arequal
            if not yum_install_packages(node, "arequal"):
                return False

            # verify arequal_checksum got installed
            if not path_exists(node, arequal_install_path):
                raise Exception("%s not found on node %s even after "
                                "installation" % (arequal_install_path, node))
            else:
                g.log.info("arequal_checksum is installed on node %s" %
                           node)
        else:
            g.log.info("arequal-checksum is already installed on %s" % node)
            continue
    if _rc:
        g.log.info("arequal-checksum is installed on nodes: %s" %
                   list_of_nodes)
    return _rc


def are_nodes_online(nodes):
    """Check whether nodes are online or not.

    Args:
        nodes (str|list): Node(s) to check whether online or not.

    Returns:
        tuple : Tuple containing two elements (ret, node_results).
        The first element ret is of type 'bool', True if all nodes
        are online. False otherwise.

        The second element 'node_results' is of type dictonary and it
        contains the node and its corresponding result. If node is online
        then the result contains True else False.
    """

    if not isinstance(nodes, list):
        nodes = [nodes]

    node_results = {}
    for node in nodes:
        cmd = "ping %s -c1" % node
        ret, out, err = g.run_local(cmd)
        if ret:
            g.log.info("%s is offline" % node)
            node_results[node] = False
        else:
            g.log.info("%s is online" % node)
            node_results[node] = True

    ret = all(node_results.values())

    return ret, node_results


def reboot_nodes(nodes):
    """Reboot the nodes and check whether nodes are offline or not.

    Args:
        nodes (str|list): Node(s) to reboot.

    Returns:
        bool: True if all nodes come offline after reboot. False otherwise.
    """
    if not isinstance(nodes, list):
        nodes = [nodes]

    cmd = "reboot"
    _rc = False
    for node in nodes:
        g.log.info("Executing cmd: %s on node %s", cmd, node)
        g.log.info("Rebooting the node %s", node)
        ret = g.run(node, cmd)

    halt = 120
    counter = 0

    g.log.info("Wait for some seconds for the nodes to go offline")
    while counter < halt:
        ret, reboot_time = are_nodes_offline(nodes)
        if not ret:
            g.log.info("Nodes are online, Retry after 2 seconds .....")
            time.sleep(2)
            counter = counter + 2
        else:
            _rc = True
            g.log.info("All nodes %s are offline", nodes)
            break
    if not _rc:
        g.log.error("Some nodes %s are online", nodes)

    return _rc


def reboot_nodes_and_wait_to_come_online(nodes, timeout=600):
    """Reboot node(s) and wait for it to become online.

    Args:
        nodes (str|list): Node(s) to reboot.

    Kwargs:
        timeout (int): timeout value in seconds to wait for node
                       to come online

    Returns:
        tuple : Tuple containing two elements (_rc, reboot_results).
        The first element '_rc' is of type 'bool', True if all nodes
        comes online after reboot. False otherwise.

        The second element 'reboot_results' is of type dictonary and it
        contains the node and corresponding result for reboot. If reboot is
        successful on node, then result contains True else False.
    """
    _rc = reboot_nodes(nodes)
    reboot_results = {}
    if not _rc:
        return _rc, reboot_results

    counter = 0
    g.log.info("Wait for some seconds for the nodes to come online"
               " after reboot")

    _rc = False
    while counter < timeout:
        ret, reboot_results = are_nodes_online(nodes)
        if not ret:
            g.log.info("Nodes are offline, Retry after 5 seconds ..... ")
            time.sleep(5)
            counter = counter + 5
        else:
            _rc = True
            break

    if not _rc:
        for node in reboot_results:
            if reboot_results[node]:
                g.log.info("Node %s is online", node)
            else:
                g.log.error("Node %s is offline even after "
                            "%d minutes", node, timeout / 60.0)
    else:
        g.log.info("All nodes %s are up and running", nodes)

    return _rc, reboot_results


def are_nodes_offline(nodes):
    """Check whether nodes are offline or not.

    Args:
        nodes (str|list): Node(s) to check whether offline or not.

    Returns:
        tuple : Tuple containing two elements (ret, node_results).
        The first element ret is of type 'bool', True if all nodes
        are offline. False otherwise.

        The second element 'node_results' is of type dictonary and it
        contains the node and its corresponding result. If node is offline
        then the result contains True else False.
    """

    if not isinstance(nodes, list):
        nodes = [nodes]

    node_results = {}
    for node in nodes:
        cmd = "ping %s -c1" % node
        ret, out, err = g.run_local(cmd)
        if ret:
            g.log.info("%s is offline" % node)
            node_results[node] = True
        else:
            g.log.info("%s is online" % node)
            node_results[node] = False

    ret = all(node_results.values())

    return ret, node_results


def drop_caches(hosts):
    """Drop Kernel Cache on a list of hosts
       (in order to run reads/renames etc on a cold cache).

    Args:
        hosts (list): List of  hosts where kernel caches need to be
                      dropped (Servers/ Clients)

    Returns:
        bool: True , post successful completion.Else,False.
    """
    cmd = "echo 3 > /proc/sys/vm/drop_caches"
    results = g.run_parallel(hosts, cmd)
    _rc = True
    for host, ret_values in results.items():
        retcode, _, _ = ret_values
        if retcode != 0:
            g.log.error("Unable to drop cache on host %s", host)
            _rc = False

    return _rc


def daemon_reload(node):
    """Reload the Daemons when unit files are changed.

    Args:
        node (str): Node on which daemon has to be reloaded.

    Returns:
        bool: True, On successful daemon reload
               False, Otherwise
    """
    if is_rhel6([node]):
        cmd = 'service glusterd reload'
        ret, _, _ = g.run(node, cmd)
        if ret != 0:
            g.log.error("Failed to reload the daemon")
            return False
    else:
        cmd = "systemctl daemon-reload"
        ret, _, _ = g.run(node, cmd)
        if ret != 0:
            g.log.error("Failed to reload the daemon")
            return False
    return True


def git_clone_and_compile(hosts, link, dir_name, compile_option='False'):
    """Clone and compile a repo.

    Args:
        hosts (list|str): List of hosts where the repo needs to be cloned.
        link (str): Link to the repo that needs to be cloned.
        dir_name (str): Directory where the repo is to be cloned.

    Kwargs:
        compile_option (bool): If this option is set to True
            then the cloned repo will be compiled otherwise
            the repo is only cloned on the hosts.

    Returns:
       bool : True on successfull cloning (and compilation)
            False otherwise.
    """
    if not isinstance(hosts, list):
        hosts = [hosts]

    cmd = "cd /root; git clone %s %s;" % (link, dir_name)
    if compile_option:
        cmd = cmd + ("cd /root/%s; make" % dir_name)

    for host in hosts:
        ret, _, _ = g.run(host, cmd)
        if ret:
            g.log.error("Cloning/Compiling repo failed on %s" % host)
            return False
        else:
            g.log.info("Successfully cloned/compiled repo on %s" % host)
    return True
