#  Copyright (C) 2015-2016  Red Hat, Inc. <http://www.redhat.com>
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

from glusto.core import Glusto as g
import os
import sys


def create_dirs(list_of_nodes, list_of_dir_paths):
    """Creates directories on nodes.

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
    """check if paths exists on nodes.

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
    """Uploads specified scripts to all the nodes.

    Args:
        list_of_nodes (list): Nodes on which scripts has to be uploaded.
        list_of_scripts_abs_path (list): List of absolute path of all
            scripts that are to be uploaded from local node.
        upload_dir (optional[str]): Name of the dir under which
            scripts will be uploaded on remote node.
        user (optional[str]): The user to use for the remote connection.

    Returns:
        bool: True if uploading scripts is sucessful on all nodes.
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
        list_of_nodes (list): Nodes on which yum repo files has to be added.
        list_of_yum_repos (list): List of yum repos

    Returns:
        bool: True if adding yum repo files is sucessful on all nodes.
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
        list_of_nodes (list): Nodes on which yum packages has to be installed.
        yum_packages (list): List of yum packages.

    Returns:
        bool: True if installation of packages is sucessful on all nodes.
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
        list_of_nodes (list): Nodes on which yum packages has to be removed.
        yum_packages (list): List of yum packages.

    Returns:
        bool: True if removing packages is sucessful on all nodes.
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
        list_of_nodes (list): Nodes on which python packages has to be
            installed.
        python_packages (list): List of python packages.

    Returns:
        bool: True if installation of packages is sucessful on all nodes.
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
        bool: True if installation of all testing tools is sucessful on
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
    """Installs arequal on nodes in /usr/bin/.

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
