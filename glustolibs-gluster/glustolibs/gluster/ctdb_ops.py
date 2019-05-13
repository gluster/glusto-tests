#!/usr/bin/env python
#  Copyright (C) 2020 Red Hat, Inc. <http://www.redeat.com>
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
CTDB library operations
pre-requisite : CTDB and Samba packages
needs to be installed on all the server nodes.
"""

import re
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.lib_utils import (add_services_to_firewall,
                                          is_rhel6, list_files)
from glustolibs.gluster.mount_ops import umount_volume
from glustolibs.gluster.volume_libs import cleanup_volume


def edit_hook_script(mnode, ctdb_volname):
    """
    Edit the hook scripts with ctdb volume name

    Args:
        mnode (str): Node on which commands has to be executed.
        ctdb_volname (str): Name of the ctdb volume
   Returns:
        bool: True if successfully edits the hook-scripts else false
    """
    # Replace META='all' to META=ctdb_volname setup hook script
    cmd = ("sed -i -- 's/META=\"all\"/META=\"%s\"/g' "
           "/var/lib/glusterd/hooks/1"
           "/start/post/S29CTDBsetup.sh")
    ret, _, _ = g.run(mnode, cmd % ctdb_volname)
    if ret:
        g.log.error("Hook script - S29CTDBsetup edit failed on %s", mnode)
        return False

    g.log.info("Hook script - S29CTDBsetup edit success on %s", mnode)
    # Replace META='all' to META=ctdb_volname teardown hook script
    cmd = ("sed -i -- 's/META=\"all\"/META=\"%s\"/g' "
           "/var/lib/glusterd/hooks/1"
           "/stop/pre/S29CTDB-teardown.sh")

    ret, _, _ = g.run(mnode, cmd % ctdb_volname)
    if ret:
        g.log.error("Hook script - S29CTDB-teardown edit failed on %s", mnode)
        return False
    g.log.info("Hook script - S29CTDBteardown edit success on %s", mnode)
    return True


def enable_ctdb_cluster(mnode):
    """
    Edit the smb.conf to add clustering = yes

    Args:
        mnode (str): Node on which commands has to be executed.

    Returns:
        bool: True if successfully enable ctdb cluster else false
    """
    # Add clustering = yes in smb.conf if not already there
    cmd = (r"grep -q 'clustering = yes' "
           r"/etc/samba/smb.conf || sed -i.bak '/\[global\]/a "
           r"clustering = yes' /etc/samba/smb.conf")
    ret, _, _ = g.run(mnode, cmd)
    if ret:
        g.log.error("Failed to add cluster = yes to smb.conf in  %s", mnode)
        return False
    g.log.info("Successfully added 'clustering = yes' to smb.conf "
               "in all nodes")
    return True


def check_file_availability(mnode, file_path, filename):
    """
    Check for ctdb files and delete

    Args:
        mnode(str): Node on which command is executed
        filepath(str): Absolute path of the file to be validated
        filename(str): File to be deleted if available in /etc/ctdb/

    Returns:
        bool: True if concerned files are available else false
    """
    if file_path in list_files(mnode, "/etc/ctdb/", filename):
        ret, _, _ = g.run(mnode, "rm -rf %s" % file_path)
        if ret:
            return False
    return True


def create_nodes_file(mnode, node_ips):
    """
    Create nodes file and add node ips

    Args:
        mnode (str): Node on which commands has to be executed.

    Returns:
        bool: True if successfully create nodes file else false
    """
    # check if nodes file is available and delete
    node_file_path = "/etc/ctdb/nodes"
    ret = check_file_availability(mnode, node_file_path, "nodes")
    if not ret:
        g.log.info("Failed to delete pre-existing nodes file in %s", mnode)
        return False
    g.log.info("Deleted pre-existing nodes file in %s", mnode)
    for node_ip in node_ips:
        ret, _, _ = g.run(mnode, "echo -e %s "
                          ">> %s" % (node_ip, node_file_path))
        if ret:
            g.log.error("Failed to add nodes list  in  %s", mnode)
            return False
    g.log.info("Nodes list added succssfully to %s"
               "file in all servers", node_file_path)
    return True


def create_public_address_file(mnode, vips):
    """
    Create public_addresses file and add vips

    Args:
        mnode (str): Node on which commands has to be executed.
        vips (list): List of virtual ips

    Returns:
        bool: True if successfully creates public_address file else false
    """
    publicip_file_path = "/etc/ctdb/public_addresses"
    ret = check_file_availability(mnode,
                                  publicip_file_path,
                                  "public_addresses")
    if not ret:
        g.log.info("Failed to delete pre-existing public_addresses"
                   "file in %s", mnode)
        return False
    g.log.info("Deleted pre-existing public_addresses"
               "file in %s", mnode)
    for vip in vips:
        ret, _, _ = g.run(mnode, "echo -e %s >>"
                          " %s" % (vip, publicip_file_path))
        if ret:
            g.log.error("Failed to add vip list  in  %s", mnode)
            return False
    g.log.info("vip list added succssfully to %s"
               "file in all node", publicip_file_path)
    return True


def ctdb_service_status(servers, mnode):
    """
    Status of ctdb service on the specified node.

    Args:
        mnode (str): Node on which ctdb status needs to be checked

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    g.log.info("Getting ctdb service status on %s", mnode)
    if is_rhel6(servers):
        return g.run(mnode, "service ctdb status")
    return g.run(mnode, "systemctl status ctdb")


def is_ctdb_service_running(servers, mnode):
    """
    Check if ctdb service is running on node

    Args:
        servers (str|list): list|str of cluster nodes
        mnode (str): Node on which ctdb service has to be checked

    Returns:
        bool: True if ctdb service running else False
    """
    g.log.info("Check if ctdb service is running on %s", mnode)
    ret, out, _ = ctdb_service_status(servers, mnode)
    if ret:
        g.log.error("Execution error service ctdb status "
                    "on %s", mnode)
        return False
    if "Active: active (running)" in out:
        g.log.info("ctdb service is running on %s", mnode)
        return True
    else:
        g.log.error("ctdb service is not "
                    "running on %s", mnode)
        return False


def start_ctdb_service(servers):
    """
    start ctdb services on all nodes &
    wait for 40 seconds

    Args:
        servers (list): IP of samba nodes

   Returns:
        bool: True if successfully starts ctdb service  else false
    """
    cmd = "pgrep ctdb || service ctdb start"
    for mnode in servers:
        ret, out, _ = g.run(mnode, cmd)
        if ret:
            g.log.error("Unable to start ctdb on server %s", str(out))
            return False
        if not is_ctdb_service_running(servers, mnode):
            g.log.error("ctdb services not running %s", str(out))
            return False
        g.log.info("Start ctdb on server %s successful", mnode)
    # sleep for 40sec as ctdb status takes time to enable
    sleep(40)
    return True


def stop_ctdb_service(servers):
    """
    stop ctdb services on all nodes

    Args:
        servers (list): IP of samba nodes

   Returns:
        bool: True if successfully stops ctdb service else false
    """
    cmd = "service ctdb stop"
    for mnode in servers:
        ret, out, _ = g.run(mnode, cmd)
        if ret:
            g.log.error("Unable to stop ctdb on server %s", str(out))
            return False
        if is_ctdb_service_running(servers, mnode):
            g.log.error("ctdb services still running %s", str(out))
            return False
        g.log.info("Stop ctdb on server %s successful", mnode)
    return True


def ctdb_server_firewall_settings(servers):
    """
    Do firewall settings for ctdb

    Args:
        servers(list): IP of sambe nodes

    Returns:
        bool: True if successfully added firewall services else false
    """
    # List of services to enable
    services = ['samba', 'rpc-bind']
    ret = add_services_to_firewall(servers, services, True)
    if not ret:
        g.log.error("Failed to set firewall zone "
                    "permanently on ctdb nodes")
        return False

    # Add ctdb and samba port
    if not is_rhel6(servers):
        for mnode in servers:
            ret, _, _ = g.run(mnode, "firewall-cmd --add-port=4379/tcp "
                              "--add-port=139/tcp")
            if ret:
                g.log.error("Failed to add firewall port in %s", mnode)
                return False
            g.log.info("samba ctdb port added successfully in %s", mnode)
            ret, _, _ = g.run(mnode, "firewall-cmd --add-port=4379/tcp "
                              "--add-port=139/tcp --permanent")
            if ret:
                g.log.error("Failed to add firewall port permanently in %s",
                            mnode)
                return False
    return True


def parse_ctdb_status(status):
    """
    Parse the ctdb status output

    Number of nodes:4
    pnn:0 <ip> OK (THIS NODE)
    pnn:1 <ip> OK
    pnn:2 <ip> OK
    pnn:3 <ip> UHEALTHY
    Generation:763624485
    Size:4
    hash:0 lmaster:0
    hash:1 lmaster:1
    hash:2 lmaster:2
    hash:3 lmaster:3
    Recovery mode:NORMAL (0)
    Recovery master:3

    Args:
        status: output of ctdb status(string)

    Returns:
        dict: {<ip>: status}
    """
    cmd = r'pnn\:\d+\s*(\S+)\s*(\S+)'
    ip_nodes = re.findall(cmd, status, re.S)
    if ip_nodes:
        # Empty dictionary to capture ctdb status output
        node_status = {}
        for item in ip_nodes:
            node_status[item[0]] = item[1]
        g.log.info("ctdb node status %s", node_status)
        return node_status
    else:
        return {}


def ctdb_status(mnode):
    """
    Execute ctdb status

    Args:
        mnode(str): primary node out of the servers

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    cmd = "ctdb status"
    return g.run(mnode, cmd)


def is_ctdb_status_healthy(mnode):
    """
    Check if ctdb is up & running

    Args:
        mnode(str): primary node out of the servers

    Returns:
        bool: True if ctdb status healthy  else false
    """
    # Get the ctdb status details
    status_res = ctdb_status(mnode)
    if status_res[0]:
        g.log.info("CTDB is not enabled for the cluster")
        return False
    # Get the ctdb status output
    output = status_res[1]
    # Parse the ctdb status output
    node_status = parse_ctdb_status(output)
    if not node_status:
        g.log.error("ctdb status return empty list")
        return False
    for node_ip, status in node_status.iteritems():
        # Check if ctdb status is OK or not
        if node_status[node_ip] != 'OK':
            g.log.error("CTDB node %s is %s",
                        str(node_ip), status)
            return False
        g.log.info("CTDB node %s is %s",
                   str(node_ip), status)
    return True


def edit_hookscript_for_teardown(mnode, ctdb_volname):
    """
    Edit the hook scripts with ctdb volume name

    Args:
        mnode (str): Node on which commands has to be executed.
        ctdb_volname (str): Name of ctdb volume
   Returns:
        bool: True if successfully edits hook-scripts else false
    """
    # Replace META='ctdb_vol' to META=all setup hook script
    cmd = ("sed -i -- 's/META=\"%s\"/META=\"all\"/g' "
           "/var/lib/glusterd/hooks/1"
           "/start/post/S29CTDBsetup.sh" % ctdb_volname)
    ret, _, _ = g.run(mnode, cmd)
    if ret:
        g.log.error("Hook script - S29CTDBsetup edit failed on %s", mnode)
        return False

    g.log.info("Hook script - S29CTDBsetup edit success on %s", mnode)
    # Replace META='all' to META=ctdb_volname teardown hook script
    cmd = ("sed -i -- 's/META=\"%s\"/META=\"all\"/g' "
           "/var/lib/glusterd/hooks/1"
           "/stop/pre/S29CTDB-teardown.sh" % ctdb_volname)
    ret, _, _ = g.run(mnode, cmd)
    if ret:
        g.log.error("Hook script - S29CTDB-teardown edit failed on %s", mnode)
        return False
    g.log.info("Hook script - S29CTDBteardown edit success on %s", mnode)
    return True


def teardown_samba_ctdb_cluster(servers, ctdb_volname):
    """
    Tear down samba ctdb setup

    Args:
        servers (list): Nodes in ctdb cluster to teardown entire
            cluster
        ctdb_volname (str): Name of ctdb volume

    Returns:
        bool: True if successfully tear downs ctdb cluster else false
    """

    node_file_path = "/etc/ctdb/nodes"
    publicip_file_path = "/etc/ctdb/public_addresses"
    g.log.info("Executing force cleanup...")
    # Stop ctdb service
    if stop_ctdb_service(servers):
        for mnode in servers:
            # check if nodes file is available and delete
            ret = check_file_availability(mnode, node_file_path, "nodes")
            if not ret:
                g.log.info("Failed to delete existing "
                           "nodes file in %s", mnode)
                return False
            g.log.info("Deleted existing nodes file in %s", mnode)

            # check if public_addresses file is available and delete
            ret = check_file_availability(mnode, publicip_file_path,
                                          "public_addresses")
            if not ret:
                g.log.info("Failed to delete existing public_addresses"
                           " file in %s", mnode)
                return False
            g.log.info("Deleted existing public_addresses"
                       "file in %s", mnode)

            ctdb_mount = '/gluster/lock'
            ret, _, _ = umount_volume(mnode, ctdb_mount, 'glusterfs')
            if ret:
                g.log.error("Unable to unmount lock volume in %s", mnode)
                return False
            if not edit_hookscript_for_teardown(mnode, ctdb_volname):
                return False
        mnode = servers[0]
        ret = cleanup_volume(mnode, ctdb_volname)
        if not ret:
            g.log.error("Failed to delete ctdb volume - %s", ctdb_volname)
            return False
        return True
    return False
