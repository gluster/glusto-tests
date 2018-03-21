#!/usr/bin/env python
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
    Description: Module for creating ssl machines for
    validating basic ssl cases
"""

from StringIO import StringIO
from glusto.core import Glusto as g


def create_ssl_machine(servers, clients):
    """Following are the steps to create ssl machines:
            - Stop glusterd on all servers
            - Run: openssl genrsa -out /etc/ssl/glusterfs.key 2048
            - Run: openssl req -new -x509 -key /etc/ssl/glusterfs.key
                   -subj "/CN=ip's" -days 365 -out /etc/ssl/glusterfs.pem
            - copy glusterfs.pem files into glusterfs.ca from all
              the nodes(servers+clients) to all the servers
            - touch /var/lib/glusterd/secure-access
            - Start glusterd on all servers
    Args:
        servers: List of servers
        clients: List of clients

    Returns:
        bool : True if successfully created ssl machine. False otherwise.
    """
    # pylint: disable=too-many-statements, too-many-branches
    # pylint: disable=too-many-return-statements
    # Variable to collect all servers ca_file for servers
    ca_file_server = StringIO()

    # Stop glusterd on all servers
    ret = g.run_parallel(servers, "systemctl stop glusterd")
    if not ret:
        g.log.error("Failed to stop glusterd on all servers")
        return False

    # Generate key file on all servers
    cmd = "openssl genrsa -out /etc/ssl/glusterfs.key 2048"
    ret = g.run_parallel(servers, cmd)
    if not ret:
        g.log.error("Failed to create /etc/ssl/glusterfs.key "
                    "file on all servers")
        return False

    # Generate glusterfs.pem file on all servers
    for server in servers:
        _, hostname, _ = g.run(server, "hostname")
        cmd = ("openssl req -new -x509 -key /etc/ssl/glusterfs.key -subj "
               "/CN=%s -days 365 -out /etc/ssl/glusterfs.pem" % (hostname))
        ret = g.run(server, cmd)
        if not ret:
            g.log.error("Failed to create /etc/ssl/glusterfs.pem "
                        "file on server %s", server)
            return False

    # Copy glusterfs.pem file of all servers into ca_file_server
    for server in servers:
        conn1 = g.rpyc_get_connection(server)
        if conn1 == "None":
            g.log.error("Failed to get rpyc connection on %s", server)

        with conn1.builtin.open('/etc/ssl/glusterfs.pem') as fin:
            ca_file_server.write(fin.read())

    # Copy all ca_file_server for clients use
    ca_file_client = ca_file_server.getvalue()

    # Generate key file on all clients
    for client in clients:
        _, hostname, _ = g.run(client, "hostname -s")
        cmd = "openssl genrsa -out /etc/ssl/glusterfs.key 2048"
        ret = g.run(client, cmd)
        if not ret:
            g.log.error("Failed to create /etc/ssl/glusterfs.key "
                        "file on client %s", client)
            return False

        # Generate glusterfs.pem file on all clients
        cmd = ("openssl req -new -x509 -key /etc/ssl/glusterfs.key -subj "
               "/CN=%s -days 365 -out /etc/ssl/glusterfs.pem" % (client))
        ret = g.run(client, cmd)
        if not ret:
            g.log.error("Failed to create /etc/ssl/glusterf.pem "
                        "file on client %s", client)
            return False

        # Copy glusterfs.pem file of client to a ca_file_server
        conn2 = g.rpyc_get_connection(client)
        if conn2 == "None":
            g.log.error("Failed to get rpyc connection on %s", server)
        with conn2.builtin.open('/etc/ssl/glusterfs.pem') as fin:
            ca_file_server.write(fin.read())

        # Copy glusterfs.pem file to glusterfs.ca of client such that
        # clients shouldn't share respectives ca file each other
        cmd = "cp /etc/ssl/glusterfs.pem /etc/ssl/glusterfs.ca"
        ret, _, _ = g.run(client, cmd)
        if ret != 0:
            g.log.error("Failed to copy the glusterfs.pem to "
                        "glusterfs.ca of client")
            return False

        # Now copy the ca_file of all servers to client ca file
        with conn2.builtin.open('/etc/ssl/glusterfs.ca', 'a') as fout:
            fout.write(ca_file_client)

        # Create /var/lib/glusterd directory on clients
        ret = g.run(client, "mkdir -p /var/lib/glusterd/")
        if not ret:
            g.log.error("Failed to create directory /var/lib/glusterd/"
                        " on clients")

    # Copy ca_file_server to all servers
    for server in servers:
        conn3 = g.rpyc_get_connection(server)
        if conn3 == "None":
            g.log.error("Failed to get rpyc connection on %s", server)

        with conn3.builtin.open('/etc/ssl/glusterfs.ca', 'w') as fout:
            fout.write(ca_file_server.getvalue())

    # Touch /var/lib/glusterd/secure-access on all servers
    ret = g.run_parallel(servers, "touch /var/lib/glusterd/secure-access")
    if not ret:
        g.log.error("Failed to touch the file on servers")
        return False

    # Touch /var/lib/glusterd/secure-access on all clients
    ret = g.run_parallel(clients, "touch /var/lib/glusterd/secure-access")
    if not ret:
        g.log.error("Failed to touch the file on clients")
        return False

    # Start glusterd on all servers
    ret = g.run_parallel(servers, "systemctl start glusterd")
    if not ret:
        g.log.error("Failed to stop glusterd on servers")
        return False

    return True


def cleanup_ssl_setup(servers, clients):
    """
    Following are the steps to cleanup ssl setup:
            - Stop glusterd on all servers
            - Remove folder /etc/ssl/*
            - Remove /var/lib/glusterd/*
            - Start glusterd on all servers

    Args:
        servers: List of servers
        clients: List of clients

    Returns:
        bool : True if successfully cleaned ssl machine. False otherwise.
    """
    # pylint: disable=too-many-return-statements
    _rc = True

    # Stop glusterd on all servers
    ret = g.run_parallel(servers, "systemctl stop glusterd")
    if not ret:
        _rc = False
        g.log.error("Failed to stop glusterd on all servers")

    # Remove glusterfs.key, glusterfs.pem and glusterfs.ca file
    # from all servers
    cmd = "rm -rf /etc/ssl/glusterfs*"
    ret = g.run_parallel(servers, cmd)
    if not ret:
        _rc = False
        g.log.error("Failed to remove folder /etc/ssl/glusterfs* "
                    "on all servers")

    # Remove folder /var/lib/glusterd/secure-access from servers
    cmd = "rm -rf /var/lib/glusterd/secure-access"
    ret = g.run_parallel(servers, cmd)
    if not ret:
        _rc = False
        g.log.error("Failed to remove folder /var/lib/glusterd/secure-access "
                    "on all servers")

    # Remove glusterfs.key, glusterfs.pem and glusterfs.ca file
    # from all clients
    cmd = "rm -rf /etc/ssl/glusterfs*"
    ret = g.run_parallel(clients, cmd)
    if not ret:
        _rc = False
        g.log.error("Failed to remove folder /etc/ssl/glusterfs* "
                    "on all clients")

    # Remove folder /var/lib/glusterd/secure-access from clients
    cmd = "rm -rf /var/lib/glusterd/secure-access"
    ret = g.run_parallel(clients, cmd)
    if not ret:
        _rc = False
        g.log.error("Failed to remove folder /var/lib/glusterd/secure-access "
                    "on all clients")

    # Start glusterd on all servers
    ret = g.run_parallel(servers, "systemctl start glusterd")
    if not ret:
        _rc = False
        g.log.error("Failed to stop glusterd on servers")

    return _rc
