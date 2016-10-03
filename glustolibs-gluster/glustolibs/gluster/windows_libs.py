#!/usr/bin/env python
#  Copyright (C) 2016 Red Hat, Inc. <http://www.redhat.com>
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
#

"""
    Description: Module for windows utility functions
"""
from glusto.core import Glusto as g


def powershell(command):
    """wrap a command in powershell call

    Args:
        command (str): the command to wrap with powershell syntax

    Returns:
        string with complete powershell command
    """
    ps_command = ("powershell -InputFormat Text -OutputFormat Text "
                  "-Command '& {%s}'" % command)

    return ps_command


def delete_all_windows_mounts(clients_info):
    """Deletes all the mounts on the windows clients.

    Args:
        clients_info (list): List of windows clients info.

        If any item in the clients info doesn't have the 'platform', it is
        assumed that it is not windows client and would be ignored.

        If any item in the clients info doesn't have the 'super_user' key,
        by default we assume the 'super_user' for windows client to be 'Admin'.

        For all the windows clients, the 'platform' key should be specified
        with value 'windows'.

        Example:
            clients_info = {
                'def.lab.eng.xyz.com': {
                    'host': 'def.lab.eng.xyz.com',
                    'super_user': 'Admin',
                    'platform': 'windows'
                },

                'ghi.lab.eng.blr.redhat.com': {
                    'host': 'ghi.lab.eng.xyz.com',
                }
            }

    Returns:
        bool : True if deleting all the mounts on all clients is successful.
            False otherwise.
    """
    rc = True
    cmd = powershell("net use * /D /Y")
    windows_clients_info = {}
    for client in clients_info:
        if ('platform' in clients_info[client] and
                clients_info[client]['platform'] == 'windows'):
            windows_clients_info[client] = clients_info[client]

    for client in windows_clients_info:
        if 'host' in windows_clients_info[client]:
            host = windows_clients_info[client]['host']
        else:
            host = client
        if 'super_user' in windows_clients_info[client]:
            user = windows_clients_info[client]['super_user']
        else:
            user = 'Admin'
        ret, out, err = g.run(host, cmd, user)
        if ret != 0:
            rc = False

        elif ret == 0:
            if not (('deleted successfully' in out) or
                    ('command completed successfully' in out) or
                    ('There are no entries in the list' in out)):
                rc = False
    return rc


def list_all_windows_mounts(clients_info):
    """Lists all the mounts on the windows clients.

    Args:
        clients_info (list): List of windows clients info.

        If any item in the clients info doesn't have the 'platform', it is
        assumed that it is not windows client and would be ignored.

        If any item in the clients info doesn't have the 'super_user' key,
        by default we assume the 'super_user' for windows client to be 'Admin'.

        For all the windows clients, the 'platform' key should be specified
        with value 'windows'.

        Example:
            clients_info = {
                'def.lab.eng.xyz.com': {
                    'host': 'def.lab.eng.xyz.com',
                    'super_user': 'Admin',
                    'platform': 'windows'
                },

                'ghi.lab.eng.blr.redhat.com': {
                    'host': 'ghi.lab.eng.xyz.com',
                }
            }

    Returns:
        bool : True if listing all the mounts on all clients is successful.
            False otherwise.
    """
    rc = True
    cmd = powershell("net use")
    windows_clients_info = {}
    for client in clients_info:
        if ('platform' in clients_info[client] and
                clients_info[client]['platform'] == 'windows'):
            windows_clients_info[client] = clients_info[client]
    for client in windows_clients_info:
        if 'host' in windows_clients_info[client]:
            host = windows_clients_info[client]['host']
        else:
            host = client
        if 'super_user' in windows_clients_info[client]:
            user = windows_clients_info[client]['super_user']
        else:
            user = 'Admin'
        ret, out, err = g.run(host, cmd, user)
        if ret != 0:
            rc = False
    return rc
