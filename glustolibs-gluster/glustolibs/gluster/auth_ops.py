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
    Description: This contains the gluster volume auth allow and
    reject operations
"""
from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_options


def set_auth_allow(volname, server, auth_dict):
    """
    Set authentication for volumes or sub directories as required

    Args:
        volname(str): The name of volume in which auth
            has to be set
        server(str): IP or hostname of one node
        auth_dict(dict): key-value pair of dirs and clients list
            Example: auth_dict = {'/d1':['10.70.37.172','10.70.37,173'],
                '/d3/subd1':['10.70.37.172','dhcp37-999.xyz.cdf.pqr.abc.com']}
            If authentication has to set on entire volume, use 'all' as key.
                auth_dict = {'all': ['10.70.37.172','10.70.37,173']}
                'all' refers to entire volume
    Returns (bool):
        True if all the auth set operation is success.
    """
    auth_cmds = []
    if not auth_dict:
        g.log.error("Authentication details are not provided")
        return False

    # If authentication has to be set on sub-dirs, convert the key-value pair
    # to gluster authentication set command format.
    if 'all' not in auth_dict:
        for key, value in auth_dict.iteritems():
            auth_cmds.append("%s(%s)" % (key, "|".join(value)))

        auth_cmd = ("gluster volume set %s auth.allow \"%s\""
                    % (volname, ",".join(auth_cmds)))

    # When authentication has to be set on entire volume, convert the
    # key-value pair to gluster authentication set command format
    else:
        auth_cmd = ("gluster volume set %s auth.allow %s"
                    % (volname, ",".join(auth_dict["all"])))

    # Execute auth.allow setting on server.
    ret, _, _ = g.run(server, auth_cmd)
    if (not ret) and (verify_auth_allow(volname, server, auth_dict)):
        g.log.info("Authentication set and verified successfully.")
        return True
    return False


def verify_auth_allow(volname, server, auth_dict):
    """
    Verify authentication for volumes or sub directories as required

    Args:
        volname(str): The name of volume in which auth
            has to be set
        server(str): IP or hostname of one node
        auth_dict(dict): key-value pair of dirs and clients list
            Example: auth_dict = {'/d1':['10.70.37.172','10.70.37,173'],
                '/d3/subd1':['10.70.37.172','10.70.37.197']}
            If authentication has to set on entire volume, use 'all' as key.
                auth_dict = {'all': ['10.70.37.172','10.70.37,173']}
                'all' refers to entire volume
    Returns (bool):
        True if the verification is success.
    """
    auth_details = []
    if not auth_dict:
        g.log.error("Authentication details are not provided")
        return False

    # Get the value of auth.allow option of the volume
    auth_clients_dict = get_volume_options(server, volname, "auth.allow")
    auth_clients = auth_clients_dict['auth.allow']

    # When authentication has to be verified on entire volume(not on sub-dirs)
    # check whether the required clients names are listed in auth.allow option
    if 'all' in auth_dict:
        clients_list = auth_clients.split(',')
        res = all(elem in clients_list for elem in auth_dict['all'])
        if not res:
            g.log.error("Authentication verification failed. auth.allow: %s",
                        auth_clients)
            return False
        g.log.info("Authentication verified successfully. auth.allow: %s",
                   auth_clients)
        return True

    # When authentication has to be verified on on sub-dirs, convert the key-
    # value pair to a format which matches the value of auth.allow option.
    for key, value in auth_dict.iteritems():
        auth_details.append("%s(%s)" % (key, "|".join(value)))

    # Check whether the required clients names are listed in auth.allow option
    for auth_detail in auth_details:
        if auth_detail not in auth_clients:
            g.log.error("Authentication verification failed. auth.allow: %s",
                        auth_clients)
            return False
    g.log.info("Authentication verified successfully. auth.allow: %s",
               auth_clients)
    return True


def verify_auth_reject(volname, server, auth_dict):
    """
    Verify auth reject for volumes or sub directories as required

    Args:
        volname(str): The name of volume in which auth reject
            has to be set
        server(str): IP or hostname of one node
        auth_dict(dict): key-value pair of dirs and clients list
            Example: auth_dict = {'/d1':['10.70.37.172','10.70.37,173'],
                '/d3/subd1':['10.70.37.172','dhcp37-999.xyz.cdf.pqr.abc.com']}
            If authentication has to set on entire volume, use 'all' as key.
                auth_dict = {'all': ['10.70.37.172','10.70.37,173']}
                            'all' refer to entire volume
    Returns (bool):
        True if all the authentication is success.
    """
    auth_details = []
    if not auth_dict:
        g.log.error("Authentication details are not provided")
        return False

    # Get the value of auth.reject option of the volume
    auth_clients_dict = get_volume_options(server, volname, "auth.reject")
    auth_clients = auth_clients_dict['auth.reject']

    # When authentication has to be verified on entire volume(not on sub-dirs)
    # check if the required clients names are listed in auth.reject option
    if 'all' in auth_dict:
        clients_list = auth_clients.split(',')
        res = all(elem in clients_list for elem in auth_dict['all'])
        if not res:
            g.log.error("Authentication verification failed. auth.reject: %s",
                        auth_clients)
            return False
        g.log.info("Authentication verified successfully. auth.reject: %s",
                   auth_clients)
        return True

    # When authentication has to be verified on on sub-dirs, convert the key-
    # value pair to a format which matches the value of auth.reject option.
    for key, value in auth_dict.iteritems():
        auth_details.append("%s(%s)" % (key, "|".join(value)))

    # Check if the required clients names are listed in auth.reject option
    for auth_detail in auth_details:
        if auth_detail not in auth_clients:
            g.log.error("Authentication verification failed. auth.reject: %s",
                        auth_clients)
            return False
    g.log.info("Authentication verified successfully. auth.reject: %s",
               auth_clients)
    return True


def set_auth_reject(volname, server, auth_dict):
    """
    Set auth reject for volumes or sub directories as required

    Args:
        volname(str): The name of volume in which auth reject
                    has to be set
        server(str): IP or hostname of one node
        auth_dict(dict): key-value pair of dirs and clients list
            Example: auth_dict = {'/d1':['10.70.37.172','10.70.37,173'],
                '/d3/subd1':['10.70.37.172',''dh37-999.xyz.cdf.pqr.abc.com'']}
            If authentication has to set on entire volume, use 'all' as key.
                auth_dict = {'all': ['10.70.37.172','10.70.37,173']}
                            'all' refer to entire volume
    Returns (bool):
        True if the auth reject operation is success.
    """
    auth_cmds = []
    if not auth_dict:
        g.log.error("Authentication details are not provided")
        return False

    # If authentication has to be set on sub-dirs, convert the key-value pair
    # to gluster authentication set command format.
    if 'all' not in auth_dict:
        for key, value in auth_dict.iteritems():
            auth_cmds.append("%s(%s)" % (key, "|".join(value)))

            auth_cmd = ("gluster volume set %s auth.reject \"%s\""
                        % (volname, ",".join(auth_cmds)))

    # When authentication has to be set on entire volume, convert the
    # key-value pair to gluster authentication set command format.
    else:
        auth_cmd = ("gluster volume set %s auth.reject %s"
                    % (volname, ",".join(auth_dict["all"])))

    # Execute auth.allow setting on server.
    ret, _, _ = g.run(server, auth_cmd)
    if (not ret) and (verify_auth_reject(volname, server, auth_dict)):
        g.log.info("Auth reject set and verified successfully.")
        return True
    return False
