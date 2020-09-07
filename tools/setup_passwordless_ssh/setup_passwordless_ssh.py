#!/usr/bin/env python3
#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY :or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import argparse
import sys
from os import system
from yaml import safe_load


def read_config_file(config_file):
    """
    A function to read the yaml file given to the script.
    Args:
        config_file(str): A config file used to run glusto-tests.
    Return:
        dict: A dictornary with all the details from config file.
    """
    return safe_load(open(config_file, 'r'))


def setup_passwordless_ssh(server, username, password):
    """
    A function to setup passwordless ssh to all servers.
    Args:
        server(str): hostname/IP of server for
                     passwordless ssh has to be configured.
        username(str): User to be used to login.
        password(str): password to be used to login.
    Returns:
        bool: True if successful else false.
    """
    command = ("sshpass -p %s ssh-copy-id -o StrictHostKeyChecking=no %s@%s"
               % (password, username, server))
    ret = system(command)
    return not ret


def check_passwordless_ssh_setup(server, username):
    """
    A function to check if passwordless ssh setup was successfull or not.
    Args:
        server(str): hostname/IP of server for
                     passwordless ssh has to be configured.
        username(str): User to be used to login.
    Returns:
        bool: True if successful else false.
    """
    command = ("ssh %s@%s hostname" % (username, server))
    ret = system(command)
    return not ret


def main():
    """
    Main function of the tool.
    """

    # Setting up command line arguments.
    parser = argparse.ArgumentParser(
        description="Tool to setup passwordless ssh to all nodes."
        )
    parser.add_argument("-c", "--config_file",
                        type=str, dest="config_file",
                        help="A glusto-tests configuration file.")
    parser.add_argument("-p", "--password", dest="password",
                        type=str, help="Password of servers.")
    parser.add_argument("-u", "--username", dest="username",
                        type=str, default="root",
                        help="User to be used to setup"
                        " passwordless ssh.")
    args = parser.parse_args()

    # Reading the config file.
    if args.config_file:
        config = read_config_file(args.config_file)
    else:
        sys.exit("[ERROR]:Config file not provided.")

    # Checking if password was provided.
    if args.password:
        password = args.password
    else:
        sys.exit("[ERROR]:Password not provided.")

    # Configuring passwordless ssh to all servers.
    for server in config.get('servers', []):
        ret = setup_passwordless_ssh(server, args.username,
                                     password)
        if not ret:
            sys.exit("[ERROR]:Unable to setup "
                     "passwordless ssh to %s."
                     % server)
        ret = check_passwordless_ssh_setup(server,
                                           args.username)
        if ret:
            print("[INFO]:Passwordless ssh setup "
                  "completed to %s." % server)

    # Configuring passwordless ssh to all clients.
    for server in config.get('clients', []):
        ret = setup_passwordless_ssh(server,
                                     args.username,
                                     password)

        if not ret:
            sys.exit("[ERROR]:Unable to setup "
                     "passwordless ssh to %s."
                     % server)

        ret = check_passwordless_ssh_setup(server,
                                           args.username)
        if ret:
            print("[INFO]:Passwordless ssh setup "
                  "completed to %s." % server)

    # Configure paswordless ssh to all geo-rep slaves nodes.
    for server in config.get('slaves', []):
        ret = setup_passwordless_ssh(server,
                                     args.username,
                                     password)
        if not ret:
            sys.exit("[ERROR]:Unable to setup "
                     "passwordless ssh to %s."
                     % server)
        ret = check_passwordless_ssh_setup(server,
                                           args.username)
        if ret:
            print("[INFO]:Passwordless ssh setup "
                  "completed to %s." % server)


if __name__ == "__main__":
    main()
