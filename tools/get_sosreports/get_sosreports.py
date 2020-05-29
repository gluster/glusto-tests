#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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

# Imports needed by the script.
import argparse
import os
import sys
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


def remove_previous_sosreports(server):
    """
    A function to remove old sosreports.

    Args:
        server: hostname/IP server from which sosreport
                has to be removed.

    Returns:
        bool: True if successful else false.
    """
    cmd = ("ssh root@{} \"rm -rf /var/tmp/sosreport-*\""
           .format(server))
    ret = os.system(cmd)
    if ret:
        return False
    return True


def collect_new_sosreports(server):
    """
    A function to generate sosreports.

    Args:
    server: hostname/IP server from which sosreport
            has to be collected.

    Returns:
        bool: True if successful else false.
    """
    cmd = ("ssh root@{} \"sosreport --batch --name=$HOSTNAME\""
           .format(server))
    ret = os.system(cmd)
    if ret:
        return False
    return True


def copy_sosreports_to_dir(server, directory):
    """
    A function to copy sosreports to local dir.

    Args:
    server: hostname/IP of server for passwordless ssh
            has to be configured.
    directory: Directory to be used to store sosreports.

    Returns:
        bool: True if successful else false.
    """
    cmd = ("scp root@{}:/var/tmp/sosreport-* {}"
           .format(server, directory))
    ret = os.system(cmd)
    if ret:
        return False
    return True


def check_and_create_dir_if_not_present(directory):
    """
    A function to check and create directory if not present.

    Args:
    directory: Directory to be checked/created.

    Returns:
        bool: True if successful else false.
    """
    if not os.path.isdir(directory):
        cmd = ("mkdir -p {}".format(directory))
        ret = os.system(cmd)
        if ret:
            return False
    else:
        print("[INFO]:The dir already exists.")
    return True


def main():
    """
    Main function of the tool.
    """
    # Setting up command line arguments.
    parser = argparse.ArgumentParser(
        description="Tool to collect sosreports from servers and clients."
        )
    parser.add_argument("-f",
                        "--config_file",
                        type=str,
                        dest="config_file",
                        help="A glusto-tests configuration file.")
    parser.add_argument("-m", "--servers", type=str,
                        dest="servers",
                        help=("A list of hostnames/ips of"
                              " servers seperated by comma(',')."))
    parser.add_argument("-d", "--dist-dir", type=str, default=".",
                        dest="directory",
                        help=("Directory where reports are to be stored."
                              "(Default:.)"))
    args = parser.parse_args()

    # Getting list of hostname/IP.
    if args.servers:
        servers = args.servers.split(',')

    # Reading the config file.
    if args.config_file:
        config = read_config_file(args.config_file)
        servers = []
        servers += config.get('clients', [])
        servers += config.get('servers', [])

    # Fetching other parameters from command line.
    directory = args.directory

    # Checking and creating dir if not present.
    ret = check_and_create_dir_if_not_present(directory)
    if not ret:
        sys.exit("[ERROR]:Unable to create dir for storing sosreports.")

    try:
        for server in servers:

            # Removing old sosreports from the server.
            ret = remove_previous_sosreports(server)
            if not ret:
                sys.exit("[ERROR]:Unable to remove old sosreports on {}!"
                         .format(server))
            print("[INFO]:Successfully removed old sosreports on {}."
                  .format(server))

            # Collecting sosreport on the server.
            ret = collect_new_sosreports(server)
            if not ret:
                sys.exit("[ERROR]:Unable to collect sosreport on {}!"
                         .format(server))
            print("[INFO]:Successfully collected sosreport on {}."
                  .format(server))

            # Downloading sosreport to local machine.
            ret = copy_sosreports_to_dir(server, directory)
            if not ret:
                sys.exit("[ERROR]:Unable download sosreport from {}."
                         .format(server))
            print("[INFO]:Successfully copied sosreports from {}."
                  .format(server))

    # If servers aren't provided.
    except UnboundLocalError:
        sys.exit("[ERROR]:servers were not provided")


if __name__ == "__main__":
    main()
