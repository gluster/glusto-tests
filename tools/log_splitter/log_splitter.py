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


def check_and_create_dir_if_not_present(directory):
    """
    A function to check and create directory if not present

    Args:
        directory(str): Directory to be created if not present

    Retuns:
        bool: True if successful else False
    """
    if not os.path.isdir(directory):
        cmd = "mkdir -p {}".format(directory)
        ret = os.system(cmd)
        if ret:
            return False
        print("[INFO]: Dir created successfully")
    else:
        print("[INFO]: The dir already exists")
    return True


def main():
    """
    Main function of the tool.
    """
    # Setting up command line arguments.
    parser = argparse.ArgumentParser(
        description="Tool to split glusto logs to individual testcase logs."
        )
    parser.add_argument(
        '-f', '--log_file', type=str, dest='log_file', required=True,
        help="Glusto test log file")
    parser.add_argument(
        '-d', '--dist-dir', type=str, default=".", dest="destination_dir",
        help="Path were individual test logs are to be stored.")
    args = parser.parse_args()

    # Fetching the values from command line.
    log_file = args.log_file
    destination_dir = args.destination_dir

    # Check and create dir if not present
    if not check_and_create_dir_if_not_present(destination_dir):
        sys.exit("[ERROR]: Unable to create dir")

    with open(log_file, 'r', encoding="ISO-8859-1") as log_file_fd:

        # Read lines and set flag to check if
        # file is open
        file_open_flag = False
        while True:
            line = log_file_fd.readline()
            if not line:
                break

            # Check if line is starting line.
            if '(setUp) Starting Test : ' in line:
                if file_open_flag:
                    file_open_flag = False

                # Open new fd for individual test
                # file
                filename = line.split(' ')[7]
                if destination_dir != '.':
                    filename = os.path.join(destination_dir,
                                            filename)
                file_open_flag = True

            # Write lines to individual test file
            if file_open_flag:
                with open(filename, 'w') as test_file:
                    test_file.write(line)

    print("[INFO]: Log file split completed")


if __name__ == "__main__":
    main()
