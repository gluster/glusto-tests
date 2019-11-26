#!/usr/bin/env python
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

from __future__ import print_function
import argparse
import datetime
from multiprocessing import Process
import os
import random
import string
import sys
import time


def is_root(path):
    """Check whether the given path is '/' or not

    Args:
        path (str):  Path of the dir to check

    Returns:
        True if path is '/' , False otherwise
    """
    if os.path.realpath(os.path.abspath(path)) == '/':
        print("Directory '%s' is the root of filesystem. "
              "Not performing any operations on the root of filesystem" % (
                  os.path.abspath(path)))
        return True
    else:
        return False


def path_exists(path):
    """Check if path exists are not.

    Args:
        path (str): Path to check if it exist or not.

    Returns:
        bool : True if path exists, False otherwise.
    """
    if os.path.exists(os.path.abspath(path)):
        return True
    else:
        return False


def create_dir(dir_path):
    """Create dir if 'dir_path' does not exists

    Args:
        dir_path (str): Directory path to create

    Returns:
        0 on successful creation of dir, 1 otherwise.
    """
    dir_abs_path = os.path.abspath(dir_path)
    if not path_exists(dir_abs_path):
        try:
            os.makedirs(dir_abs_path)
        except (OSError, IOError):
            print("Unable to create dir: %s" % dir_abs_path)
            return 1
    return 0


def fd_write_file(filename, file_size, chunk_sizes_list, write_time,
                  delay_between_writes=10, log_level='INFO'):
    """Write random data to the file until write_time."""
    rc = 0
    time_counter = 0

    try:
        fd = open(filename, "w+b")
        fd.seek(file_size - 1)
        fd.write(bytes(str("0").encode("utf-8")))
        fd.flush()
    except IOError as e:
        print("Unable to open file %s for writing : %s" % (
            filename, e.strerror))
        return 1

    while time_counter < write_time:
        try:
            actual_file_size = os.stat(filename).st_size
            current_chunk_size = random.choice(chunk_sizes_list)
            write_data = (''.join(random.choice(string.printable) for x in
                                  range(current_chunk_size)))
            offset = random.randint(0, (actual_file_size - current_chunk_size))
            if log_level.upper() == 'DEBUG':
                print("\tFileName: %s, File Size: %s, "
                      "Writing to offset: %s, "
                      "Data Length: %d, Time Counter: %d" % (
                          filename, actual_file_size, offset, len(write_data),
                          time_counter))
            fd.seek(offset)
            fd.write(bytes(str(write_data).encode("utf-8")))
            fd.seek(0)
            fd.flush()
        except IOError as e:
            print("Unable to write to file '%s' : %s at time count: %dS" % (
                filename, e.strerror, time_counter))
            rc = 1

        time.sleep(delay_between_writes)
        time_counter = time_counter + delay_between_writes

    fd.close()
    return rc


def fd_writes(args):
    dir_path = os.path.abspath(args.dir)
    number_of_files = int(args.num_of_files)
    base_file_name = args.base_file_name
    file_sizes_list = args.file_sizes_list
    if file_sizes_list:
        file_sizes_list = list(filter(None, args.file_sizes_list.split(",")))
    chunk_sizes_list = args.chunk_sizes_list
    if chunk_sizes_list:
        chunk_sizes_list = list(
            map(int, filter(None, args.chunk_sizes_list.split(","))))
    write_time = int(args.write_time)
    delay_between_writes = int(args.delay_between_writes)
    log_level = args.log_level

    # Check if dir_path is '/'
    if is_root(dir_path):
        return 1

    # Create dir_path
    rc = create_dir(dir_path)
    if rc != 0:
        return rc

    file_sizes_dict = {
        'k': 1024,
        'K': 1024,
        'm': 1024 ** 2,
        'M': 1024 ** 2,
        'g': 1024 ** 3,
        'G': 1024 ** 3,
    }

    file_sizes_expanded_list = []
    for size in file_sizes_list:
        if size.isdigit():
            file_sizes_expanded_list.append(size)
        else:
            size_numeric_value = int(size[:-1])
            size_postfix = size[-1]
            size_expanded = size_numeric_value * file_sizes_dict[size_postfix]
            file_sizes_expanded_list.append(size_expanded)

    process_list = []
    for dirName, subdirList, fileList in os.walk(dir_path, topdown=False):
        all_files_list = []
        for i in range(number_of_files):
            filename = os.path.join(dirName, "%s_%d" % (base_file_name, i))
            all_files_list.append(filename)

        for filename in all_files_list:
            process_list.append(
                Process(target=fd_write_file,
                        args=(filename,
                              random.choice(file_sizes_expanded_list),
                              chunk_sizes_list,
                              write_time, delay_between_writes, log_level)
                        ))

    for each_process in process_list:
        each_process.start()

    for each_process in process_list:
        each_process.join()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Program to perform fd based writes on files for time t",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-f', '--file-sizes-list',
                        help="Randomly select the size of the "
                        "file from comma separated file_sizes_list. "
                        "Example: 1k,10k,512K,1M,512m,1g,5G. ",
                        dest="file_sizes_list",
                        action="store",
                        default="1k,10k,512k,1M")

    parser.add_argument('-c', '--chunk-sizes-list',
                        help="Randomly select chunk size per write from "
                        "comma separated chunk_sizes_list. "
                        "Example: 16,32,64,128,256,512,1024. ",
                        dest="chunk_sizes_list",
                        action="store",
                        default="16,32,64,128,256,512,1024")

    parser.add_argument('-t', '--write-time',
                        help="Total write time for a file in seconds.",
                        dest="write_time", action="store", default=60)

    parser.add_argument('-d', '--delay-between-writes',
                        help="Delay time between writes in seconds. ",
                        dest="delay_between_writes", action="store",
                        default=10)

    parser.add_argument('-n', '--num-of-files',
                        help="Number of files to create ",
                        dest="num_of_files", action="store", default=1)

    parser.add_argument('-b', '--base-file-name',
                        help="Base File Name",
                        dest='base_file_name', action="store",
                        default="testfile")

    parser.add_argument('--log-level',
                        help="Log Level",
                        dest='log_level', action="store",
                        default="INFO")

    parser.add_argument('--dir', metavar='DIR', type=str,
                        help="Directory on which operations has "
                        "to be performed")

    parser.set_defaults(func=fd_writes)

    print("Starting Script: %s" % ' '.join(sys.argv))
    print("StarTime :'%s' " % datetime.datetime.now())

    test_start_time = datetime.datetime.now().replace(microsecond=0)
    args = parser.parse_args()
    rc = args.func(args)

    test_end_time = datetime.datetime.now().replace(microsecond=0)
    print("Execution time: %s" % (test_end_time - test_start_time))
    print("EndTime :'%s' " % datetime.datetime.now())

    sys.exit(rc)
