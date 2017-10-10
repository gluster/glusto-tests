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

import os
import argparse
import fileinput
import re
import subprocess
import time


def generate_workload_using_fio(root_dirname, ini_file):
    """
    Populates data in the given directory using fio tool.

    Args:
        root_dirname (str): Directory name
        ini_file (str): fio job file

    Example:
        generate_workload_using_fio("/tmp", 'job1.ini')

    """
    dirpath_list = [x[0] for x in (os.walk(root_dirname))]

    for dirpath in dirpath_list:
        fname = "[" + dirpath + "/fio_" + os.path.basename(ini_file) + "]"
        for line in fileinput.input(ini_file, inplace=True):
            line = re.sub(r'\[.*\]', fname, line.rstrip())
            print(line)

        fio_cmd = "fio " + ini_file
        subprocess.call(fio_cmd, shell=True)


if __name__ == "__main__":

    # Note: Make sure fio tool is installed in the node
    # Please refer below link for installing fio.
    # http://git.kernel.dk/?p=fio.git;a=blob;f=README;
    # h=5fa37f3eed33a15a15a38836cf0080edc81688fd;hb=HEAD

    parser = argparse.ArgumentParser(prog="test_fio.py",
                                     description=("Generate workload "
                                                  "using fio"))
    parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which IO has to be performed")
    parser.add_argument('--job-files',
                        metavar=('job_files'), dest='job_files',
                        help="space separated absolute paths of "
                             "ini job files", required=True)
    args = parser.parse_args()
    root_dirname = args.dir
    ini_files_list = args.job_files.split()

    for ini_file in ini_files_list:
        generate_workload_using_fio(root_dirname, ini_file)
    time.sleep(2)
