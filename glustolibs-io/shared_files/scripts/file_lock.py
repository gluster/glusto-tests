#!/usr/bin/env python
#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
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

from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN
from time import sleep
from argparse import ArgumentParser


def get_file_lock(args):
    """
    Gets the lock to a file and releases it after timeout
    """
    file_name = args.f
    timeout = args.t
    f = open(file_name, 'w')
    flock(f.fileno(), LOCK_EX | LOCK_NB)
    sleep(int(timeout))
    flock(f.fileno(), LOCK_UN)


if __name__ == "__main__":
    file_lock_parser = ArgumentParser(
        prog="file_lock.py", description="Program to validate file lock ops")

    file_lock_req_args = file_lock_parser.add_argument_group(
        'required named arguments')
    file_lock_req_args.add_argument(
        '-f', type=str, required=True,
        help="File on which lock has to be applied")
    file_lock_req_args.add_argument(
        '-t', help="time for which lock has to be retained", type=int,
        required=True)

    file_lock_parser.set_defaults(func=get_file_lock)

    args = file_lock_parser.parse_args()
    rc = args.func(args)
