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

"""
A tool to monitor and log memory consumption processes.
"""
from __future__ import print_function

import argparse
import csv
from time import sleep
import subprocess


def run_command(cmd):
    """
    Run command using Popen and return output
    """
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, shell=True)
    output = ret.stdout.read().decode('utf8').split('\n')[:-1]
    return output


def get_memory_and_cpu_consumption(proc_name):
    """
    Get the memory and cpu consumed by a given process
    """
    # The command gives an output as shown below:
    # [2020-08-07 09:34:48] 16422 0.0 9.99609
    #
    # Where,
    # [2020-08-07 09:34:48] is UTC timestamp.
    # 16422 is the process ID.
    # 0.0 is the CPU usage.
    # 9.99609 is memory consumption in MB.
    cmd = ("ps u -p `pgrep " + proc_name + "` | "
           "awk 'NR>1 && $11~/" + proc_name + "$/{print "
           "strftime(\"[%Y-%d-%m %H:%M:%S]\", "
           "systime(), 1), $2,$3,$6/1024}'")
    memory_and_cpu_consumed = run_command(cmd)
    return memory_and_cpu_consumed


def main():
    """
    Main function of the tool.
    """
    # Setting up command line arguments
    parser = argparse.ArgumentParser(
        description="A tool to log memory usage of a given process"
        )
    parser.add_argument(
        "-p", "--process_name", type=str, dest="process_name", required=True,
        help="Name of process for which cpu and memory is to be logged")
    parser.add_argument(
        "-i", "--interval", type=int, dest="interval", default=60,
        help="Time interval to wait between consecutive logs(Default:60)")
    parser.add_argument(
        "-c", "--count", type=int, dest="count", default=10,
        help="Number of times memory and CPU has to be logged (Default:10)")
    parser.add_argument(
        '-t', '--testname', type=str, dest="testname", required=True,
        help="Test name for which memory is logged")
    args = parser.parse_args()

    # Declare all three parameters
    process_name = args.process_name
    count = args.count
    interval = args.interval

    # Generating CSV file header
    with open('{}.csv'.format(process_name), 'a') as file:
        csv_writer_obj = csv.writer(file)
        csv_writer_obj.writerow([args.testname, '', '', ''])
        csv_writer_obj.writerow([
            'Time stamp', 'Process ID', 'CPU Usage', 'Memory Usage'])

        # Taking memory output for a given
        # number of times
        for counter in range(0, count):
            print("Iteration: {}".format(counter))
            data = get_memory_and_cpu_consumption(process_name)

            # Logging information to csv file
            for line in data:
                info = line.split(" ")
                csv_writer_obj.writerow([" ".join(info[:2]), info[2],
                                         info[3], info[4]])
                sleep(interval)


if __name__ == "__main__":
    main()
