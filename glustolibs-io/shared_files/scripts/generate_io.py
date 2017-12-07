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

import subprocess
import re
import time
import multiprocessing
import tempfile
import os
import shutil
import signal
import argparse
import sys
import yaml
import datetime

ONE_GB_BYTES = 1073741824.0

"""
Script for generating IO on client
"""


def get_disk_usage(path):
    """
    This module gets disk usage of the given path

    Args:
        path (str): path for which disk usage to be calculated

    Returns:
        dict: disk usage in dict format on success
        None Type, on failure

    """

    cmd = 'stat -f ' + path
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out, err = p.communicate()
    ret = p.returncode
    if ret != 0:
        print("Failed to execute stat command")
        return None

    res = ''.join(out)

    # Sample raw output of stat cmd to be parsed
    #   File: "write_data.py"
    #   ID: ffcb5576be049643 Namelen: 255     Type: ext2/ext3
    # Block size: 4096       Fundamental block size: 4096
    # Blocks: Total: 46014997   Free: 19716181   Available: 17372988
    # Inodes: Total: 11698176   Free: 11605234

    match = re.match(r'.*Block size:\s(\d+).*Blocks:\sTotal:\s(\d+)\s+?'
                     r'Free:\s(\d+)\s+?Available:\s(\d+).*Inodes:\s'
                     r'Total:\s(\d+)\s+?Free:\s(\d+)', res, re.S)
    if match is None:
        print("Regex mismatch in get_disk_usage()")
        return None

    usage_info = dict()
    keys = ['b_size', 'b_total', 'b_free', 'b_avail', 'i_total', 'i_free']
    val = list(match.groups())
    info = dict(zip(keys, val))
    usage_info['total'] = ((int(info['b_total']) * int(info['b_size'])) /
                           ONE_GB_BYTES)
    usage_info['free'] = ((int(info['b_free']) * int(info['b_size'])) /
                          ONE_GB_BYTES)
    usage_info['used_percent'] = (100 - (100.0 * usage_info['free'] /
                                  usage_info['total']))
    usage_info['total_inode'] = int(info['i_total'])
    usage_info['free_inode'] = int(info['i_free'])
    usage_info['used_percent_inode'] = ((100 -
                                        (100.0 * usage_info['free_inode']) /
                                        usage_info['total_inode']))
    usage_info['used'] = usage_info['total'] - usage_info['free']
    usage_info['used_inode'] = (usage_info['total_inode'] -
                                usage_info['free_inode'])
    return usage_info


def get_disk_used_percent(dirname):
    """
    Module to get disk used percent

    Args:
       dirname (str): absolute path of directory

    Returns:
        str: used percent for given directory
        None Type, on failure

    Example:
        get_disk_used_percent("/mnt/glusterfs")

    """

    output = get_disk_usage(dirname)
    if output is None:
        print("Failed to get disk used percent for %s"
              % dirname)
        return None
    return output['used_percent']


def check_if_percent_to_fill_or_timeout_is_met(dirname, percent_to_fill,
                                               timeout):
    """
    Module to check if percent to fill or timeout is met.

    Args:
        dirname (str): absolute path of directory
        percent_to_fill (int): percentage to fill the volume
        timeout (int): timeout value.

    Returns:
        bool: True, if volume is filled with given percent or timeout
            is met, False otherwise

    Example:
        check_if_percent_to_fill_or_timeout_is_met("/mnt/glusterfs",
                                                       10, 60)
    """
    flag = 0
    count = 0

    while ((timeout == 0) or (count < timeout)):
        output = get_disk_usage(dirname)
        used = output['used_percent']

        if int(percent_to_fill) > int(used):
            remaining_to_fill = int(percent_to_fill) - int(used)
            print("Remaining space left to fill data in directory %s is %s"
                  % (dirname, str(remaining_to_fill)))
            time_str = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            print("Directory %s used percent at time %s: %s"
                  % (dirname, time_str, used))
            if int(percent_to_fill) <= int(used):
                flag = 1
                break
            time.sleep(5)
            count = count + 5
        else:
            print("Directory %s is filled with given percent already. "
                  "Percentage filled: %s"
                  % (dirname, str(percent_to_fill)))
            flag = 1
            break

    if flag:
        print("Directory is filled with given percentage %s"
              % str(percent_to_fill))
        return True
    else:
        print("Timeout %s seconds reached before filling directory with "
              "given percentage %s" % (str(timeout), str(percent_to_fill)))
        return True
    return False


def run_check_if_percent_to_fill_or_timeout_is_met(dirname,
                                                   percent_to_fill,
                                                   timeout, event):
    """
    Helper Module to check if percent to fill or timeout is met.
    """
    ret = check_if_percent_to_fill_or_timeout_is_met(dirname,
                                                     percent_to_fill,
                                                     timeout)
    if ret:
        event.set()
        return True
    else:
        return False


def run_fio(proc_queue, script_path, dirname,
            job_files_list, log_file):
    """
    Module to invoke IOs using fio tool

    Args:
        proc_queue (obj): multiprocessing queue object
        script_path (str): absolute path of the run_fio.py script
        dirname (str): absolute path of dir to write data with fio
        job_files_list (list): list of ini job files for fio
        log_file (str): log file name for logging fio console output

    Returns:
        bool: True, if fio starts to write data and stops when it
            gets "STOP" string in queue, False otherwise

    """
    tmpdir = tempfile.mkdtemp()
    job_files_list_to_run = []
    for job_file in job_files_list:
        job_file_to_run = tmpdir + "/" + os.path.basename(job_file)
        shutil.copy(job_file, job_file_to_run)
        job_files_list_to_run.append(job_file_to_run)

    if log_file is not None:
        with open(log_file, "w") as fd:
            time_str = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            title = ("=========STARTING FIO-" + time_str +
                     "=======\n")
            fd.write(title)
            fd.close()
        cmd = ("python " + script_path +
               " --job-files '" + ' '.join(job_files_list_to_run) + "' " +
               dirname + " >> " + log_file + " 2>&1")

    else:
        cmd = ("python " + script_path +
               " --job-files '" + ' '.join(job_files_list_to_run) +
               "' " + dirname)
    p = subprocess.Popen(cmd, shell=True,
                         preexec_fn=os.setsid)
    time.sleep(10)
    if p is None:
        print("Unable to trigger IO using fio")
        return False
    while True:
        if proc_queue.get() == 'STOP':
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            time.sleep(2)
            with open(log_file, "a") as fd:
                time_str = (datetime.datetime.now().
                            strftime('%Y_%m_%d_%H_%M_%S'))
                title = ("=========ENDING FIO-" + time_str +
                         "=======\n")
                fd.write(title)
                fd.close()
            break

    shutil.rmtree(tmpdir)
    return True


def start_populate_data(mount_point, io_dict,
                        percent_to_fill, timeout):
    """
    Starts populating data on the directory

    Args:
        mount_point(str): Directory name to fill data
        io_dict (dict): dict of io related information
        percent_to_fill (int): percentage to fill the directory
        timeout (int): timeout value

    Returns:
        bool: returns True, if IO succeeds. False, otherwise

    """

    dirname = mount_point
    m = multiprocessing.Manager()
    event = m.Event()

    proc_list = []
    proc_queue = []

    for each_io in io_dict.keys():
            q = multiprocessing.Queue()
            proc_queue.append(q)
            workload_type = io_dict[each_io]['workload_type']
            proc = multiprocessing.Process(target=(io_dict[each_io]
                                                   ['function_addr']),
                                           args=(q,
                                                 (io_dict[each_io]
                                                  ['script_path']),
                                                 dirname,
                                                 (io_dict[each_io]['job_files']
                                                  [workload_type]),
                                                 io_dict[each_io]['log_file']))
            proc_list.append(proc)
            time.sleep(5)
            proc.start()

    p = multiprocessing.Process(
        target=run_check_if_percent_to_fill_or_timeout_is_met,
        args=(dirname, percent_to_fill, timeout, event,))

    time.sleep(5)
    proc_list.append(p)
    p.start()
    time.sleep(2)
    ret = stop_populate_data(proc_list, proc_queue, mevent=event)
    return ret


def stop_populate_data(proc_list, proc_queue, mevent=None):
    """
    Stops populating data on the directory

    Args:
        proc_list (list): List of processes to kill
        proc_queue (list): List of process queues to close

    Kwargs:
        mevent (obj): multiprocessing event object is passed, then
            it waits till the event is set by one of the process,
            Defaults to None.

    Returns:
        bool: If async=False, returns True, if data population is stopped
            in all the processes. False, otherwise
              If async=True, return list of process. False, otherwise

    Example:
        stop_populate_data(proc_list, proc_queue)
    """

    try:
        if mevent:
            mevent.wait()

        for q in proc_queue:
            q.put("STOP")
            time.sleep(5)
            q.close()
            q.join_thread()
        for proc in proc_list:
            proc.terminate()
        return True
    except Exception as e:
        print("Exception occured in stop_populate_data(): %s"
              % e)
        return False


def call_get_disk_usage(args):
    """
    Main method for getting disk usage
    """

    disk_usage = get_disk_usage(args.dir)
    if disk_usage is None:
        return 1
    print disk_usage
    return 0


def call_start_populate_data(args):
    """
    Main method for populating data
    """

    dirname = args.dir
    config_file_list = args.c.split()
    workload = args.w
    percent = args.p
    timeout = args.t
    log_file = args.l

    # Collects config data from multiple config files
    config_data = {}
    for config_file in config_file_list:
        with open(config_file, 'r') as f:
            each_config_data = yaml.load(f)
            config_data.update(each_config_data)

    # Handling the following cases as per user option.
    # case1: If user gives -i option only, then select io tools from user
    #        option.
    # case2: If user gives -w option only, look for given workload in config
    #        file and choose io tools for the specified workload from
    #        config file.
    # case3: if -i and -w option specified, select workload and select io
    #        tools as specified in -i and also it should be part of the list
    #        of io tools available for that workload.
    # case4: If -i | -w | -i and -w is not specified , run all the tools
    #        specified in the config file

    if args.i is not None:
        io_list = args.i.split()
    else:
        io_list = []

    workload_type = ""
    if workload is not None:
        if (('workload' in config_data['io'] and
             config_data['io']['workload'] and
             workload in config_data['io']['workload'])):
            if not io_list:
                io_list = config_data['io']['workload'][workload]
            else:
                io_list_from_user = io_list
                io_list_for_given_workload = (config_data['io']
                                              ['workload'][workload])
                io_list = (list(set(io_list_from_user).
                           intersection(io_list_for_given_workload)))
            workload_type = workload
    else:
        if not io_list:
            io_list = config_data['io']['generic_workload']

    # If workload type is not given by the user, then by default
    # generic_workload is assigned.
    if not workload_type:
        workload_type = "generic_workload"

    if timeout is None:
        timeout = 0

    log_file_dir = os.path.dirname(log_file)
    if not os.path.exists(log_file_dir):
        os.makedirs(log_file_dir)

    filename, file_ext = os.path.splitext(log_file)
    time_str = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    log_file = filename + "_" + time_str + file_ext

    print "GENERATE IO Log file: %s" % log_file

    if('io' in config_data and 'tools' in config_data['io']):
        config_data_io = dict(config_data['io']['tools'])
    else:
        print "io tools info is not given in config file"
        return 1

    if('io' in config_data and 'scripts' in config_data['io']):
        config_data_io.update(config_data['io']['scripts'])
    else:
        print "io scripts info is not given in config file"
        return 1

    io_details = {}
    for io in io_list:
        if io in config_data_io.keys():
            config_data_io[io]['function_addr'] = eval("run_" + io)
            config_data_io[io]['log_file'] = (log_file_dir + "/" +
                                              io + "_log.log")
            config_data_io[io]['workload_type'] = workload_type
            io_details[io] = config_data_io[io]
        else:
            print ("The IO tool/script - '%s' details not present in config "
                   "file. Skipping the IO - '%s'" % (io, io))

    if not io_details:
        print "Config file doesn't have IO details for %s" % ','.join(io_list)
        return 1

    # Starts generating IO
    # If -t and -p bot are passed as options, runs all the io's as specified
    # until '-t' or '-p' is reached. i.e which ever reaches first.
    ret = start_populate_data(dirname, io_details, percent, timeout)
    print "Disk Usage Details of %s: %s" % (dirname, get_disk_usage(dirname))

    fd_list = []
    for io in io_details.keys():
        if 'log_file' in io_details[io]:
            fh = open(io_details[io]['log_file'], "r")
            fd_list.append(fh)

    if log_file is not None:
        with open(log_file, 'a') as fd:
            for each_fh in fd_list:
                fd.write(each_fh.read())
                each_fh.close()
            fd.write("\nDisk Usage Details of %s: %s" % (dirname,
                     get_disk_usage(dirname)))
            fd.close()

    if ret:
        return 0
    else:
        return 1


if __name__ == "__main__":
    print "Starting IO Generation..."
    test_start_time = datetime.datetime.now().replace(microsecond=0)

    write_data_parser = argparse.ArgumentParser(prog="generate_io.py",
                                                description=("Program for "
                                                             "generating io"))

    write_data_required_parser = write_data_parser.add_argument_group(
                                                    'required named arguments')

    write_data_required_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    write_data_required_parser.add_argument('-c', help="space separated list "
                                                       "of config files",
                                            required=True)
    write_data_parser.add_argument('-i', help="space separated list of "
                                              "io tools")
    write_data_parser.add_argument('-w', help="Workload type")
    write_data_parser.add_argument('-p', help="percentage to fill the"
                                              "directory",
                                   type=int, default=100)
    write_data_parser.add_argument('-t', help="timeout value in seconds.",
                                   type=int)
    default_log_file = "/var/tmp/generate_io/generate_io.log"
    write_data_parser.add_argument('-l', help="log file name.",
                                   default=default_log_file)

    write_data_parser.set_defaults(func=call_start_populate_data)

    args = write_data_parser.parse_args()
    rc = args.func(args)
    test_end_time = datetime.datetime.now().replace(microsecond=0)
    print "Execution time: %s" % (test_end_time - test_start_time)
    print "Ending IO Generation"
    sys.exit(rc)
