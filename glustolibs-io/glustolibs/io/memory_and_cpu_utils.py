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

from glusto.core import Glusto as g

from glustolibs.gluster.volume_ops import get_volume_status
from glustolibs.gluster.glusterfile import file_exists
from glustolibs.misc.misc_libs import upload_scripts, kill_process

import numpy as np
import pandas as pd
from statistics import mean, median


def check_upload_memory_and_cpu_logger_script(servers):
    """Check and upload memory_and_cpu_logger.py to servers if not present

    Args:
     servers(list): List of all servers where script has to be uploaded

    Returns:
     bool: True if script is uploaded successfully else false
    """
    script = "/usr/share/glustolibs/io/scripts/memory_and_cpu_logger.py"
    is_present = []
    for server in servers:
        if not file_exists(server, script):
            if not upload_scripts(server, script):
                g.log.error("Unable to upload memory_and_cpu_logger.py on %s",
                            server)
                is_present.append(False)
            else:
                is_present.append(True)
    return all(is_present)


def _start_logging_processes(process, servers, test_name, interval, count):
    """Start logging processes on all nodes for a given process

    Args:
     servers(list): Servers on which CPU and memory usage has to be logged
     test_name(str): Name of testcase for which logs are to be collected
     interval(int): Time interval after which logs are to be collected
     count(int): Number of samples to be captured

    Returns:
     list: A list of logging processes
    """
    cmd = ("/usr/bin/env python "
           "/usr/share/glustolibs/io/scripts/memory_and_cpu_logger.py"
           " -p %s -t %s -i %d -c %d" % (process, test_name,
                                         interval, count))
    logging_process = []
    for server in servers:
        proc = g.run_async(server, cmd)
        logging_process.append(proc)
    return logging_process


def log_memory_and_cpu_usage_on_servers(servers, test_name, interval=60,
                                        count=100):
    """Log memory and CPU usage of gluster server processes

    Args:
     servers(list): Servers on which CPU and memory usage has to be logged
     test_name(str): Name of the testcase for which logs are to be collected

    Kwargs:
     interval(int): Time interval after which logs are to be collected
                    (Default:60)
     count(int): Number of samples to be captured (Default:100)

    Returns:
     dict: Logging processes dict for all gluster server processes
    """
    logging_process_dict = {}
    for proc_name in ('glusterd', 'glusterfs', 'glusterfsd'):
        logging_procs = _start_logging_processes(
            proc_name, servers, test_name, interval, count)
        logging_process_dict[proc_name] = logging_procs
    return logging_process_dict


def log_memory_and_cpu_usage_on_clients(servers, test_name, interval=60,
                                        count=100):
    """Log memory and CPU usage of gluster client processes

    Args:
     servers(list): Clients on which CPU and memory usage has to be logged
     test_name(str): Name of testcase for which logs are to be collected

    Kwargs:
     interval(int): Time interval after which logs are to be collected
                    (Defaults:60)
     count(int): Number of samples to be captured (Default:100)

    Returns:
     dict: Logging processes dict for all gluster client processes
    """
    logging_process_dict = {}
    logging_procs = _start_logging_processes(
        'glusterfs', servers, test_name, interval, count)
    logging_process_dict['glusterfs'] = logging_procs
    return logging_process_dict


def log_memory_and_cpu_usage_on_cluster(servers, clients, test_name,
                                        interval=60, count=100):
    """Log memory and CPU usage on gluster cluster

    Args:
     servers(list): Servers on which memory and CPU usage is to be logged
     clients(list): Clients on which memory and CPU usage is to be logged
     test_name(str): Name of testcase for which logs are to be collected

    Kwargs:
     interval(int): Time interval after which logs are to be collected
                    (Default:60)
     count(int): Number of samples to be captured (Default:100)

    Returns:
     dict: Logging processes dict for all servers and clients
    """
    # Start logging on all servers
    server_logging_processes = log_memory_and_cpu_usage_on_servers(
        servers, test_name, interval, count)
    if not server_logging_processes:
        return {}

    # Starting logging on all clients
    client_logging_processes = log_memory_and_cpu_usage_on_clients(
        clients, test_name, interval, count)
    if not client_logging_processes:
        return {}

    # Combining dicts
    logging_process_dict = {}
    for node_type, proc_dict in (('server', server_logging_processes),
                                 ('client', client_logging_processes)):
        logging_process_dict[node_type] = {}
        for proc in proc_dict:
            logging_process_dict[node_type][proc] = (
                proc_dict[proc])
    return logging_process_dict


def _process_wait_flag_append(proc, flag):
    """Run async communicate and adds true to flag list"""
    # If the process is already completed  async_communicate()
    # throws a ValueError
    try:
        proc.async_communicate()
        flag.append(True)
    except ValueError:
        flag.append(True)


def wait_for_logging_processes_to_stop(proc_dict, cluster=False):
    """Wait for all given logging processes to stop

    Args:
     proc_dict(dict): Dictionary of all the active logging processes

    Kwargs:
     cluster(bool): True if proc_dict is for the entire cluster else False
                    (Default:False)

    Retruns:
     bool: True if processes are completed else False
    """
    flag = []
    if cluster:
        for sub_dict in proc_dict:
            for proc_name in proc_dict[sub_dict]:
                for proc in proc_dict[sub_dict][proc_name]:
                    _process_wait_flag_append(proc, flag)
    else:
        for proc_name in proc_dict:
            for proc in proc_dict[proc_name]:
                _process_wait_flag_append(proc, flag)
    return all(flag)


def kill_all_logging_processes(proc_dict, nodes, cluster=False):
    """Kill logging processes on all given nodes

    Args:
     proc_dict(dict): Dictonary of all active logging processes
     nodes(list): List of nodes where logging has to be stopped

    Kwargs:
     cluster(bool): True if proc_dict is for a full cluster else False
                    (Default:False)

    Retruns:
     bool: True if processes are completed else False
    """
    # Kill all logging processes
    for server in nodes:
        if not kill_process(server, process_names='memory_and_cpu_logger.py'):
            g.log.error("Unable to kill some of the processes at %s.", server)

    # This will stop the async threads created by run_aysnc() as the proc is
    # already killed.
    ret = wait_for_logging_processes_to_stop(proc_dict, cluster)
    if ret:
        return True
    return False


def create_dataframe_from_csv(node, proc_name, test_name):
    """Creates a dataframe from a given process.

    Args:
     node(str): Node from which csv is to be picked
     proc_name(str): Name of process for which csv is to picked
     test_name(str): Name of the testcase for which CSV

    Returns:
     dataframe: Pandas dataframe if CSV file exits else None
    """
    # Read the csv file generated by memory_and_cpu_logger.py
    ret, raw_data, _ = g.run(node, "cat /root/{}.csv"
                             .format(proc_name))
    if ret:
        return None

    # Split the complete dump to individual lines
    data = raw_data.split("\r\n")
    rows, flag = [], False
    for line in data:
        values = line.split(',')
        if test_name == values[0]:
            # Reset rows if it's the second instance
            if flag:
                rows = []
            flag = True
            continue

        # Pick and append values which have complete entry
        if flag and len(values) == 4:
            rows.append(values)

    # Create a panda dataframe and set the type for columns
    dataframe = pd.DataFrame(rows[1:], columns=rows[0])
    conversion_dict = {'Process ID': int,
                       'CPU Usage': float,
                       'Memory Usage': float}
    dataframe = dataframe.astype(conversion_dict)
    return dataframe


def _get_min_max_mean_median(entrylist):
    """Get the mix, max. mean and median of a list

    Args:
      entrylist(list): List of values to be used

    Returns:
       dict:Result dict generate from list
    """
    result = {}
    result['Min'] = min(entrylist)
    result['Max'] = max(entrylist)
    result['Mean'] = mean(entrylist)
    result['Median'] = median(entrylist)
    return result


def _compute_min_max_mean_median(dataframe, data_dict, process, node,
                                 volume=None, brick=None):
    """Compute min, max, mean and median for a given process

    Args:
     dataframe(panda dataframe): Panda data frame of the csv file
     data_dict(dict): data dict to which info is to be added
     process(str): Name of process for which data is to be computed
     node(str): Node for which min, max, mean and median has to be computed

    Kwargs:
     volume(str): Volume name of the volume for which data is to be computed
     brick(str): Brick path of the brick for which data is to be computed
    """
    if volume and process == 'glusterfs':
        # Create subdict inside dict
        data_dict[node][process][volume] = {}
        for usage in ('CPU Usage', 'Memory Usage'):
            # Create usage subdict
            data_dict[node][process][volume][usage] = {}

            # Clean data and compute values
            cleaned_usage = list(dataframe[usage].dropna())
            out = _get_min_max_mean_median(cleaned_usage)

            # Add values to data_dict
            for key in ('Min', 'Max', 'Mean', 'Median'):
                data_dict[node][process][volume][usage][key] = out[key]

    if volume and brick and process == 'glusterfsd':
        # Create subdict inside dict
        data_dict[node][process][volume] = {}
        data_dict[node][process][volume][brick] = {}
        for usage in ('CPU Usage', 'Memory Usage'):
            # Create usage subdict
            data_dict[node][process][volume][brick][usage] = {}

            # Clean data and compute values
            cleaned_usage = list(dataframe[usage].dropna())
            out = _get_min_max_mean_median(cleaned_usage)

            # Add values to data_dict
            for key in ('Min', 'Max', 'Mean', 'Median'):
                data_dict[node][process][volume][brick][usage][key] = out[key]

    # Compute CPU Uage and Memory Usage for glusterd
    else:
        for usage in ('CPU Usage', 'Memory Usage'):
            # Create uage subdict
            data_dict[node][process][usage] = {}

            # Clean data and compute value
            cleaned_usage = list(dataframe[usage].dropna())
            out = _get_min_max_mean_median(cleaned_usage)

            # Add values to data_dict
            for key in ('Min', 'Max', 'Mean', 'Median'):
                data_dict[node][process][usage][key] = out[key]


def compute_data_usage_stats_on_servers(nodes, test_name):
    """Compute min, max, mean and median for servers

    Args:
     nodes(list): Servers from which data is to be used to compute min, max
                  , mean, mode and median
     test_name(str): Name of testcase for which data has to be processed

    Returns:
     dict: dict of min, max, mean and median for a given process

    NOTE:
     This function has to be always run before cleanup.
    """
    data_dict = {}
    for node in nodes:
        # Get the volume status on the node
        volume_status = get_volume_status(node)
        data_dict[node] = {}
        for process in ('glusterd', 'glusterfs', 'glusterfsd'):

            # Generate a dataframe from the csv file
            dataframe = create_dataframe_from_csv(node, process, test_name)
            if dataframe.empty:
                return {}

            data_dict[node][process] = {}
            if process == 'glusterd':
                # Checking if glusterd is restarted.
                if len(set(dataframe['Process ID'])) > 1:
                    data_dict[node][process]['is_restarted'] = True
                else:
                    data_dict[node][process]['is_restarted'] = False

                # Call function to compute min, max, mean and median
                _compute_min_max_mean_median(dataframe, data_dict, process,
                                             node)
                continue

            # Map volumes to volume process
            for volume in volume_status.keys():
                for proc in volume_status[volume][node].keys():
                    if (proc == 'Self-heal Daemon' and process == 'glusterfs'):
                        # Fetching pid from volume status output and create a
                        # dataframe with the entries of only that pid
                        pid = volume_status[volume][node][proc]['pid']
                        proc_dataframe = dataframe[
                            dataframe['Process ID'] == pid]

                        # Call function to compute min, max, mean
                        # and median
                        _compute_min_max_mean_median(
                            proc_dataframe, data_dict, process, node, volume)

                    if (proc.count('/') >= 2 and process == 'glusterfsd'):
                        # Fetching pid from volume status output and create a
                        # dataframe with the entries of only that pid
                        pid = volume_status[volume][node][proc]['pid']
                        proc_dataframe = dataframe[
                            dataframe['Process ID'] == pid]

                        # Call function to compute min, max, mean and median
                        _compute_min_max_mean_median(
                            proc_dataframe, data_dict, process, node, volume,
                            proc)

    return data_dict


def compute_data_usage_stats_on_clients(nodes, test_name):
    """Compute min, max, mean and median for clients

    Args:
     nodes(list): Clients from which data is to be used to compute min, max
                  , mean, mode and median
     test_name(str): Name of the testcase for which data has to be processed

    Returns:
     dict: dict of min, max, mean and median for a given process
    """
    data_dict = {}
    for node in nodes:
        data_dict[node] = {}
        dataframe = create_dataframe_from_csv(node, 'glusterfs', test_name)
        if dataframe.empty:
            return {}

        data_dict[node]['glusterfs'] = {}
        # Call function to compute min, max, mean and median
        _compute_min_max_mean_median(dataframe, data_dict, 'glusterfs', node)

    return data_dict


def _perform_three_point_check_for_memory_leak(dataframe, node, process, gain,
                                               volume_status=None,
                                               volume=None,
                                               vol_name=None):
    """Perform three point check

    Args:
     dataframe(panda dataframe): Panda dataframe of a given process
     node(str): Node on which memory leak has to be checked
     process(str): Name of process for which check has to be done
     gain(float): Accepted amount of leak for a given testcase in MB

    kwargs:
     volume_status(dict): Volume status output on the give name
     volumne(str):Name of volume for which 3 point check has to be done
     vol_name(str): Name of volume process according to volume status

    Returns:
     bool: True if memory leak instances are observed else False
    """
    # Filter dataframe to be process wise if it's volume specific process
    if process in ('glusterfs', 'glusterfsd'):
        if process == 'glusterfs' and vol_name:
            pid = int(volume_status[volume][node][vol_name]['pid'])
            dataframe = dataframe[dataframe['Process ID'] == pid]

    # Compute usage gain throught the data frame
    memory_increments = list(dataframe['Memory Usage'].diff().dropna())

    # Check if usage is more than accepted amount of leak
    memory_leak_decision_array = np.where(
        dataframe['Memory Usage'].diff().dropna() > gain, True, False)
    instances_of_leak = np.where(memory_leak_decision_array)[0]

    # If memory leak instances are present check if it's reduced
    count_of_leak_instances = len(instances_of_leak)
    if count_of_leak_instances > 0:
        g.log.error('There are %s instances of memory leaks on node %s',
                    count_of_leak_instances, node)
        for instance in instances_of_leak:
            # In cases of last log file entry the below op could throw
            # IndexError which is handled as below.
            try:
                # Check if memory gain had decrease in the consecutive
                # entries, after 2 entry and betwen current and last entry
                if all([memory_increments[instance+1] >
                       memory_increments[instance],
                       memory_increments[instance+2] >
                       memory_increments[instance],
                       (memory_increments[len(memory_increments)-1] >
                        memory_increments[instance])]):
                    return True

            except IndexError:
                # In case of last log file entry rerun the command
                # and check for difference
                g.log.info('Instance at last log entry.')
                if process in ('glusterfs', 'glusterfsd'):
                    cmd = ("ps u -p %s | awk 'NR>1 && $11~/%s$/{print "
                           " $6/1024}'" % (pid, process))
                else:
                    cmd = ("ps u -p `pgrep glusterd` | awk 'NR>1 && $11~/"
                           "glusterd$/{print $6/1024}'")
                ret, out, _ = g.run(node, cmd)
                if ret:
                    g.log.error('Unable to run the command to fetch current '
                                'memory utilization.')
                    continue
                usage_now = float(out.replace('\n', '')[2])
                last_entry = dataframe['Memory Usage'].iloc[-1]

                # Check if current memory usage is higher than last entry
                fresh_diff = last_entry - usage_now
                if fresh_diff > gain and last_entry > fresh_diff:
                    return True
        return False


def check_for_memory_leaks_in_glusterd(nodes, test_name, gain=30.0):
    """Check for memory leaks in glusterd

    Args:
     nodes(list): Servers on which memory leaks have to be checked
     test_name(str): Name of testcase for which memory leaks has to be checked

    Kwargs:
     gain(float): Accepted amount of leak for a given testcase in MB
                  (Default:30)

    Returns:
      bool: True if memory leak was obsevred else False
    """
    is_there_a_leak = []
    for node in nodes:
        dataframe = create_dataframe_from_csv(node, 'glusterd', test_name)
        if dataframe.empty:
            return False

        # Call 3 point check function
        three_point_check = _perform_three_point_check_for_memory_leak(
            dataframe, node, 'glusterd', gain)
        if three_point_check:
            g.log.error("Memory leak observed on node %s in glusterd",
                        node)
        is_there_a_leak.append(three_point_check)

    return any(is_there_a_leak)


def check_for_memory_leaks_in_glusterfs(nodes, test_name, gain=30.0):
    """Check for memory leaks in glusterfs

    Args:
     nodes(list): Servers on which memory leaks have to be checked
     test_name(str): Name of testcase for which memory leaks has to be checked

    Kwargs:
     gain(float): Accepted amount of leak for a given testcase in MB
                  (Default:30)

    Returns:
      bool: True if memory leak was obsevred else False

    NOTE:
     This function should be executed with the volumes present on the cluster
    """
    is_there_a_leak = []
    for node in nodes:
        # Get the volume status on the node
        volume_status = get_volume_status(node)
        dataframe = create_dataframe_from_csv(node, 'glusterfs', test_name)
        if dataframe.empty:
            return False

        for volume in volume_status.keys():
            for process in volume_status[volume][node].keys():
                # Skiping if process isn't Self-heal Deamon
                if process != 'Self-heal Daemon':
                    continue

                # Call 3 point check function
                three_point_check = _perform_three_point_check_for_memory_leak(
                    dataframe, node, 'glusterfs', gain, volume_status, volume,
                    'Self-heal Daemon')
                if three_point_check:
                    g.log.error("Memory leak observed on node %s in shd "
                                "on volume %s", node, volume)
                is_there_a_leak.append(three_point_check)

    return any(is_there_a_leak)


def check_for_memory_leaks_in_glusterfsd(nodes, test_name, gain=30.0):
    """Check for memory leaks in glusterfsd

    Args:
     nodes(list): Servers on which memory leaks have to be checked
     test_name(str): Name of testcase for which memory leaks has to be checked

    Kwargs:
     gain(float): Accepted amount of leak for a given testcase in MB
                  (Default:30)

    Returns:
      bool: True if memory leak was obsevred else False

    NOTE:
     This function should be executed with the volumes present on the cluster.
    """
    is_there_a_leak = []
    for node in nodes:
        # Get the volume status on the node
        volume_status = get_volume_status(node)
        dataframe = create_dataframe_from_csv(node, 'glusterfsd', test_name)
        if dataframe.empty:
            return False

        for volume in volume_status.keys():
            for process in volume_status[volume][node].keys():
                # Skiping if process isn't brick process
                if not process.count('/'):
                    continue

                # Call 3 point check function
                three_point_check = _perform_three_point_check_for_memory_leak(
                    dataframe, node, 'glusterfsd', gain, volume_status, volume,
                    process)
                if three_point_check:
                    g.log.error("Memory leak observed on node %s in brick "
                                " process for brick %s on volume %s", node,
                                process, volume)
                is_there_a_leak.append(three_point_check)

    return any(is_there_a_leak)


def check_for_memory_leaks_in_glusterfs_fuse(nodes, test_name, gain=30.0):
    """Check for memory leaks in glusterfs fuse

    Args:
     nodes(list): Servers on which memory leaks have to be checked
     test_name(str): Name of testcase for which memory leaks has to be checked

    Kwargs:
     gain(float): Accepted amount of leak for a given testcase in MB
                  (Default:30)

    Returns:
      bool: True if memory leak was observed else False

    NOTE:
     This function should be executed when the volume is still mounted.
    """
    is_there_a_leak = []
    for node in nodes:
        # Get the volume status on the node
        dataframe = create_dataframe_from_csv(node, 'glusterfs', test_name)
        if dataframe.empty:
            return False

        # Call 3 point check function
        three_point_check = _perform_three_point_check_for_memory_leak(
            dataframe, node, 'glusterfs', gain)
        if three_point_check:
            g.log.error("Memory leak observed on node %s for client",
                        node)

            # If I/O is constantly running on Clients the memory
            # usage spikes up and stays at a point for long.
            last_entry = dataframe['Memory Usage'].iloc[-1]
            cmd = ("ps u -p `pidof glusterfs` | "
                   "awk 'NR>1 && $11~/glusterfs$/{print"
                   " $6/1024}'")
            ret, out, _ = g.run(node, cmd)
            if ret:
                g.log.error('Unable to run the command to fetch current '
                            'memory utilization.')
                continue

            if float(out) != last_entry:
                if float(out) > last_entry:
                    is_there_a_leak.append(True)
                    continue

        is_there_a_leak.append(False)

    return any(is_there_a_leak)


def _check_for_oom_killers(nodes, process, oom_killer_list):
    """Checks for OOM killers for a specific process

    Args:
     nodes(list): Nodes on which OOM killers have to be checked
     process(str): Process for which OOM killers have to be checked
     oom_killer_list(list): A list in which the presence of
                            OOM killer has to be noted
    """
    cmd = ("grep -i 'killed process' /var/log/messages* "
           "| grep -w '{}'".format(process))
    ret_codes = g.run_parallel(nodes, cmd)
    for key in ret_codes.keys():
        ret, out, _ = ret_codes[key]
        if not ret:
            g.log.error('OOM killer observed on %s for %s', key, process)
            g.log.error(out)
            oom_killer_list.append(True)
        else:
            oom_killer_list.append(False)


def check_for_oom_killers_on_servers(nodes):
    """Check for OOM killers on servers

    Args:
     nodes(list): Servers on which OOM kills have to be checked

    Returns:
     bool: True if OOM killers are present on any server else False
    """
    oom_killer_list = []
    for process in ('glusterfs', 'glusterfsd', 'glusterd'):
        _check_for_oom_killers(nodes, process, oom_killer_list)
    return any(oom_killer_list)


def check_for_oom_killers_on_clients(nodes):
    """Check for OOM killers on clients

    Args:
     nodes(list): Clients on which OOM kills have to be checked

    Returns:
     bool: True if OOM killers are present on any client else false
    """
    oom_killer_list = []
    _check_for_oom_killers(nodes, 'glusterfs', oom_killer_list)
    return any(oom_killer_list)


def _check_for_cpu_usage_spikes(dataframe, node, process, threshold,
                                volume_status=None, volume=None,
                                vol_name=None):
    """Check for cpu spikes for a given process

    Args:
     dataframe(panda dataframe): Panda dataframe of a given process
     node(str): Node on which cpu spikes has to be checked
     process(str): Name of process for which check has to be done
     threshold(int): Accepted amount of 100% CPU usage instances

    kwargs:
     volume_status(dict): Volume status output on the give name
     volume(str):Name of volume for which check has to be done
     vol_name(str): Name of volume process according to volume status

    Returns:
     bool: True if number of instances more than threshold else False
    """
    # Filter dataframe to be process wise if it's volume specific process
    if process in ('glusterfs', 'glusterfsd'):
        pid = int(volume_status[volume][node][vol_name]['pid'])
        dataframe = dataframe[dataframe['Process ID'] == pid]

    # Check if usage is more than accepted amount of leak
    cpu_spike_decision_array = np.where(
        dataframe['CPU Usage'].dropna() == 100.0, True, False)
    instances_of_spikes = np.where(cpu_spike_decision_array)[0]

    return bool(len(instances_of_spikes) > threshold)


def check_for_cpu_usage_spikes_on_glusterd(nodes, test_name, threshold=3):
    """Check for CPU usage spikes on glusterd

    Args:
     nodes(list): Servers on which memory leaks have to be checked
     test_name(str): Name of testcase for which memory leaks has to be checked

    Kwargs:
     threshold(int): Accepted amount of instances of 100% CPU usage
                    (Default:3)

    Returns:
      bool: True if CPU spikes are more than threshold else False
    """
    is_there_a_spike = []
    for node in nodes:
        dataframe = create_dataframe_from_csv(node, 'glusterd', test_name)
        if dataframe.empty:
            return False

        # Call function to check for cpu spikes
        cpu_spikes = _check_for_cpu_usage_spikes(
            dataframe, node, 'glusterd', threshold)
        if cpu_spikes:
            g.log.error("CPU usage spikes observed more than "
                        "threshold %d on node %s for glusterd",
                        threshold, node)
        is_there_a_spike.append(cpu_spikes)

    return any(is_there_a_spike)


def check_for_cpu_usage_spikes_on_glusterfs(nodes, test_name, threshold=3):
    """Check for CPU usage spikes on glusterfs

    Args:
     nodes(list): Servers on which memory leaks have to be checked
     test_name(str): Name of testcase for which memory leaks has to be checked

    Kwargs:
     threshold(int): Accepted amount of instances of 100% CPU usage
                    (Default:3)

    Returns:
      bool: True if CPU spikes are more than threshold else False

    NOTE:
     This function should be exuected with the volumes present on the cluster.
    """
    is_there_a_spike = []
    for node in nodes:
        # Get the volume status on the node
        volume_status = get_volume_status(node)
        dataframe = create_dataframe_from_csv(node, 'glusterfs', test_name)
        if dataframe.empty:
            return False

        for volume in volume_status.keys():
            for process in volume_status[volume][node].keys():
                # Skiping if process isn't Self-heal Deamon
                if process != 'Self-heal Daemon':
                    continue

                # Call function to check for cpu spikes
                cpu_spikes = _check_for_cpu_usage_spikes(
                    dataframe, node, 'glusterfs', threshold, volume_status,
                    volume, 'Self-heal Daemon')
                if cpu_spikes:
                    g.log.error("CPU usage spikes observed more than "
                                "threshold %d on node %s on volume %s for shd",
                                threshold, node, volume)
                is_there_a_spike.append(cpu_spikes)

    return any(is_there_a_spike)


def check_for_cpu_usage_spikes_on_glusterfsd(nodes, test_name, threshold=3):
    """Check for CPU usage spikes in glusterfsd

    Args:
     nodes(list): Servers on which memory leaks have to be checked
     test_name(str): Name of testcase for which memory leaks has to be checked

    Kwargs:
     threshold(int): Accepted amount of instances of 100% CPU usage
                    (Default:3)

    Returns:
      bool: True if CPU spikes are more than threshold else False

    NOTE:
     This function should be exuected with the volumes present on the cluster.
    """
    is_there_a_spike = []
    for node in nodes:
        # Get the volume status on the node
        volume_status = get_volume_status(node)
        dataframe = create_dataframe_from_csv(node, 'glusterfsd', test_name)
        if dataframe.empty:
            return False

        for volume in volume_status.keys():
            for process in volume_status[volume][node].keys():
                # Skiping if process isn't brick process
                if process in ('Self-heal Daemon', 'Quota Daemon'):
                    continue

                # Call function to check for cpu spikes
                cpu_spikes = _check_for_cpu_usage_spikes(
                    dataframe, node, 'glusterfsd', threshold, volume_status,
                    volume, process)
                if cpu_spikes:
                    g.log.error("CPU usage spikes observed more than "
                                "threshold %d on node %s on volume %s for "
                                "brick process %s",
                                threshold, node, volume, process)
                is_there_a_spike.append(cpu_spikes)

    return any(is_there_a_spike)


def check_for_cpu_usage_spikes_on_glusterfs_fuse(nodes, test_name,
                                                 threshold=3):
    """Check for CPU usage spikes on glusterfs fuse

    Args:
     nodes(list): Servers on which memory leaks have to be checked
     test_name(str): Name of testcase for which memory leaks has to be checked

    Kwargs:
     threshold(int): Accepted amount of instances of 100% CPU usage
                    (Default:3)

    Returns:
      bool: True if CPU spikes are more than threshold else False

    NOTE:
     This function should be executed when the volume is still mounted.
    """
    is_there_a_spike = []
    for node in nodes:
        # Get the volume status on the node
        dataframe = create_dataframe_from_csv(node, 'glusterfs', test_name)
        if dataframe.empty:
            return False

        # Call function to check for cpu spikes
        cpu_spikes = _check_for_cpu_usage_spikes(
            dataframe, node, 'glusterfs', threshold)
        if cpu_spikes:
            g.log.error("CPU usage spikes observed more than "
                        "threshold %d on node %s for client",
                        threshold, node)
        is_there_a_spike.append(cpu_spikes)

    return any(is_there_a_spike)
