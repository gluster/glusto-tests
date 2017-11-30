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

"""
    Description: Helper library for gluster modules.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_info
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
import re
import time
from collections import OrderedDict
import tempfile
import subprocess
import random

ONE_GB_BYTES = 1073741824.0


def append_string_to_file(mnode, filename, str_to_add_in_file,
                          user="root"):
    """Appends the given string in the file.

    Example:
        append_string_to_file("abc.def.com", "/var/log/messages",
                           "test_1_string")

    Args:
        mnode (str): Node on which cmd has to be executed.
        filename (str): absolute file path to append the string
        str_to_add_in_file (str): string to be added in the file,
            which is used as a start and stop string for parsing
            the file in search_pattern_in_file().

    Kwargs:
        user (str): username. Defaults to 'root' user.

    Returns:
        True, on success, False otherwise
    """
    try:
        conn = g.rpyc_get_connection(mnode, user=user)
        if conn is None:
            g.log.error("Unable to get connection to 'root' of node %s"
                        " in append_string_to_file()" % mnode)
            return False

        with conn.builtin.open(filename, 'a') as _filehandle:
            _filehandle.write(str_to_add_in_file)

        return True
    except IOError:
        g.log.error("Exception occured while adding string to "
                    "file %s in append_string_to_file()", filename)
        return False
    finally:
        g.rpyc_close_connection(host=mnode, user=user)


def search_pattern_in_file(mnode, search_pattern, filename, start_str_to_parse,
                           end_str_to_parse):
    """checks if the given search pattern exists in the file
       in between 'start_str_to_parse' and 'end_str_to_parse' string.

    Example:
        search_pattern = r'.*scrub.*'
        search_log("abc.def.com", search_pattern, "/var/log/messages",
                    "start_pattern", "end_pattern")

    Args:
        mnode (str): Node on which cmd has to be executed.
        search_pattern (str): regex string to be matched in the file
        filename (str): absolute file path to search given string
        start_str_to_parse (str): this will be as start string in the
            file from which this method will check
            if the given search string is present.
        end_str_to_parse (str): this will be as end string in the
            file whithin which this method will check
            if the given search string is present.

    Returns:
        True, if search_pattern is present in the file
        False, otherwise
    """

    cmd = ("awk '{a[NR]=$0}/" + start_str_to_parse + "/{s=NR}/" +
           end_str_to_parse + "/{e=NR}END{for(i=s;i<=e;++i)print "
           "a[i]}' " + filename)

    ret, out, err = g.run(mnode, cmd)
    if ret != 0:
        g.log.error("Failed to match start and end pattern in file"
                    % filename)
        return False

    if not re.search(search_pattern, str(out), re.S):
        g.log.error("file %s did not have the expected message"
                    % filename)
        return False

    return True


def calculate_checksum(mnode, file_list, chksum_type='sha256sum'):
    """This module calculates given checksum for the given file list

    Example:
        calculate_checksum("abc.com", [file1, file2])

    Args:
        mnode (str): Node on which cmd has to be executed.
        file_list (list): absolute file names for which checksum
            to be calculated

    Kwargs:
        chksum_type (str): type of the checksum algorithm.
            Defaults to sha256sum

    Returns:
        NoneType: None if command execution fails, parse errors.
        dict: checksum value for each file in the given file list
    """

    cmd = chksum_type + " %s" % ' '.join(file_list)
    ret = g.run(mnode, cmd)
    if ret[0] != 0:
        g.log.error("Failed to execute checksum command in server %s"
                    % mnode)
        return None

    checksum_dict = {}
    for line in ret[1].split('\n')[:-1]:
        match = re.search(r'^(\S+)\s+(\S+)', line.strip())
        if match is None:
            g.log.error("checksum output is not in expected format")
            return None

        checksum_dict[match.group(2)] = match.group(1)

    return checksum_dict


def get_extended_attributes_info(mnode, file_list, encoding='hex',
                                 attr_name=''):
    """This module gets extended attribute info for the given file list

    Example:
        get_extended_attributes_info("abc.com", [file1, file2])

    Args:
        mnode (str): Node on which cmd has to be executed.
        file_list (list): absolute file names for which extended
            attributes to be fetched

    Kwargs:
        encoding (str): encoding format
        attr_name (str): extended attribute name

    Returns:
        NoneType: None if command execution fails, parse errors.
        dict: extended attribute for each file in the given file list
    """

    if attr_name == '':
        cmd = "getfattr -d -m . -e %s %s" % (encoding, ' '.join(file_list))
    else:
        cmd = ("getfattr -d -m . -e %s -n %s %s"
               % (encoding, attr_name, ' '.join(file_list)))

    ret = g.run(mnode, cmd)
    if ret[0] != 0:
        g.log.error("Failed to execute getfattr command in server %s"
                    % mnode)
        return None

    attr_dict = {}
    for each_attr in ret[1].split('\n\n')[:-1]:
        for line in each_attr.split('\n'):
            if line.startswith('#'):
                match = re.search(r'.*file:\s(\S+).*', line)
                if match is None:
                    g.log.error("getfattr output is not in expected format")
                    return None
                key = "/" + match.group(1)
                attr_dict[key] = {}
            else:
                output = line.split('=')
                attr_dict[key][output[0]] = output[1]
    return attr_dict


def get_pathinfo(mnode, filename, volname):
    """This module gets filepath of the given file in gluster server.

    Example:
        get_pathinfo(mnode, "file1", "testvol")

    Args:
        mnode (str): Node on which cmd has to be executed.
        filename (str): relative path of file
        volname (str): volume name

    Returns:
        NoneType: None if command execution fails, parse errors.
        list: file path for the given file in gluster server
    """

    mount_point = tempfile.mkdtemp()

    # Performing glusterfs mount because only with glusterfs mount
    # the file location in gluster server can be identified
    ret, _, _ = mount_volume(volname, mtype='glusterfs',
                             mpoint=mount_point,
                             mserver=mnode,
                             mclient=mnode)
    if ret != 0:
        g.log.error("Failed to do gluster mount on volume %s to fetch"
                    "pathinfo from server %s"
                    % (volname, mnode))
        return None

    filename = mount_point + '/' + filename
    attr_name = 'trusted.glusterfs.pathinfo'
    output = get_extended_attributes_info(mnode, [filename],
                                          attr_name=attr_name)
    if output is None:
        g.log.error("Failed to get path info for %s" % filename)
        return None

    pathinfo = output[filename][attr_name]

    umount_volume(mnode, mount_point)
    g.run(mnode, "rm -rf " + mount_point)

    return re.findall(".*?POSIX.*?:(\S+)\>", pathinfo)


def list_files(mnode, dir_path, parse_str="", user="root"):
    """This module list files from the given file path

    Example:
        list_files("/root/dir1/")

    Args:
        mnode (str): Node on which cmd has to be executed.
        dir_path (str): directory path name

    Kwargs:
        parse_str (str): sub string of the filename to be fetched
        user (str): username. Defaults to 'root' user.

    Returns:
        NoneType: None if command execution fails, parse errors.
        list: files with absolute name
    """

    try:
        conn = g.rpyc_get_connection(mnode, user=user)
        if conn is None:
            g.log.error("Unable to get connection to 'root' of node %s"
                        % mnode)
            return None

        filepaths = []
        for root, directories, files in conn.modules.os.walk(dir_path):
            for filename in files:
                if parse_str != "":
                    if parse_str in filename:
                        filepath = conn.modules.os.path.join(root, filename)
                        filepaths.append(filepath)
                else:
                    filepath = conn.modules.os.path.join(root, filename)
                    filepaths.append(filepath)
        return filepaths
    except StopIteration:
        g.log.error("Exception occured in list_files()")
        return None

    finally:
        g.rpyc_close_connection(host=mnode, user=user)


def get_servers_bricks_dict(servers, servers_info):
    """This module returns servers_bricks dictionary.
    Args:
        servers (str|list): A server|List of servers for which we
            need the list of bricks available on it.
        servers_info (dict): dict of server info of each servers
    Returns:
        OrderedDict: key - server
              value - list of bricks
    Example:
        get_servers_bricks_dict(g.config['servers'], g.config['servers_info'])
    """
    servers_bricks_dict = OrderedDict()
    if isinstance(servers, str):
        servers = [servers]
    for server in servers:
        server_info = servers_info[server]
        brick_root = server_info["brick_root"]
        ret, out, err = g.run(server, "cat /proc/mounts | grep %s"
                              " | awk '{ print $2}'" % brick_root)
        if ret != 0:
            g.log.error("bricks not available on %s" % server)
        else:
            servers_bricks_dict[server] = out.strip().split("\n")

    for key, value in servers_bricks_dict.items():
        value.sort()

    return servers_bricks_dict


def get_servers_used_bricks_dict(mnode, servers):
    """This module returns servers_used_bricks dictionary.
       This information is fetched from gluster volume info command.
    Args:
        servers (str|list): A server|List of servers for which we need the
            list of unused bricks on it.
        mnode (str): The node on which gluster volume info command has
            to be executed.
    Returns:
        OrderedDict: key - server
              value - list of used bricks
                      or empty list(if all bricks are free)
    Example:
        get_servers_used_bricks_dict(g.config['servers'][0]['host'],
                                     g.config['servers'])
    """
    if isinstance(servers, str):
        servers = [servers]

    servers_used_bricks_dict = OrderedDict()
    for server in servers:
        servers_used_bricks_dict[server] = []

    ret, out, err = g.run(mnode, "gluster volume info | egrep "
                          "\"^Brick[0-9]+\" | grep -v \"ss_brick\"")
    if ret != 0:
        g.log.error("error in getting bricklist using gluster v info")
    else:
        list1 = list2 = []
        list1 = out.strip().split('\n')
        for item in list1:
            x = re.search(':(.*)/(.*)', item)
            list2 = x.group(1).strip().split(':')
            if list2[0] in servers_used_bricks_dict:
                value = servers_used_bricks_dict[list2[0]]
                value.append(list2[1])
            else:
                servers_used_bricks_dict[list2[0]] = [list2[1]]

    for key, value in servers_used_bricks_dict.items():
        value.sort()

    return servers_used_bricks_dict


def get_servers_unused_bricks_dict(mnode, servers, servers_info):
    """This module returns servers_unused_bricks dictionary.
    Gets a list of unused bricks for each server by using functions,
    get_servers_bricks_dict() and get_servers_used_bricks_dict()
    Args:
        mnode (str): The node on which gluster volume info command has
            to be executed.
        servers (str|list): A server|List of servers for which we need the
            list of unused bricks available on it.
        servers_info (dict): dict of server info of each servers
     Returns:
        OrderedDict: key - server
              value - list of unused bricks
    Example:
        get_servers_unused_bricks_dict(g.config['servers'][0]['host'],
                                       g.config['servers'],
                                       g.config['servers_info'])
    """
    if isinstance(servers, str):
        servers = [servers]
    dict1 = get_servers_bricks_dict(servers, servers_info)
    dict2 = get_servers_used_bricks_dict(mnode, servers)
    servers_unused_bricks_dict = OrderedDict()
    for key, value in dict1.items():
        if key in dict2:
            unused_bricks = list(set(value) - set(dict2[key]))
            servers_unused_bricks_dict[key] = unused_bricks
        else:
            servers_unused_bricks_dict[key] = value

    for key, value in servers_unused_bricks_dict.items():
        value.sort()

    return servers_unused_bricks_dict


def form_bricks_list(mnode, volname, number_of_bricks, servers, servers_info):
    """Forms bricks list for create-volume/add-brick given the num_of_bricks
        servers and servers_info.

    Args:
        mnode (str): The node on which the command has to be run.
        volname (str): Volume name for which we require brick-list.
        number_of_bricks (int): The number of bricks for which brick list
            has to be created.
        servers (str|list): A server|List of servers from which the bricks
            needs to be selected for creating the brick list.
        servers_info (dict): dict of server info of each servers.

    Returns:
        list - List of bricks to use with volume-create/add-brick
        None - if number_of_bricks is greater than unused bricks.

    Example:
        form_bricks_path(g.config['servers'](0), "testvol", 6,
                         g.config['servers'], g.config['servers_info'])
    """
    if isinstance(servers, str):
        servers = [servers]
    dict_index = 0
    bricks_list = []

    servers_unused_bricks_dict = get_servers_unused_bricks_dict(mnode, servers,
                                                                servers_info)
    num_of_unused_bricks = 0
    for each_server_unused_bricks_list in servers_unused_bricks_dict.values():
        num_of_unused_bricks = (num_of_unused_bricks +
                                len(each_server_unused_bricks_list))

    if num_of_unused_bricks < number_of_bricks:
        g.log.error("Not enough bricks available for creating the bricks")
        return None

    brick_index = 0
    vol_info_dict = get_volume_info(mnode, volname)
    if vol_info_dict:
        brick_index = int(vol_info_dict[volname]['brickCount'])

    for num in range(brick_index, brick_index + number_of_bricks):
        # current_server is the server from which brick path will be created
        current_server = servers_unused_bricks_dict.keys()[dict_index]
        current_server_unused_bricks_list = (
            servers_unused_bricks_dict.values()[dict_index])
        brick_path = ''
        if current_server_unused_bricks_list:
            brick_path = ("%s:%s/%s_brick%s" %
                          (current_server,
                           current_server_unused_bricks_list[0], volname, num))
            bricks_list.append(brick_path)

            # Remove the added brick from the current_server_unused_bricks_list
            servers_unused_bricks_dict.values()[dict_index].pop(0)

        if dict_index < len(servers_unused_bricks_dict) - 1:
            dict_index = dict_index + 1
        else:
            dict_index = 0

    return bricks_list


def is_rhel6(servers):
    """Function to get whether the server is RHEL-6

    Args:
    servers (str|list): A server|List of servers hosts to know the RHEL Version

    Returns:
    bool:Returns True, if its RHEL-6 else returns false
    """
    if isinstance(servers, str):
        servers = [servers]

    results = g.run_parallel(servers, "cat /etc/redhat-release")
    rc = True
    for server, ret_values in results.iteritems():
        retcode, out, err = ret_values
        if retcode != 0:
            g.log.error("Unable to get the RHEL version on server %s" %
                        (server))
            rc = False
        if retcode == 0 and "release 6" not in out:
            g.log.error("Server %s is not RHEL-6" % (server))
            rc = False
    return rc


def is_rhel7(servers):
    """Function to get whether the server is RHEL-7

    Args:
    servers (str|list): A server|List of servers hosts to know the RHEL Version

    Returns:
    bool:Returns True, if its RHEL-7 else returns false
    """
    if isinstance(servers, str):
        servers = [servers]

    results = g.run_parallel(servers, "cat /etc/redhat-release")
    rc = True
    for server, ret_values in results.iteritems():
        retcode, out, err = ret_values
        if retcode != 0:
            g.log.error("Unable to get the RHEL version on server %s" %
                        (server))
            rc = False
        if retcode == 0 and "release 7" not in out:
            g.log.error("Server %s is not RHEL-7" % (server))
            rc = False
    return rc


def get_disk_usage(mnode, path, user="root"):
    """
    This module gets disk usage of the given path

    Args:
        path (str): path for which disk usage to be calculated
        conn (obj): connection object of the remote node

    Kwargs:
        user (str): username

    Returns:
        dict: disk usage in dict format on success
        None Type, on failure

    Example:
        get_disk_usage("abc.com", "/mnt/glusterfs")
    """

    inst = random.randint(10, 100)
    conn = g.rpyc_get_connection(mnode, user=user, instance=inst)
    if conn is None:
        g.log.error("Failed to get rpyc connection")
        return None
    cmd = 'stat -f ' + path
    p = conn.modules.subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
    out, err = p.communicate()
    ret = p.returncode
    if ret != 0:
        g.log.error("Failed to execute stat command")
        return None

    g.rpyc_close_connection(host=mnode, user=user, instance=inst)
    res = ''.join(out)
    match = re.match(r'.*Block size:\s(\d+).*Blocks:\sTotal:\s(\d+)\s+?'
                     r'Free:\s(\d+)\s+?Available:\s(\d+).*Inodes:\s'
                     r'Total:\s(\d+)\s+?Free:\s(\d+)', res, re.S)
    if match is None:
        g.log.error("Regex mismatch in get_disk_usage()")
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
    usage_info['used_percent_inode'] = (100 - (100.0 *
                                               usage_info['free_inode'] /
                                               usage_info['total_inode']))
    usage_info['used'] = usage_info['total'] - usage_info['free']
    usage_info['used_inode'] = (usage_info['total_inode'] -
                                usage_info['free_inode'])
    return usage_info


def get_disk_used_percent(mnode, dirname):
    """
    Module to get disk used percent

    Args:
       mnode (str): node on which cmd has to be executed
       dirname (str): absolute path of directory

    Returns:
        str: used percent for given directory
        None Type, on failure

    Example:
        get_disk_used_percent("abc.com", "/mnt/glusterfs")

    """

    output = get_disk_usage(mnode, dirname)
    if output is None:
        g.log.error("Failed to get disk used percent for %s"
                    % dirname)
        return None
    return output['used_percent']


def check_if_dir_is_filled(mnode, dirname, percent_to_fill,
                           timeout=3600):
    """
    Module to check if the directory is filled with given percentage.

    Args:
        mnode (str): node to check if directory is filled
        dirname (str): absolute path of directory
        percent_to_fill (int): percentage to fill the volume

    Kwargs:
        timeout (int): overall timeout value for wait till the dir fills
            with given percentage

    Returns:
        bool: True, if volume is filled with given percent, False otherwise

    Example:
        check_if_dir_is_filled("abc.com", "/mnt/glusterfs", 10)
    """
    flag = 0
    count = 0
    while count < timeout:
        output = get_disk_usage(mnode, dirname)
        used = output['used_percent']

        if int(percent_to_fill) > int(used):
            g.log.info("Directory %s used percent: %s"
                       % (dirname, used))
            if int(percent_to_fill) <= int(used):
                flag = 1
                g.rpyc_close_connection(host=mnode)
                break
            time.sleep(5)
            count = count + 5
        else:
            g.log.info("Diectory %s is filled with given percent already"
                       % dirname)
            g.rpyc_close_connection(host=mnode)
            flag = 1
            break

    if flag:
        g.log.info("Directory is filled with given percentage")
        return True
    else:
        g.log.info("Timeout reached before filling directory with given"
                   " percentage")
        return True
    return False


def install_epel(servers):
    """
    Module to install epel in rhel/centos/fedora systems.

    Args:
        servers (str|list): A server|List of servers in which epel
            to be installed.

    Returns:
        bool: True, if epel is installed successfully, False otherwise

    Example:
        install_epel(["abc.com", "def.com"])
    """
    if isinstance(servers, str):
        servers = [servers]

    rt = True
    results = g.run_parallel(servers, "yum list installed epel-release")
    for server in servers:
        if results[server][0] != 0:
            ret, out, _ = g.run(server,
                                "cat /etc/redhat-release")
            if ret != 0:
                g.log.error("Failed to recognize OS release")
                rt = False
            release_string = out
            if "release 5" in release_string:
                ret, _, _ = g.run(server,
                                  "yum -y install http://dl.fedoraproject.org/"
                                  "pub/epel/epel-release-latest-5.noarch.rpm")
                if ret != 0:
                    g.log.error("Epel install failed")
                    rt = False
            elif "release 6" in release_string:
                ret, _, _ = g.run(server,
                                  "yum -y install http://dl.fedoraproject.org/"
                                  "pub/epel/epel-release-latest-6.noarch.rpm")
                if ret != 0:
                    g.log.error("Epel install failed")
                    rt = False
            elif (("release 7" in release_string) or
                  ("Fedora" in release_string)):
                ret, _, _ = g.run(server,
                                  "yum -y install http://dl.fedoraproject.org/"
                                  "pub/epel/epel-release-latest-7.noarch.rpm")
                if ret != 0:
                    g.log.error("Epel install failed")
                    rt = False
            else:
                g.log.error("Unrecognized release. Skipping epel install")
                rt = False
    return rt


def inject_msg_in_logs(nodes, log_msg, list_of_dirs=None, list_of_files=None):
    """Injects the message to all log files under all dirs specified on nodes.

    Args:
        nodes (str|list): A server|List of nodes on which message has to be
            injected to logs
        log_msg (str): Message to be injected
        list_of_dirs (list): List of dirs to inject message on log files.
        list_of_files (list): List of files to inject message.

    Returns:
        bool: True if successfully injected msg on all log files.
    """
    if isinstance(nodes, str):
        nodes = [nodes]

    if list_of_dirs is None:
        list_of_dirs = ""

    if isinstance(list_of_dirs, list):
        list_of_dirs = ' '.join(list_of_dirs)

    if list_of_files is None:
        list_of_files = ''

    if isinstance(list_of_files, list):
        list_of_files = ' '.join(list_of_files)

    inject_msg_on_dirs = ""
    inject_msg_on_files = ""
    if list_of_dirs:
        inject_msg_on_dirs = (
            "for dir in %s ; do "
            "for file in `find ${dir} -type f -name '*.log'`; do "
            "echo \"%s\" >> ${file} ; done ;"
            "done; " % (list_of_dirs, log_msg))
    if list_of_files:
        inject_msg_on_files = ("for file in %s ; do "
                               "echo \"%s\" >> ${file} ; done; " %
                               (list_of_files, log_msg))

    cmd = inject_msg_on_dirs + inject_msg_on_files

    results = g.run_parallel(nodes, cmd)

    _rc = True
    # Check for return status
    for host in results:
        ret, _, _ = results[host]
        if ret != 0:
            g.log.error("Failed to inject log message '%s' in dirs '%s', "
                        "in files '%s',  on node'%s'",
                        log_msg, list_of_dirs, list_of_files, host)
            _rc = False
    return _rc
