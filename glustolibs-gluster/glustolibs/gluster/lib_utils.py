#!/usr/bin/env python
#  Copyright (C) 2015-2021  Red Hat, Inc. <http://www.redhat.com>
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
    cmd = "echo '{0}' >> {1}".format(str_to_add_in_file,
                                     filename)
    ret, out, err = g.run(mnode, cmd, user)
    if ret or out or err:
        g.log.error("Unable to append string '{0}' to file "
                    "'{1}' on node {2} using user {3}"
                    .format(str_to_add_in_file, filename,
                            mnode, user))
        return False
    return True


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
            file within which this method will check
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

    return re.findall(r".*?POSIX.*?:(\S+)\>", pathinfo)


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
    if parse_str == "":
        cmd = "find {0} -type f".format(dir_path)
    else:
        cmd = "find {0} -type f | grep {1}".format(dir_path,
                                                   parse_str)
    ret, out, err = g.run(mnode, cmd, user)
    if ret or err:
        g.log.error("Unable to get the list of files on path "
                    "{0} on node {1} using user {2} due to error {3}"
                    .format(dir_path, mnode, user, err))
        return None
    file_list = out.split('\n')
    return file_list[0:len(file_list)-1]


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
    if not isinstance(servers, list):
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

    for key, value in list(servers_bricks_dict.items()):
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
    if not isinstance(servers, list):
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

    for key, value in list(servers_used_bricks_dict.items()):
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
    if not isinstance(servers, list):
        servers = [servers]
    dict1 = get_servers_bricks_dict(servers, servers_info)
    dict2 = get_servers_used_bricks_dict(mnode, servers)
    servers_unused_bricks_dict = OrderedDict()
    for key, value in list(dict1.items()):
        if key in dict2:
            unused_bricks = list(set(value) - set(dict2[key]))
            if unused_bricks:
                servers_unused_bricks_dict[key] = unused_bricks
        else:
            servers_unused_bricks_dict[key] = value

    for key, value in list(servers_unused_bricks_dict.items()):
        value.sort()

    return servers_unused_bricks_dict


def form_bricks_list(mnode, volname, number_of_bricks, servers, servers_info,
                     dirname=None):
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

    kwargs:
        dirname (str): Name of the directory for glusterfs brick

    Returns:
        list - List of bricks to use with volume-create/add-brick
        None - if number_of_bricks is greater than unused bricks.

    Example:
        form_bricks_path(g.config['servers'](0), "testvol", 6,
                         g.config['servers'], g.config['servers_info'])
    """
    if not isinstance(servers, list):
        servers = [servers]
    dict_index = 0
    bricks_list = []

    servers_unused_bricks_dict = get_servers_unused_bricks_dict(mnode, servers,
                                                                servers_info)
    num_of_unused_bricks = 0
    for each_server_unused_bricks_list in list(
            servers_unused_bricks_dict.values()):
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
        current_server = list(servers_unused_bricks_dict.keys())[dict_index]
        current_server_unused_bricks_list = (
            list(servers_unused_bricks_dict.values())[dict_index])
        brick_path = ''
        if current_server_unused_bricks_list:
            if dirname and (" " not in dirname):
                brick_path = ("%s:%s/%s_brick%s" %
                              (current_server,
                               current_server_unused_bricks_list[0], dirname,
                               num))
                bricks_list.append(brick_path)
            else:
                brick_path = ("%s:%s/%s_brick%s" %
                              (current_server,
                               current_server_unused_bricks_list[0], volname,
                               num))
                bricks_list.append(brick_path)

            # Remove the added brick from the current_server_unused_bricks_list
            list(servers_unused_bricks_dict.values())[dict_index].pop(0)

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
    if not isinstance(servers, list):
        servers = [servers]

    results = g.run_parallel(servers, "cat /etc/redhat-release")
    rc = True
    for server, ret_values in list(results.items()):
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
    if not isinstance(servers, list):
        servers = [servers]

    results = g.run_parallel(servers, "cat /etc/redhat-release")
    rc = True
    for server, ret_values in list(results.items()):
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
    cmd = 'stat -f {0}'.format(path)
    ret, out, err = g.run(mnode, cmd, user)
    if ret:
        g.log.error("Unable to get stat of path {0} on node {1} "
                    "using user {2} due to error {3}".format(path, mnode,
                                                             user, err))
        return None
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
    info = dict(list(zip(keys, val)))
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
    if not isinstance(servers, list):
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
    if not isinstance(nodes, list):
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


def is_core_file_created(nodes, testrun_timestamp,
                         paths=['/', '/var/log/core',
                                '/tmp', '/var/crash', '~/']):
    '''
    Listing directories and files in "/", /var/log/core, /tmp,
    "/var/crash", "~/" directory for checking if the core file created or not

    Args:

    nodes(list):
        List of nodes need to pass from test method
    testrun_timestamp:
        This time stamp need to pass from test method
        test case running started time, time format is EPOCH
        time format, use below command for getting timestamp
        of test case 'date +%s'
    paths(list):
        By default core file will be verified in "/","/tmp",
        "/var/log/core", "/var/crash", "~/"
       If test case need to verify core file in specific path,
       need to pass path from test method
    '''
    count = 0
    cmd_list = []
    for path in paths:
        cmd = ' '.join(['cd', path, '&&', 'ls', 'core*'])
        cmd_list.append(cmd)

    # Checks for core file in "/", "/var/log/core", "/tmp" "/var/crash",
    # "~/" directory
    for node in nodes:
        ret, logfiles, err = g.run(node, 'grep -r "time of crash" '
                                         '/var/log/glusterfs/')
        if ret == 0:
            g.log.error(" Seems like there was a crash, kindly check "
                        "the logfiles, even if you don't see a core file")
            for logfile in logfiles.strip('\n').split('\n'):
                g.log.error("Core was found in %s " % logfile.split(':')[0])
        for cmd in cmd_list:
            ret, out, _ = g.run(node, cmd)
            g.log.info("storing all files and directory names into list")
            dir_list = re.split(r'\s+', out)

            # checking for core file created or not in "/"
            # "/var/log/core", "/tmp" directory
            g.log.info("checking core file created or not")
            for file1 in dir_list:
                if (re.search(r'\bcore\.[\S]+\b', file1)):
                    file_path_list = re.split(r'[\s]+', cmd)
                    file_path = file_path_list[1] + '/' + file1
                    time_cmd = 'stat ' + '-c ' + '%X ' + file_path
                    ret, file_timestamp, _ = g.run(node, time_cmd)
                    file_timestamp = file_timestamp.strip()
                    if(file_timestamp > testrun_timestamp):
                        count += 1
                        g.log.error("New core file was created and found  "
                                    "at %s " % file1)
                    else:
                        g.log.info("Old core file Found")
    # return the status of core file
    if (count >= 1):
        g.log.error("Core file created glusterd crashed")
        return False
    else:
        g.log.info("No core files found ")
        return True


def remove_service_from_firewall(nodes, firewall_service, permanent=False):
    """Removing services from firewall on nodes
    This library only for RHEL7, for RHEL6 not required
        Args:
            nodes(list|str): List of server on which firewalls services to be
            removed
            firewall_service(list|str): List of firewall services to be removed
            permanent(boolean): True|False
        Return:
            bool: True|False(Firewall removed or Failed)
    """

    if not isinstance(nodes, list):
        nodes = [nodes]

    if not isinstance(firewall_service, list):
        firewall_service = [firewall_service]

    _rc = True
    if not is_rhel6(nodes):
        for service in firewall_service:
            cmd = ("firewall-cmd --zone=public " + "--remove-service=" +
                   service)
            results = g.run_parallel(nodes, cmd)
            # Check for return status
            for host in results:
                ret, _, _ = results[host]
                if ret != 0:
                    g.log.error("Failed to remove firewall")
                    _rc = False
        if permanent and _rc:
            for service in firewall_service:
                cmd = ("firewall-cmd --zone=public " + "--remove-service=" +
                       service + " --permanent")
                results = g.run_parallel(nodes, cmd)
                # Check for return status
                for host in results:
                    ret, _, _ = results[host]
                    if ret != 0:
                        g.log.error("Failed to remove firewall pemanently")
                        _rc = False

    return _rc


def add_services_to_firewall(nodes, firewall_service, permanent=False):
    """Adding services to firewall on nodes
    This lib only for RHEL7, RHEL6 Not Required
    Args:
        nodes(list|str): List of server on which firewalls to be enabled
        firewall_service(list|str): List of firewall services to be enabled
        permanent(boolean): True|False
    Return:
        bool: True|False(Firewall Enabled or Failed)
    """

    if not isinstance(nodes, list):
        nodes = [nodes]

    if not isinstance(firewall_service, list):
        firewall_service = [firewall_service]

    _rc = True
    if not is_rhel6(nodes):
        for service in firewall_service:
            cmd = ("firewall-cmd --zone=public " + "--add-service=" + service)
            results = g.run_parallel(nodes, cmd)
            # Check for return status
            for host in results:
                ret, _, _ = results[host]
                if ret != 0:
                    g.log.error("Failed to execute firewall command on %s"
                                % host)
                    _rc = False
        if permanent and _rc:
            for service in firewall_service:
                cmd = ("firewall-cmd --zone=public " + "--add-service=" +
                       service + " --permanent")
                results = g.run_parallel(nodes, cmd)
                # Check for return status
                for host in results:
                    ret, _, _ = results[host]
                    if ret != 0:
                        g.log.error("Failed to add firewall permanently")
                        _rc = False

    return _rc


def get_size_of_mountpoint(node, mount_point):
    """
    get_size_of_mountpoint:
        Returns the size in blocks for the mount point

    Args:
        node - node on which path is mounted
        mount_point - mount point path

    Returns:
        Size of the mount point in blocks or none.
    """

    cmd = "df %s | grep -v '^Filesystem' | awk '{print $4}'" % (mount_point)
    _, out, _ = g.run(node, cmd)

    return out


def add_user(servers, username, group=None):
    """
    Add user with default home directory

    Args:
        servers(list|str): hostname/ip of the system
        username(str): username of the user to be created.
    Kwargs:
        group(str): Group name to which user is to be
                        added.(Default:None)

    Returns:
        bool : True if user add is successful on all servers.
            False otherwise.
    """
    # Checking if group is given or not.
    if not group:
        cmd = "useradd -m %s -d /home/%s" % (username, username)
    else:
        cmd = "useradd -G %s %s" % (group, username)

    if not isinstance(servers, list):
        servers = [servers]

    results = g.run_parallel(servers, cmd)
    for server, ret_value in list(results.items()):
        retcode, _, err = ret_value
        if retcode != 0 and "already exists" not in err:
            g.log.error("Unable to add user on %s", server)
            return False
    return True


def del_user(host, uname):
    """
    Delete user with home directory

    Args:
        host (str): hostname/ip of the system
        uname (str): username

    Return always True
    """
    command = "userdel -r %s" % (uname)
    ret, _, err = g.run(host, command)
    if 'does not exist' in err:
        g.log.warn("User %s is already deleted", uname)
    else:
        g.log.info("User %s successfully deleted", uname)
    return True


def group_add(servers, groupname):
    """
    Creates a group in all the servers.

    Args:
        servers(list|str): Nodes on which cmd is to be executed.
        groupname(str): Name of the group to be created.

    Returns:
        bool: True if add group is successful on all servers.
            False otherwise.

    """
    if not isinstance(servers, list):
        servers = [servers]

    cmd = "groupadd %s" % groupname
    results = g.run_parallel(servers, cmd)

    for server, ret_value in list(results.items()):
        retcode, _, err = ret_value
        if retcode != 0 and "already exists" not in err:
            g.log.error("Unable to add group %s on server %s",
                        groupname, server)
            return False
    return True


def group_del(servers, groupname):
    """
    Deletes a group in all the servers.

    Args:
        servers(list|str): Nodes on which cmd is to be executed.
        groupname(str): Name of the group to be removed.

    Return always True
    """
    if not isinstance(servers, list):
        servers = [servers]

    cmd = "groupdel %s" % groupname
    results = g.run_parallel(servers, cmd)

    for server, ret_value in list(results.items()):
        retcode, _, err = ret_value
        if retcode != 0 and "does not exist" in err:
            g.log.error("Group %s on server %s already removed",
                        groupname, server)
    return True


def ssh_keygen(mnode):
    """
    Creates a pair of ssh private and public key if not present

    Args:
        mnode (str): Node on which cmd is to be executed
    Returns:
        bool : True if ssh-keygen is successful on all servers.
            False otherwise. It also returns True if ssh key
            is already present

    """
    cmd = 'echo -e "n" | ssh-keygen -f ~/.ssh/id_rsa -q -N ""'
    ret, out, _ = g.run(mnode, cmd)
    if ret and "already exists" not in out:
        return False
    return True


def ssh_copy_id(mnode, tonode, passwd, username="root"):
    """
    Copies the default ssh public key onto tonode's
    authorized_keys file.

    Args:
        mnode (str): Node on which cmd is to be executed
        tonode (str): Node to which ssh key is to be copied
        passwd (str): passwd of the user of tonode
    Kwargs:
         username (str): username of tonode(Default:root)

    Returns:
        bool: True if ssh-copy-id is successful to tonode.
            False otherwise. It also returns True if ssh key
            is already present

    """
    cmd = ('sshpass -p "%s" ssh-copy-id -o StrictHostKeyChecking=no %s@%s' %
           (passwd, username, tonode))
    ret, _, _ = g.run(mnode, cmd)
    if ret:
        return False
    return True


def set_passwd(servers, username, passwd):
    """
    Sets password for a given username.

    Args:
        servers(list|str): list of nodes on which cmd is to be executed.
        username(str): username of user for which password is to be set.
        passwd(str): Password to be set.

    Returns:
        bool : True if password set is successful on all servers.
            False otherwise.

    """
    if not isinstance(servers, list):
        servers = [servers]
    cmd = "echo %s:%s | chpasswd" % (username, passwd)
    results = g.run_parallel(servers, cmd)

    for server, ret_value in list(results.items()):
        retcode, _, _ = ret_value
        if retcode != 0:
            g.log.error("Unable to set passwd for user %s on %s",
                        username, server)
            return False
    return True


def is_user_exists(servers, username):
    """
    Checks if user is present on the given servers or not.

    Args:
        servers(str|list): list of nodes on which you need to
                           check if the user is present or not.
        username(str): username of user whose presence has to be checked.

    Returns:
        bool: True if user is present on all nodes else False.
    """
    if not isinstance(servers, list):
        servers = [servers]

    cmd = "id %s" % username
    results = g.run_parallel(servers, cmd)

    for server, (ret_value, _, _) in results.items():
        if not ret_value:
            g.log.error("User %s doesn't exists on server %s.",
                        (username, server))
            return False
    return True


def is_group_exists(servers, group):
    """
    Checks if group is present on the given servers.

    Args:
        servers(str|list): list of nodes on which you need to
                           check if group is present or not.
        group(str): groupname of group whose presence has
                    to be checked.

    Returns:
        bool: True if group is present on all nodes else False.
    """
    if not isinstance(servers, list):
        servers = [servers]

    cmd = "grep -q %s /etc/group" % group
    results = g.run_parallel(servers, cmd)

    for server, (ret_value, _, _) in results.items():
        if not ret_value:
            g.log.error("Group %s doesn't exists on server %s.",
                        (group, server))
            return False
    return True


def is_passwordless_ssh_configured(fromnode, tonode, username):
    """
    Checks if passwordless ssh is configured between nodes or not.

    Args:
        fromnode: Server from which passwordless ssh has to be
                  configured.
        tonode: Server to which passwordless ssh has to be
                configured.
        username: username of user to be used for checking
                  passwordless ssh.
    Returns:
        bool: True if configured else false.
    """
    cmd = ("ssh %s@%s hostname" % (username, tonode))
    ret, out, _ = g.run(fromnode, cmd)
    _, hostname, _ = g.run(tonode, "hostname")
    if ret or hostname not in out:
        g.log.error("Passwordless ssh not configured "
                    "from server %s to server %s using user %s.",
                    (fromnode, tonode, username))
        return False
    return True


def collect_bricks_arequal(bricks_list):
    """Collects arequal for all bricks in list

    Args:
        bricks_list (list): List of bricks.
        Example:
            bricks_list = 'gluster.blr.cluster.com:/bricks/brick1/vol'

    Returns:
        tuple(bool, list):
            On success returns (True, list of arequal-checksums of each brick)
            On failure returns (False, list of arequal-checksums of each brick)
            arequal-checksum for a brick would be 'None' when failed to
            collect arequal for that brick.

    Example:
        >>> all_bricks = get_all_bricks(self.mnode, self.volname)
        >>> ret, arequal = collect_bricks_arequal(all_bricks)
        >>> ret
        True
    """
    # Converting a bricks_list to list if not.
    if not isinstance(bricks_list, list):
        bricks_list = [bricks_list]

    return_code, arequal_list = True, []
    for brick in bricks_list:

        # Running arequal-checksum on the brick.
        node, brick_path = brick.split(':')
        cmd = ('arequal-checksum -p {} -i .glusterfs -i .landfill -i .trashcan'
               .format(brick_path))
        ret, arequal, _ = g.run(node, cmd)

        # Generating list accordingly
        if ret:
            g.log.error('Failed to get arequal on brick %s', brick)
            return_code = False
            arequal_list.append(None)
        else:
            g.log.info('Successfully calculated arequal for brick %s', brick)
            arequal_list.append(arequal)

    return (return_code, arequal_list)


def get_usable_size_per_disk(brickpath, min_free_limit=10):
    """Get the usable size per disk

    Args:
     brickpath(str): Brick path to be used to calculate usable size

    Kwargs:
     min_free_limit(int): Min free disk limit to be used

    Returns:
      (int): Usable size in GB. None in case of errors.
    """
    node, brick_path = brickpath.split(':')
    size = get_size_of_mountpoint(node, brick_path)
    if not size:
        return None
    size = int(size)
    min_free_size = size * min_free_limit // 100
    usable_size = ((size - min_free_size) // 1048576) + 1
    return usable_size
