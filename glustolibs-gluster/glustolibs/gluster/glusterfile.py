#!/usr/bin/env python
#  Copyright (C) 2018-2020 Red Hat, Inc. <http://www.redhat.com>
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
#
"""Description: Module for library gluster file class and related functions.

A GlusterFile is a file object that exists on the client and backend brick.
This module provides low-level functions and a GlusterFile class to maintain
state and manage properties of a file in both locations.
"""

import ctypes
import os
import re

from glusto.core import Glusto as g
from glustolibs.gluster.layout import Layout
from glustolibs.misc.misc_libs import upload_scripts


def calculate_hash(host, filename):
    """ Function to import DHT Hash library.

    Args:
        filename (str): the name of the file

    Returns:
        An integer representation of the hash

    TODO: For testcases specifically testing hashing routine
          consider using a baseline external Davies-Meyer hash_value.c
          Creating comparison hash from same library we are testing
          may not be best practice here. (Holloway)
    """
    try:
        # Check if libglusterfs.so.0 is available locally
        glusterfs = ctypes.cdll.LoadLibrary("libglusterfs.so.0")
        g.log.debug("Library libglusterfs.so.0 loaded locally")
        computed_hash = (
            ctypes.c_uint32(glusterfs.gf_dm_hashfn(filename,
                                                   len(filename))))
        hash_value = int(computed_hash.value)
    except OSError:
        script_path = ("/usr/share/glustolibs/scripts/"
                       "compute_hash.py")
        if not file_exists(host, script_path):
            if upload_scripts(host, script_path,
                              '/usr/share/glustolibs/scripts/'):
                g.log.info("Successfully uploaded script "
                           "compute_hash.py!")
            else:
                g.log.error('Unable to upload the script to node {0}'
                            .format(host))
                return 0
        else:
            g.log.info("compute_hash.py already present!")
        cmd = ("/usr/bin/env python {0} {1}".format(script_path,
                                                    filename))
        ret, out, _ = g.run(host, cmd)
        if ret:
            g.log.error('Unable to run the script on node {0}'
                        .format(host))
            return 0
        hash_value = int(out.split('\n')[0])
    return hash_value


def get_mountpoint(host, fqpath):
    """Retrieve the mountpoint under a file

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.

    Returns:
        The mountpoint on success. None on fail.
    """
    command = "df -P %s | awk 'END{print $NF}'" % fqpath
    rcode, rout, rerr = g.run(host, command)
    if rcode == 0:
        return rout.strip()

    g.log.error("Get mountpoint failed: %s" % rerr)
    return None


def get_fattr(host, fqpath, fattr, encode="hex"):
    """getfattr for filepath on remote system

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.
        fattr (str): name of the fattr to retrieve
    Kwargs:
        encode(str): The supported types of encoding are
                     [hex|text|base64]
                     Defaults to hex type of encoding
    Returns:
        getfattr result on success. None on fail.
    """
    command = ("getfattr --absolute-names -e '%s' "
               "-n '%s' %s" %
               (encode, fattr, fqpath))
    rcode, rout, rerr = g.run(host, command)
    if not rcode:
        return rout.strip().split('=')[1]

    g.log.error('getfattr failed: %s' % rerr)
    return None


def get_fattr_list(host, fqpath, encode_hex=False):
    """List of xattr for filepath on remote system.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.

    Kwargs:
       encode_hex(bool): Fetch xattr in hex if True
                         (Default:False)

    Returns:
        Dictionary of xattrs on success. None on fail.
    """
    cmd = "getfattr --absolute-names -d -m - {}".format(fqpath)
    if encode_hex:
        cmd = ("getfattr --absolute-names -d -m - -e hex {}"
               .format(fqpath))
    rcode, rout, rerr = g.run(host, cmd)

    if rcode == 0:
        xattr_list = {}
        for xattr_string in rout.strip().split('\n'):
            xattr = xattr_string.split('=', 1)
            if len(xattr) > 1:
                key, value = xattr
                xattr_list[key] = value

        return xattr_list

    g.log.error('getfattr failed: %s' % rerr)
    return None


def set_fattr(host, fqpath, fattr, value):
    """setfattr for filepath on remote system

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.
        fattr (str): The name of the fattr to retrieve.

    Returns:
        setfattr result on success. errorcode on fail
    """
    command = 'setfattr -n %s -v %s %s' % (fattr, value, fqpath)
    rcode, _, rerr = g.run(host, command)

    if rcode == 0:
        return True

    g.log.error('setfattr failed: %s', rerr)
    return False


def delete_fattr(host, fqpath, fattr):
    """remove fattr for filepath on remote system

    Args:
        host (str): hostname/ip of remote system
        fqpath (str): the fully qualified path of the file
        fattr (str): name of the fattr to delete

    Returns:
        setfattr result on success. errorcode on fail
    """
    command = 'setfattr -x %s %s' % (fattr, fqpath)
    rcode, _, rerr = g.run(host, command)

    if rcode == 0:
        return True

    g.log.error('setfattr -x failed: %s' % rerr)
    return False


def file_exists(host, fqpath):
    """Check if file exists at path on host

    Args:
        host (str): hostname or ip of system
        filename (str): fully qualified path of file

    Returns:
        True if file exists. False if file does not exist
    """
    command = "ls -ld %s" % fqpath
    rcode, _, rerr = g.run(host, command)
    if rcode == 0:
        return True

    g.log.error('File does not exist: %s', rerr)
    return False


def get_md5sum(host, fqpath):
    """Get the md5 checksum for the file.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.

    Returns:
        The md5sum of the file on success. None on fail.
    """
    command = "md5sum %s" % fqpath
    rcode, rout, rerr = g.run(host, command)

    if rcode == 0:
        return rout.strip()

    g.log.error('md5sum failed: %s' % rerr)
    return None


def get_file_stat(host, fqpath):
    """Get file stat information about a file.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.

    Returns:
        A dictionary of file stat data. None on fail.
    """
    statformat = '%F$%n$%i$%a$%s$%h$%u$%g$%U$%G$%x$%y$%z$%X$%Y$%Z'
    command = "stat -c '%s' %s" % (statformat, fqpath)
    rcode, rout, rerr = g.run(host, command)
    if rcode == 0:
        stat_data = {}
        stat_string = rout.strip()
        (filetype, filename, inode,
         access, size, links,
         uid, gid, username, groupname,
         atime, mtime, ctime, epoch_atime,
         epoch_mtime, epoch_ctime) = stat_string.split("$")

        stat_data['filetype'] = filetype
        stat_data['filename'] = filename
        stat_data["inode"] = inode
        stat_data["access"] = access
        stat_data["size"] = size
        stat_data["links"] = links
        stat_data["username"] = username
        stat_data["groupname"] = groupname
        stat_data["uid"] = uid
        stat_data["gid"] = gid
        stat_data["atime"] = atime
        stat_data["mtime"] = mtime
        stat_data["ctime"] = ctime
        stat_data["epoch_atime"] = epoch_atime
        stat_data["epoch_mtime"] = epoch_mtime
        stat_data["epoch_ctime"] = epoch_ctime

        return stat_data

    g.log.error("Could not stat file %s: %s" % (fqpath, rerr))
    return None


def set_file_permissions(host, fqpath, perms):
    """Set permissions on a remote file.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.
        perms (str): A permissions string as passed to chmod.

    Returns:
        True on success. False on fail.
    """
    command = "chmod %s %s" % (perms, fqpath)
    rcode, _, rerr = g.run(host, command)

    if rcode == 0:
        return True

    g.log.error('chmod failed: %s' % rerr)
    return False


def set_file_owner(host, fqpath, user):
    """Set file owner for  a remote file.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.
        user (str): The user owning the file.

    Returns:
        True on success. False on fail.
    """
    command = "chown %s %s" % (user, fqpath)
    rcode, _, rerr = g.run(host, command)

    if rcode == 0:
        return True

    g.log.error('chown failed: %s' % rerr)
    return False


def set_file_group(host, fqpath, group):
    """Set file group for  a remote file.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.
        group (str): The group owning the file.

    Returns:
        True on success. False on fail.
    """
    command = "chgrp %s %s" % (group, fqpath)
    rcode, _, rerr = g.run(host, command)

    if rcode == 0:
        return True

    g.log.error('chgrp failed: %s' % rerr)
    return False


def move_file(host, source_fqpath, dest_fqpath):
    """Move a remote file.

    Args:
        host (str): The hostname/ip of the remote system.
        source_fqpath (str): The fully-qualified path to the file to move.
        dest_fqpath (str): The fully-qualified path to the new file location.

    Returns:
        True on success. False on fail.
    """
    command = "mv %s %s" % (source_fqpath, dest_fqpath)
    rcode, _, rerr = g.run(host, command)

    if rcode == 0:
        return True

    g.log.error('mv failed: %s' % rerr)
    return False


def remove_file(host, fqpath, force=False):
    """Removes a remote file.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.

    Returns:
        True on success. False on fail.
    """
    command_list = ['rm']
    if force:
        command_list.append('-f')
    command_list.append(fqpath)
    rcode, _, rerr = g.run(host, ' '.join(command_list))

    if rcode == 0:
        return True

    g.log.error('Remove file failed: %s' % rerr)
    return False


def get_pathinfo(host, fqpath):
    """Get pathinfo for a remote file.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.

    Returns:
        A dictionary of pathinfo data for a remote file. None on fail.
    """
    pathinfo = {}
    pathinfo['raw'] = get_fattr(host, fqpath, 'trusted.glusterfs.pathinfo')
    pathinfo['brickdir_paths'] = re.findall(r".*?POSIX.*?:(\S+)\>",
                                            pathinfo['raw'])

    return pathinfo


def is_linkto_file(host, fqpath):
    """Test if file is a dht linkto file.
    To return True, file must...
    1. be of file type 'sticky empty'
    2. have size of 0
    3. have the glusterfs.dht.linkto xattr set.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.

    Returns:
        True or False
    """
    command = 'file %s' % fqpath
    rcode, rout, _ = g.run(host, command)
    if rcode == 0:
        # An additional ',' is there for newer platforms
        if 'sticky empty' or 'sticky, empty' in rout.strip():
            stat = get_file_stat(host, fqpath)
            if int(stat['size']) == 0:
                xattr = get_dht_linkto_xattr(host, fqpath)
                if xattr is not None:
                    return True
    return False


def get_dht_linkto_xattr(host, fqpath):
    """Get the glusterfs.dht.linkto xattr for a file on a brick.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.

    Returns:
        Return value of get_fattr trusted.glusterfs.dht.linkto call.
    """
    linkto_xattr = get_fattr(host, fqpath, 'trusted.glusterfs.dht.linkto')

    return linkto_xattr


def find_and_replace_in_file(host, fpattern, rpattern, fqpath):
    """Find and replace all occurances of a given pattern in a specific file.

    Args:
        host (str): The hostname/ip of the remote system.
        fpattern(str): Pattern to be found in file.
        rpattern(str): Pattern to used as replacement in file.
        fqpath (str): The fully-qualified path to the file.

    Returns:
        True: If find and replace is successful.
        False: If find and replace is failed.
    Note:
    / can't be given as an input in patterns(fpattern/rpattern).
    Please follow proper regex format for patterns.
    """
    cmd = (" sed -i 's/%s/%s/g' %s" % (fpattern, rpattern, fqpath))
    ret, _, _ = g.run(host, cmd)

    if ret != 0:
        return False
    return True


def check_if_pattern_in_file(host, pattern, fqpath):
    """Check if a give pattern is in seen in file or not.

    Args:
        host (str): The hostname/ip of the remote system.
        pattern(str): Pattern to be found in file.
        fqpath (str): The fully-qualified path to the file.

    Returns:
        0: If pattern present in file.
        1: If pattern not present in file.
       -1: If command was not executed.
    """
    cmd = ("cat %s | grep '%s'" % (fqpath, pattern))
    ret, out, _ = g.run(host, cmd)
    if ret:
        return -1
    if not out:
        return 1
    return 0


def occurences_of_pattern_in_file(node, search_pattern, filename):
    """
    Get the number of occurences of pattern in the file

    Args:
        node (str): Host on which the command is executed.
        search_pattern (str): Pattern to be found in the file.
        filename (str): File in which the pattern is to be validated

    Returns:
         (int): (-1), When the file doesn't exists.
                (0), When pattern doesn't exists in the file.
                (number), When pattern is found and the number of
                          occurences of pattern in the file.

    Example:
    occurences_of_pattern_in_file(node, search_pattern, filename)
    """

    ret = file_exists(node, filename)
    if not ret:
        g.log.error("File %s is not present on the node " % filename)
        return -1

    cmd = ("grep -c '%s' %s" % (search_pattern, filename))
    ret, out, _ = g.run(node, cmd)
    if ret:
        g.log.error("No occurence of the pattern found in the file %s" %
                    filename)
        return 0
    return int(out.strip('\n'))


def create_link_file(node, file, link, soft=False):
    """
    Create hard or soft link for an exisiting file

    Args:
        node(str): Host on which the command is executed.
        file(str): Path to the source file.
        link(str): Path to the link file.

    Kawrgs:
        soft(bool): Create soft link if True else create
        hard link.

    Returns:
        (bool): True if command successful else False.

    Example:
        >>> create_link_file('10.20.30.40', '/mnt/mp/file.txt',
                             '/mnt/mp/link')
        True
    """
    cmd = "ln {} {}".format(file, link)
    if soft:
        cmd = "ln -s {} {}".format(file, link)

    ret, _, err = g.run(node, cmd)
    if ret:
        if soft:
            g.log.error('Failed to create soft link on {} '
                        'for file {} with error {}'
                        .format(node, file, err))
        else:
            g.log.error('Failed to create hard link on {} '
                        'for file {} with error {}'
                        .format(node, file, err))
        return False
    return True


class GlusterFile(object):
    """Class to handle files specific to Gluster (client and backend)"""
    def __init__(self, host, fqpath):
        self._host = host
        self._fqpath = fqpath

        self._mountpoint = None
        self._calculated_hash = None
        self._pathinfo = None
        self._parent_dir_pathinfo = None
        self._parent_dir_layout = None

        self._previous_fqpath = None

    @property
    def host(self):
        """str: the hostname/ip of the client system hosting the file."""
        return self._host

    @property
    def fqpath(self):
        """str: the fully-qualified path of the file on the client system."""
        return self._fqpath

    @property
    def relative_path(self):
        """str: the relative path from the mountpoint of the file."""
        return os.path.relpath(self._fqpath, self.mountpoint)

    @property
    def basename(self):
        """str: the name of the file with directories stripped from string."""
        return os.path.basename(self._fqpath)

    @property
    def parent_dir(self):
        """str: the full-qualified path of the file's parent directory."""
        return os.path.dirname(self._fqpath)

    @property
    def mountpoint(self):
        """str: the fully-qualified path of the mountpoint under the file."""
        if self._mountpoint is None:
            self._mountpoint = get_mountpoint(self._host, self._fqpath)

        return self._mountpoint

    @property
    def pathinfo(self):
        """dict: a dictionary of path_info-related values"""
        self._pathinfo = get_pathinfo(self._host, self._fqpath)

        return self._pathinfo

    @property
    def parent_dir_pathinfo(self):
        """"dict: a dictionary of path_info-related values for the parent dir
                    of the file's fqpath.
        """
        parent_dir_pathinfo = get_pathinfo(self._host, self.parent_dir)

        return parent_dir_pathinfo

    @property
    def exists_on_client(self):
        """bool: Does the file exists on the client?"""
        ret = file_exists(self._host, self._fqpath)

        if ret:
            return True

        return False

    @property
    def exists_on_bricks(self):
        """bool: Does the file exist on the backend bricks?"""
        flag = 0
        for brickdir_path in self.pathinfo['brickdir_paths']:
            (host, fqpath) = brickdir_path.split(':')
            if not file_exists(host, fqpath):
                flag = flag | 1

        if flag == 0:
            return True

        return False

    @property
    def exists_on_hashed_bricks(self):
        """bool: Does the file exist on the hashed bricks as expected?"""
        # TODO: inject check for linkto and data files
        flag = 0
        for brickdir_path in self.hashed_bricks:
            (host, fqpath) = brickdir_path.split(':')
            if not file_exists(host, fqpath):
                flag = flag | 1

        if flag == 0:
            return True

        return False

    @property
    def exists_on_cached_bricks(self):
        """bool: Does the file exist on the cached bricks as expected?

            This currently is redundant as the cache list is currently
            created by searching bricks for the file. This will be more
            useful when the cached brick list is compiled by following the
            subvolume info provided in the linkto xattr.
        """
        flag = 0
        for brickdir_path in self.cached_bricks:
            (host, fqpath) = brickdir_path.split(':')
            if not file_exists(host, fqpath):
                flag = flag | 1

        if flag == 0:
            return True

        return False

    @property
    def exists(self):
        """bool: does the file exist on both client and backend bricks"""
        return (self.exists_on_client, self.exists_on_bricks)

    @property
    def stat_on_client(self):
        """dict: a dictionary of stat data"""
        return get_file_stat(self._host, self._fqpath)

    @property
    def stat_on_bricks(self):
        """dict: a dictionary of stat dictionaries for the file on bricks"""
        file_stats = {}
        for brickdir_path in self.pathinfo['brickdir_paths']:
            (host, fqpath) = brickdir_path.split(':')
            file_stats[brickdir_path] = get_file_stat(host, fqpath)

        return file_stats

    @property
    def stat(self):
        """list: a list of the stat dictionary data for client and bricks."""
        return (self.stat_on_client, self.stat_on_bricks)

    @property
    def md5sum_on_client(self):
        """str: the md5sum for the file on the client"""
        return get_md5sum(self._host, self._fqpath)

    @property
    def md5sum_on_bricks(self):
        """dict: a dictionary of md5sums for the file on bricks"""
        # TODO: handle dispersed ???
        file_md5s = {}
        for brickdir_path in self.pathinfo['brickdir_paths']:
            (host, fqpath) = brickdir_path.split(':')
            file_md5s[brickdir_path] = get_md5sum(host, fqpath)

        return file_md5s

    @property
    def md5sum(self):
        """list: a list of client and brick md5sum data"""
        return (self.md5sum_on_client, self.md5sum_on_bricks)

    @property
    def calculated_hash(self):
        """str: the computed hash of the file using libglusterfs"""
        if self._calculated_hash is None:
            self._calculated_hash = calculate_hash(self._host, self.basename)

        return self._calculated_hash

    @property
    def parent_dir_layout(self):
        """obj: Layout instance of the file's parent directory"""
        if self._parent_dir_layout is None:
            layout = Layout(self.parent_dir_pathinfo)
            self._parent_dir_layout = layout
        else:
            layout = self._parent_dir_layout

        return layout

    @property
    def hashed_bricks(self):
        """list: the list of bricks matching with hashrange surrounding hash"""
        brickpaths = []
        for brickdir in self.parent_dir_layout.brickdirs:
            low = brickdir.hashrange_low
            high = brickdir.hashrange_high
            if low < self.calculated_hash < high:
                brickpaths.append(brickdir.path)
                g.log.debug("%s: %d - %d - %d" % (brickdir.path,
                                                  brickdir.hashrange_low,
                                                  self.calculated_hash,
                                                  brickdir.hashrange_high))

        return brickpaths

    @property
    def cached_bricks(self):
        """list: the list of bricks with the cached file(s)"""
        # TODO: build list from subvolume in glusterfs.dht.linkto xattr
        brickpaths = []
        for brickdir in self.parent_dir_layout.brickdirs:
            fqpath = os.path.join(brickdir.fqpath, self.basename)
            if file_exists(brickdir.host, fqpath):
                if not is_linkto_file(brickdir.host, fqpath):
                    brickpaths.append(brickdir.path)

        return brickpaths

    def move(self, dest_fqpath):
        """Move the file to a new location and store previous fqpath.

        Args:
            dest_fqpath (str): The fully-qualified destination path.

        Returns:
            True on success. False on fail.
        """
        ret = move_file(self._host, self._fqpath, dest_fqpath)

        if ret:
            # TODO: change this to use a setter/getter for heavy lifting once
            #        and can reset everything from one place
            self._previous_fqpath = self._fqpath
            self._fqpath = dest_fqpath

            return True

        return False

    def create(self):
        """Creates a simple file via copy for testing purposes.
            Also creates parent directories if they don't exist.
        Args:
            None

        Returns:
            True on success. False on failure.
        """
        if not self.exists_on_client:
            command = "mkdir -p %s" % self.parent_dir
            rcode, _, _ = g.run(self._host, command)
            if rcode != 0:
                return False
            command = "cp /etc/inittab %s" % self._fqpath
            rcode, _, _ = g.run(self._host, command)
            if rcode == 0:
                return True

        return False

    def get_xattr(self, xattr):
        """Get the xattr for the file instance.

        Args:
            xattr (str): The file attribute to get from file.

        Returns:
            Result of get_fattr function.
        """
        return get_fattr(self._host, self._fqpath, xattr)

    def set_xattr(self, xattr, value):
        """Set the specified xattr for the file instance.

        Args:
            xattr (str): The attribute to set on the file.
            value (str): the value for the attribute.

        Returns:
            Return of set_fattr function.
        """
        return set_fattr(self._host, self._fqpath, xattr, value)

    def delete_xattr(self, xattr):
        """Delete the specified xattr for the file instance.

        Args:
            xattr (str): The attribute to delete.

        Returns:
            Return of delete_fattr function.
        """
        return delete_fattr(self._host, self._fqpath, xattr)
