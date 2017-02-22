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
    Description: Helper library for io modules.
"""
import os
import subprocess
from glusto.core import Glusto as g


def collect_mounts_arequal(mounts):
    """Collects arequal from all the mounts

    Args:
        mounts (list): List of all GlusterMount objs.

    Returns:
        tuple(bool, list):
            On success returns (True, list of arequal-checksums of each mount)
            On failure returns (False, list of arequal-checksums of each mount)
            arequal-checksum for a mount would be 'None' when failed to
            collect arequal for that mount.
    """
    if isinstance(mounts, str):
        mounts = [mounts]

    # Collect arequal-checksum from all mounts
    g.log.info("Start collecting arequal-checksum from all mounts")
    all_mounts_procs = []
    for mount_obj in mounts:
        g.log.info("arequal-checksum of mount %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        cmd = "arequal-checksum -p %s -i .trashcan" % mount_obj.mountpoint
        proc = g.run_async(mount_obj.client_system, cmd,
                           user=mount_obj.user)
        all_mounts_procs.append(proc)
    all_mounts_arequal_checksums = []
    _rc = True
    for i, proc in enumerate(all_mounts_procs):
        ret, out, _ = proc.async_communicate()
        if ret != 0:
            g.log.error("Collecting arequal-checksum failed on %s:%s",
                        mounts[i].client_system, mounts[i].mountpoint)
            _rc = False
            all_mounts_arequal_checksums.append(None)
        else:
            g.log.info("Collecting arequal-checksum successful on %s:%s",
                       mounts[i].client_system, mounts[i].mountpoint)
            all_mounts_arequal_checksums.append(out)
    return (_rc, all_mounts_arequal_checksums)


def log_mounts_info(mounts):
    """Logs mount information like df, stat, ls

    Args:
        mounts (list): List of all GlusterMount objs.
    """
    if isinstance(mounts, str):
        mounts = [mounts]

    g.log.info("Start logging mounts information:")
    for mount_obj in mounts:
        g.log.info("Information of mount %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        # Mount Info
        g.log.info("Look For Mountpoint:\n")
        cmd = "mount | grep %s" % mount_obj.mountpoint
        _, _, _ = g.run(mount_obj.client_system, cmd)

        # Disk Space Usage
        g.log.info("Disk Space Usage Of Mountpoint:\n")
        cmd = "df -h %s" % mount_obj.mountpoint
        _, _, _ = g.run(mount_obj.client_system, cmd)

        # Long list the mountpoint
        g.log.info("List Mountpoint Entries:\n")
        cmd = "ls -ld %s" % mount_obj.mountpoint
        _, _, _ = g.run(mount_obj.client_system, cmd)

        # Stat mountpoint
        g.log.info("Mountpoint Status:\n")
        cmd = "stat %s" % mount_obj.mountpoint
        _, _, _ = g.run(mount_obj.client_system, cmd)


def get_mounts_stat(mounts):
    """Recursively get stat of the mountpoint

    Args:
        mounts (list): List of all GlusterMount objs.

    Returns:
        bool: True if recursively getting stat from all mounts is successful.
            False otherwise.
    """
    if isinstance(mounts, str):
        mounts = [mounts]

    g.log.info("Start getting stat of the mountpoint recursively")
    all_mounts_procs = []
    for mount_obj in mounts:
        g.log.info("Stat of mount %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        cmd = ("find %s | xargs stat" % (mount_obj.mountpoint))
        proc = g.run_async(mount_obj.client_system, cmd,
                           user=mount_obj.user)
        all_mounts_procs.append(proc)
    _rc = True
    for i, proc in enumerate(all_mounts_procs):
        ret, _, _ = proc.async_communicate()
        if ret != 0:
            g.log.error("Stat of files and dirs under %s:%s Failed",
                        mounts[i].client_system, mounts[i].mountpoint)
            _rc = False
        else:
            g.log.info("Stat of files and dirs under %s:%s is successful",
                       mounts[i].client_system, mounts[i].mountpoint)
    return _rc


def list_all_files_and_dirs_mounts(mounts):
    """List all Files and Directories from mounts.

    Args:
        mounts (list): List of all GlusterMount objs.

    Returns:
        bool: True if listing file and dirs on mounts is successful.
            False otherwise.
    """
    if isinstance(mounts, str):
        mounts = [mounts]

    ignore_dirs_list = [".trashcan"]
    ignore_dirs = "\|".join(ignore_dirs_list)

    g.log.info("Start Listing mounts files and dirs")
    all_mounts_procs = []
    for mount_obj in mounts:
        g.log.info("Listing files and dirs on %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        cmd = ("find %s | grep -ve '%s'" % (mount_obj.mountpoint, ignore_dirs))
        proc = g.run_async(mount_obj.client_system, cmd, user=mount_obj.user)
        all_mounts_procs.append(proc)
    _rc = True
    for i, proc in enumerate(all_mounts_procs):
        ret, _, _ = proc.async_communicate()
        if ret != 0:
            g.log.error("Failed to list all files and dirs under %s:%s",
                        mounts[i].client_system, mounts[i].mountpoint)
            _rc = False
        else:
            g.log.info("Successfully listed all files and dirs under %s:%s",
                       mounts[i].client_system, mounts[i].mountpoint)
    return _rc


def validate_io_procs(all_mounts_procs, mounts):
    """Validates whether IO was successful or not

    Args:
        all_mounts_procs (list): List of open connection descriptor as
            returned by g.run_async method.
        mounts (list): List of all GlusterMount objs on which process were
            started.

    Returns:
        bool: True if IO is successful on all mounts. False otherwise.
    """
    if isinstance(all_mounts_procs, subprocess.Popen):
        all_mounts_procs = [all_mounts_procs]

    if isinstance(mounts, str):
        mounts = [mounts]

    _rc = True
    g.log.info("Start validating IO procs")
    for i, proc in enumerate(all_mounts_procs):
        g.log.info("Validating IO on %s:%s", mounts[i].client_system,
                   mounts[i].mountpoint)
        ret, _, _ = proc.async_communicate()
        if ret != 0:
            g.log.error("IO Failed on %s:%s", mounts[i].client_system,
                        mounts[i].mountpoint)
            _rc = False
        else:
            g.log.info("IO Successful on %s:%s", mounts[i].client_system,
                       mounts[i].mountpoint)
    return _rc


def wait_for_io_to_complete(all_mounts_procs, mounts):
    """Waits for IO to complete

    Args:
        all_mounts_procs (list): List of open connection descriptor as
            returned by g.run_async method.
        mounts (list): List of all GlusterMount objs on which process were
            started.

    Returns:
        bool: True if IO is complete on all mounts. False otherwise.
    """
    if isinstance(all_mounts_procs, subprocess.Popen):
        all_mounts_procs = [all_mounts_procs]

    if isinstance(mounts, str):
        mounts = [mounts]

    _rc = True
    for i, proc in enumerate(all_mounts_procs):
        g.log.info("Waiting for IO to be complete on %s:%s",
                   mounts[i].client_system, mounts[i].mountpoint)
        ret, _, _ = proc.async_communicate()
        if ret != 0:
            g.log.error("IO Not complete on %s:%s", mounts[i].client_system,
                        mounts[i].mountpoint)
            _rc = False
        else:
            g.log.info("IO is complete on %s:%s", mounts[i].client_system,
                       mounts[i].mountpoint)
    return _rc


def cleanup_mounts(mounts):
    """Removes all the data from all the mountpoints

    Args:
        mounts (list): List of all GlusterMount objs.

    Returns:
        bool: True if cleanup is successful on all mounts. False otherwise.
    """
    if isinstance(mounts, str):
        mounts = [mounts]

    g.log.info("Start cleanup mounts")
    all_mounts_procs = []
    valid_mounts = []
    for mount_obj in mounts:
        g.log.info("Cleaning up data from %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        if (not mount_obj.mountpoint or
                (os.path.realpath(os.path.abspath(mount_obj.mountpoint))
                 is '/')):
            g.log.error("%s on %s is not a valid mount point",
                        mount_obj.mountpoint, mount_obj.client_system)
            continue
        cmd = "rm -rf %s/*" % (mount_obj.mountpoint)
        proc = g.run_async(mount_obj.client_system, cmd,
                           user=mount_obj.user)
        all_mounts_procs.append(proc)
        valid_mounts.append(mount_obj)
    g.log.info("rm -rf on all clients is complete. Validating "
               "deletion now...")

    # Get cleanup status
    _rc_rmdir = True
    for i, proc in enumerate(all_mounts_procs):
        ret, out, err = proc.async_communicate()
        if ret != 0 or out or err:
            g.log.error("Deleting files/dirs Failed on %s:%s",
                        valid_mounts[i].client_system,
                        valid_mounts[i].mountpoint)
            _rc_rmdir = False
        else:
            g.log.info("Deleting files/dirs is successful on %s:%s",
                       valid_mounts[i].client_system,
                       valid_mounts[i].mountpoint)
    if _rc_rmdir:
        g.log.info("Successfully deleted files/dirs from all mounts")
    else:
        g.log.error("Deleting files/dirs failed on some of the mounts")

    # Check if mount points are empty
    ignore_dirs_list = [".trashcan"]
    ignore_dirs = "\|".join(ignore_dirs_list)
    all_mounts_procs = []
    for mount_obj in mounts:
        cmd = ("find %s -mindepth 1 | grep -ve '%s'" %
               (mount_obj.mountpoint, ignore_dirs))
        proc = g.run_async(mount_obj.client_system, cmd,
                           user=mount_obj.user)
        all_mounts_procs.append(proc)

    # Get cleanup status
    _rc_lookup = True
    for i, proc in enumerate(all_mounts_procs):
        ret, out, err = proc.async_communicate()
        if ret == 0:
            g.log.error("Mount %s on %s is still having entries:\n%s",
                        mounts[i].mountpoint, mounts[i].client_system, out)
            _rc_lookup = False
        else:
            g.log.info("Mount %s on %s is cleaned up\n%s",
                       mounts[i].mountpoint, mounts[i].client_system, out)
    if _rc_lookup:
        g.log.info("All the mounts are successfully cleaned up")
    else:
        g.log.error("Failed to cleanup all mounts")

    # List mounts entries
    g.log.info("Listing mounts entries:")
    list_all_files_and_dirs_mounts(mounts)

    return _rc_lookup
