#  Copyright (C) 2015-2020  Red Hat, Inc. <http://www.redhat.com>
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
from multiprocessing import Pool
import os
import subprocess

from glusto.core import Glusto as g
from glustolibs.gluster.glusterfile import file_exists
from glustolibs.gluster.mount_ops import GlusterMount
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.misc.misc_libs import upload_scripts


def collect_mounts_arequal(mounts, path=''):
    """Collects arequal from all the mounts

    Args:
        mounts (list): List of all GlusterMount objs.

    Kwargs:
        path (str): Path whose arequal is to be calculated.
                    Defaults to root of mountpoint
    Returns:
        tuple(bool, list):
            On success returns (True, list of arequal-checksums of each mount)
            On failure returns (False, list of arequal-checksums of each mount)
            arequal-checksum for a mount would be 'None' when failed to
            collect arequal for that mount.
    """
    if isinstance(mounts, GlusterMount):
        mounts = [mounts]

    # Collect arequal-checksum from all mounts
    g.log.info("Start collecting arequal-checksum from all mounts")
    all_mounts_procs = []
    for mount_obj in mounts:
        total_path = os.path.join(mount_obj.mountpoint, path)
        g.log.info("arequal-checksum of mount %s:%s", mount_obj.client_system,
                   total_path)
        cmd = "arequal-checksum -p %s -i .trashcan" % total_path
        proc = g.run_async(mount_obj.client_system, cmd, user=mount_obj.user)
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
    """Log mount information like df, stat, ls

    Args:
        mounts (list): List of all GlusterMount objs.
    """
    if isinstance(mounts, GlusterMount):
        mounts = [mounts]

    g.log.info("Start logging mounts information:")
    for mount_obj in mounts:
        g.log.info("Information of mount %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        # Mount Info
        g.log.info("Look For Mountpoint:\n")
        cmd = "mount | grep %s" % mount_obj.mountpoint
        g.run(mount_obj.client_system, cmd)

        # Disk Space Usage
        g.log.info("Disk Space Usage Of Mountpoint:\n")
        cmd = "df -h %s" % mount_obj.mountpoint
        g.run(mount_obj.client_system, cmd)

        # Long list the mountpoint
        g.log.info("List Mountpoint Entries:\n")
        cmd = "ls -ld %s" % mount_obj.mountpoint
        g.run(mount_obj.client_system, cmd)

        # Stat mountpoint
        g.log.info("Mountpoint Status:\n")
        cmd = "stat %s" % mount_obj.mountpoint
        g.run(mount_obj.client_system, cmd)


def get_mounts_stat(mounts):
    """Recursively get stat of the mountpoint

    Args:
        mounts (list): List of all GlusterMount objs.

    Returns:
        bool: True if recursively getting stat from all mounts is successful.
            False otherwise.
    """
    if isinstance(mounts, GlusterMount):
        mounts = [mounts]

    g.log.info("Start getting stat of the mountpoint recursively")
    all_mounts_procs = []
    for mount_obj in mounts:
        g.log.info("Stat of mount %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        cmd = "find %s | xargs stat" % (mount_obj.mountpoint)
        proc = g.run_async(mount_obj.client_system, cmd, user=mount_obj.user)
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
    if isinstance(mounts, GlusterMount):
        mounts = [mounts]

    ignore_dirs_list = [".trashcan"]
    ignore_dirs = r"\|".join(ignore_dirs_list)

    g.log.info("Start Listing mounts files and dirs")
    all_mounts_procs = []
    for mount_obj in mounts:
        g.log.info("Listing files and dirs on %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        cmd = "find %s | grep -ve '%s'" % (mount_obj.mountpoint, ignore_dirs)
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


def view_snaps_from_mount(mounts, snaps):
    """View snaps from the mountpoint under ".snaps" directory

    Args:
        mounts (list): List of all GlusterMount objs.
        snaps (list): List of snaps to be viewed from '.snaps' directory

    Returns:
        bool: True if viewing all snaps under '.snaps' directory is successful
            from all mounts. False otherwise
    """
    if isinstance(mounts, GlusterMount):
        mounts = [mounts]

    if isinstance(snaps, str):
        snaps = [snaps]

    all_mounts_procs = []
    for mount_obj in mounts:
        g.log.info("Viewing '.snaps' on %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        cmd = "ls -1 %s/.snaps" % mount_obj.mountpoint
        proc = g.run_async(mount_obj.client_system, cmd, user=mount_obj.user)
        all_mounts_procs.append(proc)

    _rc = True
    for i, proc in enumerate(all_mounts_procs):
        ret, out, err = proc.async_communicate()
        if ret != 0:
            g.log.error("Failed to list '.snaps' on %s:%s - %s",
                        mounts[i].client_system, mounts[i].mountpoint, err)
            _rc = False
        else:
            snap_list = out.splitlines()
            if not snap_list:
                g.log.error("No snaps present in the '.snaps' dir on %s:%s",
                            mounts[i].client_system, mounts[i].mountpoint)
                _rc = False
                continue

            for snap_name in snaps:
                if snap_name not in snap_list:
                    g.log.error("Failed to list snap %s in '.snaps' dir on "
                                "%s:%s - %s", snap_name,
                                mounts[i].client_system, mounts[i].mountpoint,
                                snap_list)
                    _rc = False
                else:
                    g.log.info("Successful listed snap %s in '.snaps' dir on "
                               "%s:%s - %s", snap_name,
                               mounts[i].client_system, mounts[i].mountpoint,
                               snap_list)
    return _rc


def validate_io_procs(all_mounts_procs, mounts):
    """Validate whether IO was successful or not.

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

    if isinstance(mounts, GlusterMount):
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
    if _rc:
        g.log.info("IO is successful on all mounts")
        return True
    return False


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

    if isinstance(mounts, GlusterMount):
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
    if isinstance(mounts, GlusterMount):
        mounts = [mounts]

    g.log.info("Start cleanup mounts")
    all_mounts_procs = []
    valid_mounts = []
    for mount_obj in mounts:
        g.log.info("Cleaning up data from %s:%s", mount_obj.client_system,
                   mount_obj.mountpoint)
        if (not mount_obj.mountpoint or (os.path.realpath(os.path.abspath(
                mount_obj.mountpoint)) == '/')):
            g.log.error("%s on %s is not a valid mount point",
                        mount_obj.mountpoint, mount_obj.client_system)
            continue
        cmd = "rm -rf %s/*" % (mount_obj.mountpoint)
        proc = g.run_async(mount_obj.client_system, cmd, user=mount_obj.user)
        all_mounts_procs.append(proc)
        valid_mounts.append(mount_obj)
    g.log.info("rm -rf on all clients is complete. Validating deletion now...")

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
    ignore_dirs = r"\|".join(ignore_dirs_list)
    all_mounts_procs = []
    for mount_obj in mounts:
        cmd = ("find %s -mindepth 1 | grep -ve '%s'" %
               (mount_obj.mountpoint, ignore_dirs))
        proc = g.run_async(mount_obj.client_system, cmd, user=mount_obj.user)
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


def run_bonnie(servers, directory_to_run, username="root"):
    """Run bonnie test suite on the given servers.

    Args:
        servers (list): servers in which tests to be run.
        directory_to_run (list): directory path where tests will run for
         each server.

    Kwargs:
        username (str): username. Defaults to root.

    Returns:
        bool: True, if test passes in all servers, False otherwise

    Example:
        run_bonnie(["abc.com", "def.com"], ["/mnt/test1", "/mnt/test2"])
    """

    g.log.info("Running bonnie tests on %s" % ','.join(servers))
    rt = True
    options_for_each_servers = []

    # Install bonnie test suite if not installed
    results = g.run_parallel(servers, "yum list installed bonnie++")
    for index, server in enumerate(servers):
        if results[server][0] != 0:
            ret, out, _ = g.run(server,
                                "yum list installed bonnie++ || "
                                "yum -y install bonnie++")
            if ret != 0:
                g.log.error("Failed to install bonnie on %s" % server)
                return False

        # Building options for bonnie tests
        options_list = []
        options = ""
        freemem_command = "free -g | grep Mem: | awk '{ print $2 }'"
        ret, out, _ = g.run(server, freemem_command)
        memory = int(out)
        g.log.info("Memory = %i", memory)
        options_list.append("-d %s -u %s" % (directory_to_run[index],
                                             username))
        if memory >= 8:
            options_list.append("-r 16G -s 16G -n 0 -m TEST -f -b")

        options = " ".join(options_list)
        options_for_each_servers.append(options)

    proc_list = []
    for index, server in enumerate(servers):
        bonnie_command = "bonnie++ %s" % (options_for_each_servers[index])
        proc = g.run_async(server, bonnie_command)
        proc_list.append(proc)

    for index, proc in enumerate(proc_list):
        results = proc.async_communicate()
        if results[0] != 0:
            g.log.error("Bonnie test failed on server %s" % servers[index])
            rt = False

    for index, server in enumerate(servers):
        ret, out, _ = g.run(server, "rm -rf %s/Bonnie.*"
                            % directory_to_run[index])
        if ret != 0:
            g.log.error("Failed to remove files from %s" % server)
            rt = False

    for server in servers:
        ret, out, _ = g.run(server, "yum -y remove bonnie++")
        if ret != 0:
            g.log.error("Failed to remove bonnie from %s" % server)
            return False
    return rt


def run_fio(servers, directory_to_run):
    """Run fio test suite on the given servers.

    Args:
        servers (list): servers in which tests to be run.
        directory_to_run (list): directory path where tests will run for
         each server.

    Returns:
        bool: True, if test passes in all servers, False otherwise

    Example:
        run_fio(["abc.com", "def.com"], ["/mnt/test1", "/mnt/test2"])
    """

    g.log.info("Running fio tests on %s" % ','.join(servers))
    rt = True

    # Installing fio if not installed
    results = g.run_parallel(servers, "yum list installed fio")
    for index, server in enumerate(servers):
        if results[server][0] != 0:
            ret, out, _ = g.run(server,
                                "yum list installed fio || "
                                "yum -y install fio")
            if ret != 0:
                g.log.error("Failed to install fio on %s" % server)
                return False

        # building job file for running fio
        # TODO: parametrizing the fio and to get input values from user
        job_file = "/tmp/fio_job.ini"
        cmd = ("echo -e '[global]\nrw=randrw\nio_size=1g\nfsync_on_close=1\n"
               "size=4g\nbs=64k\nrwmixread=20\nopenfiles=1\nstartdelay=0\n"
               "ioengine=sync\n[write]\ndirectory=%s\nnrfiles=1\n"
               "filename_format=fio_file.$jobnum.$filenum\nnumjobs=8' "
               "> %s" % (directory_to_run[index], job_file))

        ret, _, _ = g.run(server, cmd)
        if ret != 0:
            g.log.error("Failed to create fio job file")
            rt = False

    proc_list = []
    for index, server in enumerate(servers):
        fio_command = "fio %s" % (job_file)
        proc = g.run_async(server, fio_command)
        proc_list.append(proc)

    for index, proc in enumerate(proc_list):
        results = proc.async_communicate()
        if results[0] != 0:
            g.log.error("fio test failed on server %s" % servers[index])
            rt = False

    for index, server in enumerate(servers):
        ret, out, _ = g.run(server, "rm -rf %s/fio_file.*"
                            % directory_to_run[index])
        if ret != 0:
            g.log.error("Failed to remove files from %s" % server)
            rt = False

    for index, server in enumerate(servers):
        ret, out, _ = g.run(server, "rm -rf %s" % job_file)
        if ret != 0:
            g.log.error("Failed to remove job file from %s" % server)
            rt = False

    for server in servers:
        ret, out, _ = g.run(server, "yum -y remove fio")
        if ret != 0:
            g.log.error("Failed to remove fio from %s" % server)
            return False
    return rt


def run_mixed_io(servers, io_tools, directory_to_run):
    """Run different io patterns on each given servers.

    Args:
        servers (list): servers in which tests to be run.
        io_tools (list): different io tools. Currently fio, bonnie are
            supported.
        directory_to_run (list): directory path where tests will run for
            each server.

    Returns:
        bool: True, if test passes in all servers, False otherwise

    Example:
        run_mixed_io(["abc.com", "def.com"], ["/mnt/test1", "/mnt/test2"])
    """

    g.log.info("Running mixed IO tests on %s" % ','.join(servers))

    # Assigning IO tool to each server in round robin way
    if len(servers) > len(io_tools):
        for index, tool in enumerate(io_tools):
            io_tools.append(io_tools[index])
            if len(servers) == len(io_tools):
                break
    server_io_dict = {}
    for items in zip(servers, io_tools):
        server_io_dict[items[0]] = items[1]

    io_dict = {'fio': run_fio, 'bonnie': run_bonnie}

    func_list = []
    for index, server in enumerate(servers):
        tmp_list = ([server], [directory_to_run[index]])
        tmp_list_item = (io_dict[server_io_dict[server]], tmp_list)
        func_list.append(tmp_list_item)

    pool = Pool()
    results = []
    ret = True
    for func, func_args in func_list:
        results.append(pool.apply_async(func, func_args))
    for result in results:
        ret = ret & result.get()
    pool.terminate()
    return ret


def is_io_procs_fail_with_rofs(self, all_mounts_procs, mounts):
    """Check whether IO failed with Read-only file system error.

    Args:
        all_mounts_procs (list): List of open connection descriptor as
                                 returned by g.run_async method.
        mounts (list): List of all GlusterMount objs on which process were
                       started.

    Returns:
        tuple : Tuple containing two elements (ret, io_results).
        The first element 'ret' is of type 'bool', True if
        IO failed with ROFS on all mount procs. False otherwise.

        The second element 'io_results' is of type dictonary and it
        contains the proc and corresponding result for IO. If IO failed with
        ROFS, then proc value contains True else False.
    """
    if isinstance(all_mounts_procs, subprocess.Popen):
        all_mounts_procs = [all_mounts_procs]

    if isinstance(mounts, GlusterMount):
        mounts = [mounts]

    io_results = {}
    for i, proc in enumerate(all_mounts_procs):
        g.log.info("Validating IO on %s:%s", self.mounts[i].client_system,
                   self.mounts[i].mountpoint)
        ret, out, err = proc.async_communicate()
        if ret != 0:
            g.log.info("EXPECTED : IO Failed on %s:%s",
                       self.mounts[i].client_system,
                       self.mounts[i].mountpoint)
            if ("Read-only file system" in err
                    or "Read-only file system" in out):
                g.log.info("EXPECTED : Read-only file system in output")
                io_results[proc] = True
            else:
                g.log.error("Read-only file system error not found in output")
                io_results[proc] = False
        else:
            g.log.error("IO Successful on Read-only file system %s:%s",
                        self.mounts[i].client_system,
                        self.mounts[i].mountpoint)
            io_results[proc] = False
    ret = all(io_results.values())

    return ret, io_results


def is_io_procs_fail_with_error(self, all_mounts_procs, mounts, mount_type):
    """Check whether IO failed with connection error.

    Args:
        all_mounts_procs (list): List of open connection descriptor as
                                 returned by g.run_async method.
        mounts (list): List of all GlusterMount objs on which process were
                       started.
        mount_type (str): Type of mount

    Returns:
        tuple : Tuple containing two elements (ret, io_results).
        The first element 'ret' is of type 'bool', True if
        IO failed with connection error on all mount procs. False otherwise.

        The second element 'io_results' is of type dictonary and it
        contains the proc and corresponding result for IO. If IO failed with
        connection error, then proc value contains True else False.
    """
    if isinstance(all_mounts_procs, subprocess.Popen):
        all_mounts_procs = [all_mounts_procs]

    if isinstance(mounts, GlusterMount):
        mounts = [mounts]

    io_results = {}
    for i, proc in enumerate(all_mounts_procs):
        g.log.info("Validating IO on %s:%s", self.mounts[i].client_system,
                   self.mounts[i].mountpoint)
        ret, out, err = proc.async_communicate()
        if ret != 0:
            g.log.info("EXPECTED : IO Failed on %s:%s",
                       self.mounts[i].client_system,
                       self.mounts[i].mountpoint)
            if mount_type == "glusterfs":
                if ("Transport endpoint is not connected" in err
                        or "Transport endpoint is not connected" in out):
                    g.log.info("EXPECTED : Transport endpoint is not connected"
                               " in output")
                    io_results[proc] = True
                else:
                    g.log.error(
                        "Transport endpoint is not connected error "
                        "not found in output")
                    io_results[proc] = False
            if mount_type == "nfs":
                if "Input/output error" in err or "Input/output error" in out:
                    g.log.info("EXPECTED : Input/output error in output")
                    io_results[proc] = True
                else:
                    g.log.error(
                        "Input/output error error not found in output")
                    io_results[proc] = False
        else:
            g.log.error("IO Successful on not connected mountpoint %s:%s",
                        self.mounts[i].client_system,
                        self.mounts[i].mountpoint)
            io_results[proc] = False
    ret = all(io_results.values())

    return ret, io_results


def compare_dir_structure_mount_with_brick(mnthost, mntloc, brick_list, type):
    """Compare mount point dir structure with brick path along with stat param..

    Args:
        mnthost (str): hostname or ip of mnt system
        mntloc (str) : mount location of gluster file system
        brick_list (list) : list of all brick ip's with brick path
        type  (int) : 0 represent user permission
                    : 1 represent group permission
                    : 2 represent access permission
    Returns:
        True if directory structure are same
        False if structure is not same
    """

    statformat = ''
    if type == 0:
        statformat = '%U'
    if type == 1:
        statformat = '%G'
    if type == 2:
        statformat = '%A'

    command = "find %s -mindepth 1 -type d | xargs -r stat -c '%s'" % (
        mntloc, statformat)
    rcode, rout, _ = g.run(mnthost, command)
    all_dir_mnt_perm = rout.strip().split('\n')

    for brick in brick_list:
        brick_node, brick_path = brick.split(":")
        command = ("find %s -mindepth 1 -type d | grep -ve \".glusterfs\" | "
                   "xargs -r stat -c '%s'" % (brick_path, statformat))
        rcode, rout, _ = g.run(brick_node, command)
        all_brick_dir_perm = rout.strip().split('\n')
        retval = (all_dir_mnt_perm > all_brick_dir_perm) - (
            all_dir_mnt_perm < all_brick_dir_perm)
        if retval != 0:
            return False

    return True


def check_arequal_bricks_replicated(mnode, volname):
    """Collects arequal from all the bricks in subvol and compare it
       with first brick in subvols.

    Args:
        mnode : Node on which commands are executed
        volname : Name of the volume

    Returns:
        Returns:
        bool: True if arequal of all the bricks in the subvolume are same.
        False otherwise.
    """
    # Check arequals
    # get the subvolumes
    g.log.info("Starting to get sub-volumes for volume %s", volname)
    subvols_dict = get_subvols(mnode, volname)
    num_subvols = len(subvols_dict['volume_subvols'])
    g.log.info("Number of subvolumes in volume %s:", num_subvols)

    # Get arequals and compare
    for i in range(0, num_subvols):
        # Get arequal for first brick
        subvol_brick_list = subvols_dict['volume_subvols'][i]
        node, brick_path = subvol_brick_list[0].split(':')
        command = ('arequal-checksum -p %s '
                   '-i .glusterfs -i .landfill -i .trashcan' % brick_path)
        ret, arequal, _ = g.run(node, command)
        if ret != 0:
            g.log.error("Failed to calculate arequal for first brick"
                        "of subvol %s of volume %s", i, volname)
            return False
        first_brick_total = arequal.splitlines()[-1].split(':')[-1]

        # Get arequal for every brick and compare with first brick
        for brick in subvol_brick_list[1:]:
            node, brick_path = brick.split(':')
            command = ('arequal-checksum -p %s '
                       '-i .glusterfs -i .landfill -i .trashcan' % brick_path)
            ret, brick_arequal, _ = g.run(node, command)
            if ret != 0:
                g.log.error('Failed to get arequal on brick %s' % brick)
                return False
            g.log.info('Getting arequal for %s is successful', brick)
            brick_total = brick_arequal.splitlines()[-1].split(':')[-1]
            # compare arequal of first brick of subvol with all brick other
            # bricks in subvol
            if first_brick_total != brick_total:
                g.log.error('Arequals for subvol and %s are not equal' % brick)
                return False
            g.log.info('Arequals for subvol and %s are equal', brick)
    g.log.info('All arequals are equal for volume %s', volname)
    return True


def run_crefi(client, mountpoint, number, breadth, depth, thread=5,
              random_size=False, fop='create', filetype='text',
              minfs=10, maxfs=500, single=False, multi=False, size=100,
              interval=100, nameBytes=10, random_filename=True):
    """Run crefi on a given mount point and generate I/O.

    Args:
        client(str): Client on which I/O has to be performed.
        mountpoint(str): Mount point where the client is mounted.
        number(int): Number of files to be created.
        breadth(int): Number of directories in one level.
        depth(int): Number of levels of directories.

    Kwargs:
        thread(int): Number of threads used to generate fop.
        random_size(bool): Random size of the file between min and max.
        fop(str): fop can be [create|rename|chmod|chown|chgrp|symlink|hardlink|
                  truncate|setxattr] this specifies the type of fop to be
                  executed by default it is create.
        filetype(str): filetype can be [text|sparse|binary|tar] this specifies
                       the type of file by default it is text.
        minfs(int): If random is set to true then this value has to be altered
                    to change minimum file size. (Value is in KB)
        maxfs(int): If random is set to true then this value has to be altered
                    to change maximum file size. (Value is in KB)
        single(bool): Create files in a single directory.
        multi(bool): Create files in sub-dir and sub-sub dir.
        size(int): Size of the files to be created. (Value is in KB)
        interval(int): Print number files created of interval.
        nameBytes(int): Number of bytes for filename. (Value is in Bytes)
        random_filename(bool): It creates files with random names, if set to
                               False it creates files with file name file1,
                               file2 and so on.

    Returns:
        bool: True if I/O was sucessfully otherwise False.

    NOTE:
        To use this function it is a prerequisite to have crefi installed
        on all the clients. Please use the below command to install it:
        $ pip install crefi
        $ pip install pyxattr
    """

    # Checking value of fop.
    list_of_fops = ["create", "rename", "chmod", "chown", "chgrp", "symlink",
                    "hardlink", "truncate", "setxattr"]
    if fop not in list_of_fops:
        g.log.error("fop value is not valid.")
        return False

    # Checking value of filetype.
    list_of_filetypes = ["text", "sparse", "binary", "tar"]
    if filetype not in list_of_filetypes:
        g.log.error("filetype is not a valid file type.")
        return False

    # Checking if single and multi both are set to true.
    if single and multi:
        g.log.error("single and mutli both can't be true.")
        return False

    # Checking if file size and random size arguments are given together.
    if (size > 100 or size < 100) and random_size:
        g.log.error("Size and Random size can't be used together.")
        return False

    # Checking if minfs is greater than or equal to maxfs.
    if random_size and (minfs >= maxfs):
        g.log.error("minfs shouldn't be greater than or equal to maxfs.")
        return False

    # Creating basic command.
    command = "crefi %s -n %s -b %s -d %s " % (
        mountpoint, number, breadth, depth)

    # Checking thread value and adding it, If it is greater or smaller than 5.
    if thread > 5 or thread < 5:
        command = command + ("-T %s " % thread)

    # Checking if random size is true or false.
    if random_size:
        command = command + "--random "
        if minfs > 10 or minfs < 10:
            command = command + ("--min %s " % minfs)
        if maxfs > 500 or maxfs < 500:
            command = command + ("--max %s " % maxfs)

    # Checking fop and adding it if not create.
    if fop != "create":
        command = command + ("--fop %s " % fop)

    # Checking if size if greater than or less than 100.
    if size > 100 or size < 100:
        command = command + ("--size %s " % size)

    # Checking if single or mutli is true.
    if single:
        command = command + "--single "
    if multi:
        command = command + "--multi "

    # Checking if random_filename is false.
    if not random_filename:
        command = command + "-R "

    # Checking if print interval is greater than or less than 100.
    if interval > 100 or interval < 100:
        command = command + ("-I %s " % interval)

    # Checking if name Bytes is greater than or less than 10.
    if nameBytes > 10 or nameBytes < 10:
        command = command + ("-l %s " % nameBytes)

    # Checking filetype and setting it if not
    # text.
    if filetype != "text":
        command = command + ("-t %s " % filetype)

    # Running the command on the client node.
    ret, _, _ = g.run(client, command)
    if ret:
        g.log.error("Failed to run crefi on %s." % client)
        return False
    return True


def run_cthon(mnode, volname, clients, dir_name):
    """This function runs the cthon test suite.

    Args:
        mnode (str) : IP of the server exporting the gluster volume.
        volname (str) : The volume name.
        clients (list) : List of client machines where
                         the test needs to be run.
        dir_name (str) : Directory where the repo
                         is cloned.

    Returns:
        bool : True if the cthon test passes successfully
            False otherwise.
    """
    param_list = ['-b', '-g', '-s', '-l']
    vers_list = ['4.0', '4.1']

    for client in clients:
        g.log.info("Running tests on client %s" % client)
        for vers in vers_list:
            g.log.info("Running tests on client version %s" % vers)
            for param in param_list:
                # Initialising the test_type that will be running
                if param == '-b':
                    test_type = "Basic"
                elif param == '-g':
                    test_type = "General"
                elif param == '-s':
                    test_type = "Special"
                else:
                    test_type = "Lock"
                g.log.info("Running %s test" % test_type)
                cmd = "cd /root/%s; ./server %s -o vers=%s -p %s -N 1 %s;" % (
                    dir_name, param, vers, volname, mnode)
                ret, _, _ = g.run(client, cmd)
                if ret:
                    g.log.error("Error with %s test" % test_type)
                    return False
                else:
                    g.log.info("%s test successfully passed" % test_type)
    return True


def upload_file_dir_ops(clients):
    """Upload file_dir_ops.py to all the clients.

    Args:
      clients(list): List of client machines where we need to upload
                     the script.

    Returns:
        bool: True if script is uploaded successfully
              False otherwise.
    """

    g.log.info("Upload io scripts to clients %s for running IO on "
               "mounts", clients)
    file_dir_ops_path = ("/usr/share/glustolibs/io/scripts/"
                         "file_dir_ops.py")

    if not upload_scripts(clients, file_dir_ops_path):
        g.log.error("Failed to upload IO scripts to clients %s" %
                    clients)
        return False

    g.log.info("Successfully uploaded IO scripts to clients %s",
               clients)
    return True


def open_file_fd(mountpoint, time, client, start_range=0,
                 end_range=0):
    """Open FD for a file and write to file.

    Args:
      mountpoint(str): The mount point where the FD of file is to
                       be opened.
      time(int): The time to wait after opening an FD.
      client(str): The client from which FD is to be opened.

    Kwargs:
        start_range(int): The start range of the open FD.
                          (Default: 0)
        end_range(int): The end range of the open FD.
                        (Default: 0)

    Returns:
      proc(object): Returns a process object

    NOTE:
    Before opening FD, check the currently used fds on the
    system as only a limited number of fds can be opened on
    a system at a given time for each process.
    """
    if not (start_range and end_range):
        cmd = ("cd {}; exec 30<> file_openfd ; sleep {};"
               "echo 'xyz' >&30".format(mountpoint, time))
    else:
        cmd = ('cd {}; for i in `seq {} {}`;'
               ' do eval "exec $i<>file_openfd$i"; sleep {};'
               ' echo "Write to open FD" >&$i; done'.format(
                   mountpoint, start_range, end_range, time))
    proc = g.run_async(client, cmd)
    return proc


def run_linux_untar(clients, mountpoint, dirs=('.')):
    """Run linux kernal untar on a given mount point

    Args:
      clients(str|list): Client nodes on which I/O
                         has to be started.
      mountpoint(str): Mount point where the volume is
                       mounted.
    Kwagrs:
       dirs(tuple): A tuple of dirs where untar has to
                    started. (Default:('.'))
    Returns:
       list: Returns a list of process object else None
    """
    # Checking and convering clients to list.
    if not isinstance(clients, list):
        clients = [clients]

    list_of_procs = []
    for client in clients:
        # Download linux untar to root, so that it can be
        # utilized in subsequent run_linux_untar() calls.
        cmd = ("wget https://cdn.kernel.org/pub/linux/kernel/"
               "v5.x/linux-5.4.54.tar.xz")
        if not file_exists(client, '/root/linux-5.4.54.tar.xz'):
            ret, _, _ = g.run(client, cmd)
            if ret:
                return None

        for directory in dirs:
            # copy linux tar to dir
            cmd = ("cp /root/linux-5.4.54.tar.xz {}/{}"
                   .format(mountpoint, directory))
            ret, _, _ = g.run(client, cmd)
            if ret:
                return None
            # Start linux untar
            cmd = ("cd {}/{};tar -xvf linux-5.4.54.tar.xz"
                   .format(mountpoint, directory))
            proc = g.run_async(client, cmd)
            list_of_procs.append(proc)

    return list_of_procs
