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
from glustolibs.gluster.mount_ops import GlusterMount
from multiprocessing import Pool


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
    if isinstance(mounts, GlusterMount):
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
    if isinstance(mounts, GlusterMount):
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
    if isinstance(mounts, GlusterMount):
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
    if isinstance(mounts, GlusterMount):
        mounts = [mounts]

    ignore_dirs_list = [".trashcan"]
    ignore_dirs = r"\|".join(ignore_dirs_list)

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
        cmd = ("ls -1 %s/.snaps" % mount_obj.mountpoint)
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
    ignore_dirs = r"\|".join(ignore_dirs_list)
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


def run_bonnie(servers, directory_to_run, username="root"):
    """
    Module to run bonnie test suite on the given servers.

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
    """
    Module to run fio test suite on the given servers.

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
                g.log.error("Failed to install bonnie on %s" % server)
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
    """
    Module to run different io patterns on each given servers.

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

    io_dict = {'fio': run_fio,
               'bonnie': run_bonnie}

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
    """
    Checks whether IO failed with Read-only file system error

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
            if ("Read-only file system" in err or
                    "Read-only file system" in out):
                g.log.info("EXPECTED : Read-only file system in output")
                io_results[proc] = True
            else:
                g.log.error("Read-only file system error not found in output")
                io_results[proc] = False
        else:
            g.log.error("IO Successful on Read-only file system %s:%s",
                        self.mounts[i].client_system,
                        self.mounts[i].mountpoint)
    ret = all(io_results.values())

    return ret, io_results


def compare_dir_structure_mount_with_brick(mnthost, mntloc, brick_list, type):
    """ Compare directory structure from mount point with  brick path along
        with stat parameter

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

    command = ("find %s -mindepth 1 -type d | xargs -r stat -c '%s'"
               % (mntloc, statformat))
    rcode, rout, _ = g.run(mnthost, command)
    all_dir_mnt_perm = rout.strip().split('\n')

    for brick in brick_list:
        brick_node, brick_path = brick.split(":")
        command = ("find %s -mindepth 1 -type d | grep -ve \".glusterfs\" | "
                   "xargs -r stat -c '%s'" % (brick_path, statformat))
        rcode, rout, _ = g.run(brick_node, command)
        all_brick_dir_perm = rout.strip().split('\n')
        retval = cmp(all_dir_mnt_perm, all_brick_dir_perm)
        if retval != 0:
            return False

    return True
