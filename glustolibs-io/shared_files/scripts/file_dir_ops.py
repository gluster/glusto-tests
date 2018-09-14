#!/usr/bin/env python
#  Copyright (C) 2015-2018  Red Hat, Inc. <http://www.redhat.com>
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
    Description: Helpers for performing file/dir operations
"""

from __future__ import print_function
import os
import argparse
import sys
import random
import string
import datetime
from multiprocessing import Process
import subprocess
from docx import Document
import contextlib
import platform
import shutil

if platform.system() == "Windows":
    path_sep = "\\"
elif platform.system() == "Linux":
    path_sep = "/"


def is_root(path):
    """Check whether the given path is '/' or not

    Args:
        path (str):  Path of the dir to check

    Returns:
        True if path is '/' , False otherwise
    """
    if os.path.realpath(os.path.abspath(path)) is '/':
        print ("Directory '%s' is the root of filesystem. "
               "Not performing any operations on the root of filesystem" %
               os.path.abspath(path))
        return True
    else:
        return False


def path_exists(path):
    """Check if path exists are not.

    Args:
        path (str): Path to check if it exist or not.

    Returns:
        bool : True if path exists, False otherwise.
    """
    if os.path.exists(os.path.abspath(path)):
        return True
    else:
        return False


@contextlib.contextmanager
def open_file_to_write(filename=None):
    """Opens filename to write if not None else writes to stdout.
    """
    if filename:
        fh = open(filename, 'w')
    else:
        fh = sys.stdout

    try:
        yield fh
    finally:
        if fh is not sys.stdout:
            fh.close()


def _get_current_time():
    return datetime.datetime.now().strftime("%I:%M:%S:%p:%b_%d_%Y")


def create_dir(dir_path):
    """Create dir if 'dir_path' does not exists

    Args:
        dir_path (str): Directory path to create

    Returns:
        0 on successful creation of dir, 1 otherwise.
    """
    dir_abs_path = os.path.abspath(dir_path)
    if not path_exists(dir_abs_path):
        try:
            os.makedirs(dir_abs_path)
        except (OSError, IOError):
            print ("Unable to create dir: %s" % dir_abs_path)
            return 1
    return 0


def create_dirs(dir_path, depth, num_of_dirs, num_of_files=0,
                fixed_file_size=None, base_file_name='testfile',
                file_types='txt'):
    """Recursively creates dirs under the dir_path with specified depth
        and num_of_dirs in each level

    Args:
        dir_path (str): Directory under which sub-dirs to be created
        depth (int): Depth of the directory from the first dir_path
        num_of_dirs (int): Number of directories to be created in each level

    Kwargs:
        num_of_files (int): Number of files to be created in each dir.
            Defaults to 0.
        fixed_file_size (str): If creating fixed sized files on all dirs.
            Defaults to None.
        base_file_name (str): base name of the file to be created.
        file_types (str): file types to be created.
    """
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            if num_of_files != 0:
                _create_files(dir_path, num_of_files, fixed_file_size,
                              base_file_name, file_types)
        except (OSError, IOError) as e:
            if 'File exists' not in e.strerror:
                print ("Unable to create dir '%s' : %s"
                       % (dir_path, e.strerror))
                with open("/tmp/file_dir_ops_create_dirs_rc", "w") as fd:
                    try:
                        fd.write("1")
                        fd.flush()
                        fd.close()
                    except IOError as e:
                        print ("Unable to write the rc to the "
                               "/tmp/file_dir_ops_create_dirs_rc file")
    if depth == 0:
        return 0
    for i in range(num_of_dirs):
        dirname = "dir%d" % i
        create_dirs(os.path.join(dir_path, dirname), depth - 1, num_of_dirs,
                    num_of_files, fixed_file_size)


def create_deep_dirs(args):
    """Creates Deep Directories of specified length, depth and number of dirs
        in each level under 'dir'.
    """
    dir_path = os.path.abspath(args.dir)
    dir_depth = args.dir_depth
    dir_length = args.dir_length
    max_num_of_dirs = args.max_num_of_dirs
    dirname_start_num = args.dirname_start_num

    # Check if dir_path is '/'
    if is_root(dir_path):
        return 1

    # Create dir_path
    rc = create_dir(dir_path)
    if rc != 0:
        return rc

    # Remove the file which saves the rc if already exists
    if os.path.exists("/tmp/file_dir_ops_create_dirs_rc"):
        os.remove("/tmp/file_dir_ops_create_dirs_rc")

    process_list = []
    for i in range(dirname_start_num, (dirname_start_num + dir_length)):
        num_of_dirs = random.choice(range(1, max_num_of_dirs + 1))
        process_dir_path = os.path.join(dir_path, "user%d" % i)
        process_list.append(Process(target=create_dirs,
                                    args=(process_dir_path, dir_depth,
                                          num_of_dirs)))
    for each_process in process_list:
        each_process.start()

    for each_process in process_list:
        each_process.join()

    rc = 0
    if os.path.exists("/tmp/file_dir_ops_create_dirs_rc"):
        fd = open("/tmp/file_dir_ops_create_dirs_rc", "r")
        rc = fd.read()
        fd.close()
        os.remove("/tmp/file_dir_ops_create_dirs_rc")
    return int(rc)


def create_deep_dirs_with_files(args):
    """Creates Deep Directories of specified length, depth ,number of dirs
        in each level, number of files, with fixed size or
        random size, and with specified basename of the file
        in each directory under 'dir'.
    """
    dir_path = os.path.abspath(args.dir)
    dir_depth = args.dir_depth
    dir_length = args.dir_length
    max_num_of_dirs = args.max_num_of_dirs
    num_of_files = args.num_of_files
    file_types = args.file_types
    try:
        fixed_file_size = args.fixed_file_size
    except AttributeError:
        fixed_file_size = None
    base_file_name = args.base_file_name
    dirname_start_num = args.dirname_start_num

    # Check if dir_path is '/'
    if is_root(dir_path):
        return 1

    # Create dir_path
    rc = create_dir(dir_path)
    if rc != 0:
        return rc

    # Remove the file which saves the rc if already exists
    if os.path.exists("/tmp/file_dir_ops_create_dirs_rc"):
        os.remove("/tmp/file_dir_ops_create_dirs_rc")

    process_list = []
    for i in range(dirname_start_num, (dirname_start_num + dir_length)):
        num_of_dirs = random.choice(range(1, max_num_of_dirs + 1))
        process_dir_path = os.path.join(dir_path, "user%d" % i)
        process_list.append(Process(target=create_dirs,
                                    args=(process_dir_path, dir_depth,
                                          num_of_dirs, num_of_files,
                                          fixed_file_size, base_file_name,
                                          file_types)))
    for each_process in process_list:
        each_process.start()

    for each_process in process_list:
        each_process.join()
    rc = 0
    if os.path.exists("/tmp/file_dir_ops_create_dirs_rc"):
        fd = open("/tmp/file_dir_ops_create_dirs_rc", "r")
        rc = fd.read()
        fd.close()
        os.remove("/tmp/file_dir_ops_create_dirs_rc")
    return int(rc)


def _create_files(dir_path, num_of_files, fixed_file_size=None,
                  base_file_name='testfile', file_types='txt'):
    rc = 0
    file_types_list = file_types.split()
    file_sizes_dict = {
        '1k': 1024,
        '10k': 10240,
        '512k': 524288,
        '1M': 1048576
        }

    # Create dir_path
    rc = create_dir(dir_path)
    if rc != 0:
        return rc

    for count in range(num_of_files):
        fname = base_file_name + str(count)
        fname_abs_path = os.path.join(dir_path, fname)
        if fixed_file_size is None:
            file_size = (
                file_sizes_dict[random.choice(list(file_sizes_dict.keys()))])
        else:
            try:
                file_size = file_sizes_dict[fixed_file_size]
            except KeyError as e:
                print ("File sizes can be [1k, 10k, 512k, 1M]")
                return 1

        type = random.choice(file_types_list)
        if type == 'txt':
            fname_abs_path = fname_abs_path + ".txt"

            with open(fname_abs_path, "w+") as fd:
                try:
                    fd.write(''.join(random.choice(string.printable) for x in
                                     range(file_size)))
                    fd.flush()
                    fd.close()
                except IOError as e:
                    print ("Unable to write to file '%s' : %s" %
                           (fname_abs_path, e.strerror))
                    rc = 1
        elif type == 'docx':
            fname_abs_path = fname_abs_path + ".docx"
            try:
                document = Document()
                str_to_write = string.ascii_letters + string.digits
                file_str = (''.join(random.choice(str_to_write)
                                    for x in range(file_size)))
                document.add_paragraph(file_str)
                document.save(fname_abs_path)
            except Exception as e:
                print ("Unable to write to file '%s' : %s" %
                       (fname_abs_path, e.strerror))
                rc = 1
        elif type == 'empty_file':
            try:
                with open(fname_abs_path, "w+") as fd:
                    fd.close()
            except IOError as e:
                print ("Unable to write to file '%s' : %s" %
                       (fname_abs_path, e.strerror))
                rc = 1
    return rc


def create_files(args):
    """Create specified num_of_files in each dir with fixed size or
        random size, and with specified basename of the file under 'dir'
    """
    dir_path = os.path.abspath(args.dir)
    num_of_files = args.num_of_files
    try:
        fixed_file_size = args.fixed_file_size
    except AttributeError:
        fixed_file_size = None
    base_file_name = args.base_file_name
    file_types = args.file_types

    # Check if dir_path is '/'
    if is_root(dir_path):
        return 1

    # Create dir_path
    rc = create_dir(dir_path)
    if rc != 0:
        return rc

    rc = 0
    for dirName, subdirList, fileList in os.walk(dir_path, topdown=False):
        _rc = _create_files(dirName, num_of_files, fixed_file_size,
                            base_file_name, file_types)
        if _rc != 0:
            rc = 1
    return rc


def rename(args):
    """Recursively rename all the files/dirs under 'dir' to
        "'filename'/'dirname' + '_postfix'".
    """
    dir_path = os.path.abspath(args.dir)
    postfix = args.postfix

    # Check if dir_path is '/'
    if is_root(dir_path):
        return 1

    # Check if dir_path exists
    if not path_exists(dir_path):
        print ("Directory '%s' does not exist" % dir_path)
        return 1

    rc = 0
    for dirName, subdirList, fileList in os.walk(dir_path, topdown=False):
        # rename files
        for fname in fileList:
            old = os.path.join(dirName, fname)
            new_fname, ext = os.path.splitext(fname)
            new = os.path.join(dirName, (new_fname + "_" + postfix + ext))
            try:
                os.rename(old, new)
            except OSError:
                rc = 1
                print ("Unable to rename %s -> %s" % (old, new))

        # rename dirs
        if dirName != dir_path:
            old = dirName
            new = dirName + "_" + postfix
            try:
                os.rename(old, new)
            except OSError:
                rc = 1
                print ("Unable to rename %s -> %s" % (old, new))
    return rc


def ls(args):
    """Recursively list all the files/dirs under 'dir'
    """
    dir_path = os.path.abspath(args.dir)
    log_file_name = args.log_file_name

    # Check if dir_path exists
    if not path_exists(dir_path):
        print ("Directory '%s' does not exist" % dir_path)
        return 1

    with open_file_to_write(log_file_name) as file_handle:
        if log_file_name:
            time_str = _get_current_time()
            file_handle.write("Starting 'ls -R' : %s" % time_str)
        for dirName, subdirList, fileList in os.walk(dir_path):
            file_handle.write('Dir: %s' % dirName)
            for dname in subdirList:
                file_handle.write('\t%s' % os.path.join(dirName, dname))
            for fname in fileList:
                file_handle.write('\t%s' % os.path.join(dirName, fname))
        if log_file_name:
            time_str = _get_current_time()
            file_handle.write("\tEnding 'ls -R' : %s" % time_str)
    return 0


def _get_path_stats(path):
    """Get the stat of a specified path.
    """
    rc = 0
    path = os.path.abspath(args.path)
    file_stats = {}
    file_stats = {}

    if platform.system() == "Linux":
        cmd = "stat -c " + "'%A %U %G' " + path
        subp = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True)
        out, err = subp.communicate()
        if subp.returncode != 0:
            rc = 1
        else:
            if out:
                out = out.decode()
                out = out.split(" ")
                file_stats['mode'] = out[0].strip()
                file_stats['user'] = out[1].strip()
                file_stats['group'] = out[2].strip()
            else:
                rc = 1
    try:
        stat = os.stat(path)
        file_stats.update({
            'atime': stat.st_atime,
            'mtime': stat.st_mtime,
            'ctime': stat.st_ctime,
            'inode': stat.st_ino,
            'stat': stat
            })
    except Exception:
        rc = 1
        err = "Unable to get the stat of path %s" % path

    return (rc, file_stats, err)


def get_path_stats(args):
    """Get file/dir Stat
    """
    path = os.path.abspath(args.path)
    recursive = args.recursive
    log_file_name = args.log_file_name

    # Check if dir_path exists
    if not path_exists(path):
        print ("PATH '%s' does not exist" % path)
        return 1

    file_stats = {}

    if os.path.isfile(path):
        file_stats[path] = (_get_path_stats(path))

    if os.path.isdir(path):
        if recursive:
            for dirName, subdirList, fileList in os.walk(path, topdown=False):
                file_stats[dirName] = (_get_path_stats(dirName))

                for fname in fileList:
                    fname_abs_path = os.path.join(dirName, fname)
                    file_stats[fname_abs_path] = (_get_path_stats(
                        fname_abs_path))
        else:
            file_stats[path] = (_get_path_stats(path))

    rc = 0

    with open_file_to_write(log_file_name) as file_handle:
        if log_file_name:
            time_str = _get_current_time()
            file_handle.write("Starting 'stat %s' : %s" % (
                path, time_str))
        for key in file_stats.keys():
            file_handle.write("\nFile: %s" % key)
            ret, file_stat, err = file_stats[key]
            if ret != 0:
                rc = 1
                file_handle.write("\t%s\n" % err)
            else:
                file_handle.write("\t%s\n" % file_stat)
        if log_file_name:
            time_str = _get_current_time()
            file_handle.write("Ending 'stat %s' : %s" % (
                path, time_str))
        file_handle.write("\n")

    return rc


def compress(args):
    """Compress each top level dirs and complete dir under
       destination directory
    """
    dir_path = os.path.abspath(args.dir)
    compress_type = args.compress_type
    dest_dir = args.dest_dir

    # Check if dir_path is '/'
    if is_root(dir_path):
        return 1

    # Check if dir_path exists
    if not path_exists(dir_path):
        print ("Directory '%s' does not exist" % dir_path)
        return 1

    # Create dir_path
    rc = create_dir(dest_dir)
    if rc != 0:
        return 1

    rc = 0
    dirs = [os.path.join(dir_path, name) for name in os.listdir(dir_path)
            if os.path.isdir(os.path.join(dir_path, name))]

    proc_list = []
    for each_dir in dirs:
        if compress_type == '7z':
            file_name = (dest_dir + path_sep +
                         os.path.basename(each_dir) + "_7z.7z")
            cmd = "7z a -t7z " + file_name + " " + each_dir
        elif compress_type == 'gzip':
            tmp_file_name = (dir_path + path_sep +
                             os.path.basename(each_dir) + "_tar.tar")
            file_name = (dest_dir + path_sep +
                         os.path.basename(each_dir) + "_tgz.tgz")
            cmd = ("7z a -ttar -so " + tmp_file_name + " " +
                   each_dir + " | 7z a -si " + file_name)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, shell=True)
        proc_list.append(proc)

    for proc in proc_list:
        proc.communicate()
        ret = proc.returncode
        if ret == 1:
            rc = 1

    if compress_type == '7z':
        file_name = dest_dir + path_sep + os.path.basename(dir_path) + "_7z.7z"
        cmd = "7z a -t7z " + file_name + " " + dir_path
    elif compress_type == 'gzip':
        tmp_file_name = (dest_dir + path_sep + os.path.basename(dir_path) +
                         "_tar.tar")
        file_name = (dest_dir + path_sep + os.path.basename(dir_path) +
                     "_tgz.tgz")
        cmd = ("7z a -ttar -so " + tmp_file_name + " " + dir_path +
               " | 7z a -si " + file_name)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    proc.communicate()
    ret = proc.returncode
    if ret == 1:
        rc = 1

    return rc


def uncompress(args):
    """UnCompress the given compressed file
    """
    compressed_file = os.path.abspath(args.compressed_file)
    dest_dir = args.dest_dir
    date_time = datetime.datetime.now().strftime("%I_%M%p_%B_%d_%Y")
    cmd = ("7z x " + compressed_file + " -o" + dest_dir + path_sep +
           "uncompress_" + date_time + " -y")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    proc.communicate()
    ret = proc.returncode
    if ret == 1:
        return 1

    return 0


def uncompress_dir(args):
    """UnCompress all compressed files in destination directory
    """
    dir_path = os.path.abspath(args.dir)
    dest_dir = args.dest_dir
    date_time = datetime.datetime.now().strftime("%I_%M%p_%B_%d_%Y")
    cmd = ("7z x " + dir_path + " -o" + dest_dir + path_sep +
           "uncompress_" + date_time + " -y")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    proc.communicate()
    ret = proc.returncode
    if ret == 1:
        return 1

    return 0


def create_hard_links(args):
    """Creates hard link"""
    src_dir = os.path.abspath(args.src_dir)
    dest_dir = args.dest_dir

    # Check if src_dir is '/'
    if is_root(src_dir):
        return 1

    # Check if src_dir exists
    if not path_exists(src_dir):
        print ("Directory '%s' does not exist" % src_dir)
        return 1

    # Create dir_path
    rc = create_dir(dest_dir)
    if rc != 0:
        return 1

    rc = 0
    for dir_name, subdir_list, file_list in os.walk(src_dir, topdown=False):
        for fname in file_list:
            new_fname, ext = os.path.splitext(fname)
            try:
                tmp_dir = dir_name.replace(src_dir, "")
                rc = create_dir(dest_dir + path_sep + tmp_dir)
                if rc != 0:
                    rc = 1
                link_file = (dest_dir + path_sep + tmp_dir + path_sep +
                             new_fname + "_h")
                target_file = os.path.join(dir_name, fname)
                if platform.system() == "Windows":
                    cmd = "mklink /H " + link_file + " " + target_file
                elif platform.system() == "Linux":
                    cmd = "ln " + target_file + " " + link_file
                subprocess.call(cmd, shell=True)
            except OSError:
                rc = 1

        if platform.system() == "Windows":
            if dir_name != src_dir:
                try:
                    tmp_dir = dir_name.replace(src_dir, "")
                    rc = create_dir(dest_dir + path_sep + tmp_dir)
                    if rc != 0:
                        rc = 1
                    link_file = dest_dir + path_sep + tmp_dir + "_h"
                    target_file = dir_name
                    cmd = "mklink /J " + link_file + " " + target_file
                    subprocess.call(cmd, shell=True)
                except OSError:
                    rc = 1

    return rc


def read(args):
    """Reads all files under 'dir' and logs the contents of the file
       in given log file.
    """
    dir_path = os.path.abspath(args.dir)
    log_file = args.log_file
    rc = 0
    for dir_name, subdir_list, file_list in os.walk(dir_path, topdown=False):
        for fname in file_list:
            new_fname, ext = os.path.splitext(fname)
            try:
                if platform.system() == "Windows":
                    cmd = "type " + os.path.join(dir_name, fname)
                elif platform.system() == "Linux":
                    cmd = "cat " + os.path.join(dir_name, fname)
                fh = open(log_file, "a")
                subprocess.call(cmd, shell=True, stdout=fh)
                fh.close()
            except OSError:
                rc = 1
    return rc


def copy(args):
    """
    Copies files/dirs under 'dir' to destination directory
    """
    src_dir = os.path.abspath(args.src_dir)
    dest_dir = args.dest_dir

    # Check if src_dir is '/'
    if is_root(src_dir):
        return 1

    # Check if src_dir exists
    if not path_exists(src_dir):
        print ("Directory '%s' does not exist" % src_dir)
        return 1

    # Create dest_dir
    rc = create_dir(dest_dir)
    if rc != 0:
        return 1

    rc = 0
    for dir_name, subdir_list, file_list in os.walk(src_dir, topdown=False):
        for fname in file_list:
            try:
                src = os.path.join(dir_name, fname)
                dst = dest_dir
                shutil.copy(src, dst)
            except OSError:
                rc = 1

        if dir_name != src_dir:
            try:
                src = dir_name
                dst = (dest_dir + path_sep +
                       os.path.basename(os.path.normpath(src)))
                shutil.copytree(src, dst)
            except OSError:
                rc = 1
    return rc


def delete(args):
    """
    Deletes files/dirs under 'dir'
    """
    dir_path = os.path.abspath(args.dir)

    # Check if dir_path is '/'
    if is_root(dir_path):
        return 1

    # Check if dir_path exists
    if not path_exists(dir_path):
        print ("Directory '%s' does not exist" % dir_path)
        return 1

    rc = 0
    for dir_name, subdir_list, file_list in os.walk(dir_path, topdown=False):
        for fname in file_list:
            try:
                os.remove(os.path.join(dir_name, fname))
            except OSError:
                rc = 1

        if dir_name != dir_path:
            try:
                os.rmdir(dir_name)
            except OSError:
                rc = 1
    return rc


if __name__ == "__main__":
    print ("Starting File/Dir Ops: %s" % _get_current_time())
    test_start_time = datetime.datetime.now().replace(microsecond=0)

    parser = argparse.ArgumentParser(
        prog='file_dir_ops.py',
        description=("Program for performing file/directory operations."))

    subparsers = parser.add_subparsers(title='Available sub commands',
                                       help='sub-command help')

    # Create Deep Directories
    create_deep_dir_parser = subparsers.add_parser(
        'create_deep_dir',
        help=("Create deep dirs under 'dir' with depth 'dir_depth'."
              "In each level creates sub-dirs max up to 'max_num_of_dirs'."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    create_deep_dir_parser.add_argument(
        '-d', '--dir-depth',
        help="Directory depth", metavar=('dir_depth'), dest='dir_depth',
        default=1, type=int)
    create_deep_dir_parser.add_argument(
        '-l', '--dir-length',
        help="Top level directory length", metavar=('dir_length'),
        dest='dir_length', default=1, type=int)
    create_deep_dir_parser.add_argument(
        '-n', '--num-of-dirs',
        help="Maximum number of directories in each level",
        metavar=('max_num_of_dirs'), dest='max_num_of_dirs', default=1,
        type=int)
    create_deep_dir_parser.add_argument(
        '--dirname-start-num',
        help="Start the directory naming from 'dirname-start-num'",
        metavar=('dirname_start_num'), dest='dirname_start_num', default=1,
        type=int)
    create_deep_dir_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    create_deep_dir_parser.set_defaults(func=create_deep_dirs)

    # Create Deep Directories with Files
    create_deep_dir_with_files_parser = subparsers.add_parser(
        'create_deep_dirs_with_files',
        help=("Create deep dirs under 'dir' with depth 'dir_depth'. "
              "In each level creates sub-dirs max up to 'max_num_of_dirs'. "
              "Creates specified 'num_of_files' in each dir created."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    create_deep_dir_with_files_parser.add_argument(
        '-d', '--dir-depth',
        help="Directory depth", metavar=('dir_depth'), dest='dir_depth',
        default=1, type=int)
    create_deep_dir_with_files_parser.add_argument(
        '-l', '--dir-length',
        help="Top level directory length", metavar=('dir_length'),
        dest='dir_length', default=1, type=int)
    create_deep_dir_with_files_parser.add_argument(
        '-n', '--max-num-of-dirs',
        help="Maximum number of directories in each level",
        metavar=('max_num_of_dirs'), dest='max_num_of_dirs', default=1,
        type=int)
    create_deep_dir_with_files_parser.add_argument(
        '-f', '--num-of-files',
        help="Number of files to be created in each level",
        metavar=('num_of_files'), dest='num_of_files', default=1,
        type=int)
    create_deep_dir_with_files_parser.add_argument(
        '--fixed-file-size', help=("Fixed file size. The sizes can be "
                                   "1k, 10k, 512k, 1M"),
        metavar=('file_size'), dest='fixed_file_size', type=str)
    create_deep_dir_with_files_parser.add_argument(
        '--base-file-name', help=("Base File Name"),
        metavar=('base_file_name'), dest='base_file_name', type=str,
        default="testfile")
    create_deep_dir_with_files_parser.add_argument(
        '--file-types', help=("File Types to be created. File types "
                              "can be txt, docx, empty_file"
                              " separated with space"),
        metavar=('file_types'), dest='file_types', type=str,
        default="txt")
    create_deep_dir_with_files_parser.add_argument(
        '--dirname-start-num',
        help="Start the directory naming from 'dirname-start-num'",
        metavar=('dirname_start_num'), dest='dirname_start_num', default=1,
        type=int)
    create_deep_dir_with_files_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    create_deep_dir_with_files_parser.set_defaults(
        func=create_deep_dirs_with_files)

    # Create files recursively under specified dir
    create_files_parser = subparsers.add_parser(
        'create_files',
        help=("Create specified num_of_files in each dir under 'dir'."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    create_files_parser.add_argument(
        '-f', help="Number of files to be created recursively under 'dir'",
        metavar=('num_of_files'), dest='num_of_files', default=1,
        type=int)
    create_files_parser.add_argument(
        '--fixed-file-size', help=("Fixed file size. The sizes can be "
                                   "1k, 10k, 512k, 1M"),
        metavar=('file_size'), dest='fixed_file_size', type=str)
    create_files_parser.add_argument(
        '--base-file-name', help=("Base File Name"),
        metavar=('base_file_name'), dest='base_file_name', type=str,
        default="testfile")
    create_files_parser.add_argument(
        '--file-types', help=("File Types to be created. File types "
                              "can be txt, docx, empty_file"
                              " separated with space"),
        metavar=('file_types'), dest='file_types', type=str,
        default="txt")
    create_files_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    create_files_parser.set_defaults(func=create_files)

    # Rename all files/directories recursively under dir
    rename_parser = subparsers.add_parser(
        'mv',
        help=("Recursively rename all the files/dirs under 'dir' to "
              "'filename'/'dirname' + '_postfix'."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    rename_parser.add_argument(
        '-s', '--postfix-string', help="Postfix String",
        metavar=('postfix_string'), dest='postfix', default='a',
        type=str)
    rename_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    rename_parser.set_defaults(func=rename)

    # List all files/directories recursively under dir
    ls_parser = subparsers.add_parser(
        'ls',
        help=("Recursively list all the files/dirs under 'dir'"),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ls_parser.add_argument(
        '-l', '--log-file',
        help="Redirect the output to specified log file name",
        dest='log_file_name', default=None)
    ls_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    ls_parser.set_defaults(func=ls)

    # Stat files/dirs
    stat_parser = subparsers.add_parser(
        'stat',
        help=("Get files/dirs Stat"),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    stat_parser.add_argument(
        '-R', '--recursive',
        help="Recursively get the stat of files/dirs under given dir",
        dest='recursive', action='store_true')
    stat_parser.add_argument(
        '-l', '--log-file',
        help="Redirect the output to specified log file name",
        dest='log_file_name', default=None)
    stat_parser.add_argument(
        'path', metavar='PATH', type=str,
        help="File/Directory for which stat has to be performed")
    stat_parser.set_defaults(func=get_path_stats)

    # Compress files/directories under dir
    compress_parser = subparsers.add_parser(
        'compress',
        help=("Recursively compress all the files/dirs under 'dir'. "),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    compress_parser.add_argument(
        '--compress-type', help="Compress type. It can be 7z,gzip",
        metavar=('compress_type'), dest='compress_type', default='7z',
        type=str)
    compress_parser.add_argument(
        '--dest-dir', help="Destination directory to place compress files",
        metavar=('dest_dir'), dest='dest_dir',
        type=str)
    compress_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    compress_parser.set_defaults(func=compress)

    # UnCompress the given compressed file
    uncompress_file_parser = subparsers.add_parser(
        'uncompress',
        help=("Uncompress the given compressed file. "),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    uncompress_file_parser.add_argument(
        'compressed_file', metavar='compressed_file', type=str,
        help="File to be uncompressed")
    uncompress_file_parser.add_argument(
        '--dest-dir', help="Destination directory to place uncompressed files",
        metavar=('dest_dir'), dest='dest_dir',
        type=str)
    uncompress_file_parser.set_defaults(func=uncompress)

    # UnCompress compressed files under dir
    uncompress_dir_parser = subparsers.add_parser(
        'uncompress_dir',
        help=("Uncompress compressed files under 'dir'. "),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    uncompress_dir_parser.add_argument(
        '--dest-dir', help="Destination directory to place uncompress files",
        metavar=('dest_dir'), dest='dest_dir',
        type=str)
    uncompress_dir_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    uncompress_dir_parser.set_defaults(func=uncompress_dir)

    # Creates hard link for each file and directory under dir
    hard_link_parser = subparsers.add_parser(
        'create_hard_link',
        help=("Creates hard link for files/directory under 'dir'. "),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    hard_link_parser.add_argument(
        '--dest-dir', help="Destination directory to create hard links",
        metavar=('dest_dir'), dest='dest_dir',
        type=str)
    hard_link_parser.add_argument(
        'src_dir', metavar='src_dir', type=str,
        help="Directory on which operations has to be performed")
    hard_link_parser.set_defaults(func=create_hard_links)

    # Reads files under dir
    if platform.system() == "Windows":
        default_log_file = "NUL"
    elif platform.system() == "Linux":
        default_log_file = "/dev/null"

    read_parser = subparsers.add_parser(
        'read',
        help=("Read all the files under 'dir'. "),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    read_parser.add_argument(
        '--log-file', help="Output log filename to log the "
                           "contents of file",
        metavar=('log_file'), dest='log_file',
        type=str, default=default_log_file)
    read_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    read_parser.set_defaults(func=read)

    # copy all files/directories under dir
    copy_parser = subparsers.add_parser(
        'copy',
        help=("Copy all files/directories under 'dir'. "),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    copy_parser.add_argument(
        '--dest-dir', help="Output directory to copy files/dirs",
        metavar=('dest_dir'), dest='dest_dir',
        type=str)
    copy_parser.add_argument(
        'src_dir', metavar='src_dir', type=str,
        help="Directory on which operations has to be performed")
    copy_parser.set_defaults(func=copy)

    # Deletes all files/directories under dir
    delete_parser = subparsers.add_parser(
        'delete',
        help=("Delete all the files/dirs under 'dir'"),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    delete_parser.add_argument(
        'dir', metavar='DIR', type=str,
        help="Directory on which operations has to be performed")
    delete_parser.set_defaults(func=delete)

    args = parser.parse_args()
    rc = args.func(args)

    test_end_time = datetime.datetime.now().replace(microsecond=0)
    print ("Execution time: %s" % (test_end_time - test_start_time))
    print ("Ending File/Dir Ops %s" % _get_current_time())
    sys.exit(rc)
