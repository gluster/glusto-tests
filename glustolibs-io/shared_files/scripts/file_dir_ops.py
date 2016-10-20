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
    Description: Helpers for performing file/dir operations
"""

import os
import argparse
import sys
import time
import random
import string
import datetime
from multiprocessing import Process
import subprocess
from docx import Document
import contextlib
import platform


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
        except (OSError, IOError) as e:
            print "Unable to create dir: %s" % dir_abs_path
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
                print "Unable to create dir '%s' : %s" % (dir_path, e.strerror)
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
    except AttributeError as e:
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
            file_size = file_sizes_dict[random.choice(file_sizes_dict.keys())]
        else:
            try:
                file_size = file_sizes_dict[fixed_file_size]
            except KeyError as e:
                print "File sizes can be [1k, 10k, 512k, 1M]"
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
                p = document.add_paragraph(file_str)
                document.save(fname_abs_path)
            except:
                print ("Unable to write to file '%s' : %s" %
                       (fname_abs_path, e.strerror))
                rc = 1
        elif type == 'empty_file':
            try:
                with open(fname_abs_path, "w+") as fd:
                    fd.close()
            except:
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
    except AttributeError as e:
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
        print "Directory '%s' does not exist" % dir_path
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
                print "Unable to rename %s -> %s" % (old, new)

        # rename dirs
        if dirName != dir_path:
            old = dirName
            new = dirName + "_" + postfix
            try:
                os.rename(old, new)
            except OSError:
                rc = 1
                print "Unable to rename %s -> %s" % (old, new)
    return rc


def ls(args):
    """Recursively list all the files/dirs under 'dir'
    """
    dir_path = os.path.abspath(args.dir)
    log_file_name = args.log_file_name

    # Check if dir_path exists
    if not path_exists(dir_path):
        print "Directory '%s' does not exist" % dir_path
        return 1

    with open_file_to_write(log_file_name) as file_handle:
        if log_file_name:
            time_str = _get_current_time()
            print >>file_handle, "Starting 'ls -R' : %s" % time_str
        for dirName, subdirList, fileList in os.walk(dir_path):
            print >>file_handle, ('Dir: %s' % dirName)
            for dname in subdirList:
                print >>file_handle, ('\t%s' % os.path.join(dirName, dname))
            for fname in fileList:
                print >>file_handle, ('\t%s' % os.path.join(dirName, fname))
        if log_file_name:
            time_str = _get_current_time()
            print >>file_handle, "Ending 'ls -R' : %s" % time_str
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
    except Exception as e:
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
        print "PATH '%s' does not exist" % path
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
            print >>file_handle, "Starting 'stat %s' : %s" % (
                path, time_str)
        for key in file_stats.keys():
            print >>file_handle, "\nFile: %s" % key
            ret, file_stat, err = file_stats[key]
            if ret != 0:
                rc = 1
                print >>file_handle, "\t%s" % err
            else:
                print >>file_handle, "\t%s" % file_stat
        if log_file_name:
            time_str = _get_current_time()
            print >>file_handle, "Ending 'stat %s' : %s" % (
                path, time_str)
        print >>file_handle, "\n"

    return rc


if __name__ == "__main__":
    print "Starting File/Dir Ops: %s" % _get_current_time()
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
              "In each level creates sub-dirs max upto 'max_num_of_dirs'."),
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
              "In each level creates sub-dirs max upto 'max_num_of_dirs'. "
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

    args = parser.parse_args()
    rc = args.func(args)

    test_end_time = datetime.datetime.now().replace(microsecond=0)
    print "Execution time: %s" % (test_end_time - test_start_time)
    print "Ending File/Dir Ops %s" % _get_current_time()
    sys.exit(rc)
