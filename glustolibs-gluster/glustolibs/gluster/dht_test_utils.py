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
"""Module for library DHT test utility functions"""

import os

from glusto.core import Glusto as g

from glustolibs.gluster.glusterfile import (GlusterFile, calculate_hash,
                                            get_pathinfo, file_exists)
from glustolibs.gluster.glusterdir import GlusterDir
from glustolibs.gluster.layout import Layout
import glustolibs.gluster.constants as k
import glustolibs.gluster.exceptions as gex
from glustolibs.gluster.brickdir import BrickDir
from glustolibs.gluster.volume_libs import get_subvols, get_volume_type
from glustolibs.gluster.gluster_init import get_gluster_version
from glustolibs.misc.misc_libs import upload_scripts


def run_layout_tests(mnode, fqpath, layout, test_type):
    """run the is_complete and/or is_balanced tests"""
    ret = get_pathinfo(mnode, fqpath)
    brick_path_list = ret.get('brickdir_paths')
    for brickdir_path in brick_path_list:
        (server_ip, _) = brickdir_path.split(':')
        if (get_gluster_version(server_ip) >= 6.0 and
                get_volume_type(brickdir_path) in ('Replicate', 'Disperse',
                                                   'Arbiter')):
            g.log.info("Cannot check for layout completeness as"
                       " volume under test is Replicate/Disperse/Arbiter")
        else:
            if test_type & k.TEST_LAYOUT_IS_COMPLETE:
                g.log.info("Testing layout complete for %s" % fqpath)
                if not layout.is_complete:
                    msg = ("Layout for %s IS NOT COMPLETE" % fqpath)
                    g.log.error(msg)
                    raise gex.LayoutIsNotCompleteError(msg)
            if test_type & k.TEST_LAYOUT_IS_BALANCED:
                g.log.info("Testing layout balance for %s" % fqpath)
                if not layout.is_balanced:
                    msg = ("Layout for %s IS NOT BALANCED" % fqpath)
                    g.log.error(msg)
                    raise gex.LayoutIsNotBalancedError(msg)

            # returning True until logic requires non-exception error check(s)
            return True


def run_hashed_bricks_test(gfile):
    """run check for file/dir existence on brick based on calculated hash"""
    g.log.info("Testing file/dir %s existence on hashed brick(s)." %
               gfile.fqpath)
    if not gfile.exists_on_hashed_bricks:
        msg = ("File/Dir %s DOES NOT EXIST on hashed bricks." %
               gfile.fqpath)
        g.log.error(msg)
        raise gex.FileDoesNotExistOnHashedBricksError(msg)

    return True


def validate_files_in_dir(mnode, rootdir,
                          file_type=k.FILETYPE_ALL,
                          test_type=k.TEST_ALL):
    """walk a directory tree and check if layout is_complete.

    Args:
        mnode (str): The host of the directory being traversed.
        rootdir (str): The fully qualified path of the dir being traversed.
        file_type (int): An or'd set of constants defining the file types
                        to test.
                            FILETYPE_DIR
                            FILETYPE_DIRS
                            FILETYPE_FILE
                            FILETYPE_FILES
                            FILETYPE_ALL

        test_type (int): An or'd set of constants defining the test types
                        to run.
                            TEST_LAYOUT_IS_COMPLETE
                            TEST_LAYOUT_IS_BALANCED
                            TEST_FILE_EXISTS_ON_HASHED_BRICKS
                            TEST_ALL

    Examples:
        # TEST LAYOUTS FOR FILES IN A DIRECTORY

        validate_files_in_dir(clients[0], '/mnt/glusterfs')
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              file_type=k.FILETYPE_DIRS)
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              file_type=k.FILETYPE_FILES)
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              test_type=k.TEST_LAYOUT_IS_COMPLETE,
                              file_type=(k.FILETYPE_DIRS | k.FILETYPE_FILES))
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              test_type=k.TEST_LAYOUT_IS_BALANCED)
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              test_type=k.TEST_LAYOUT_IS_BALANCED,
                              file_type=k.FILETYPE_FILES)

        # TEST FILES IN DIRECTORY EXIST ON HASHED BRICKS
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              test_type=k.TEST_FILE_EXISTS_ON_HASHED_BRICKS)
    """
    layout_cache = {}

    script_path = ("/usr/share/glustolibs/scripts/walk_dir.py")
    if not file_exists(mnode, script_path):
        if upload_scripts(mnode, script_path,
                          "/usr/share/glustolibs/scripts/"):
            g.log.info("Successfully uploaded script "
                       "walk_dir.py!")
        else:
            g.log.error("Faild to upload walk_dir.py!")
            return False
    else:
        g.log.info("compute_hash.py already present!")

    cmd = ("/usr/bin/env python {0} {1}".format(script_path, rootdir))
    ret, out, _ = g.run(mnode, cmd)
    if ret:
        g.log.error('Unable to run the script on node {0}'
                    .format(mnode))
        return False
    for walkies in eval(out):
        g.log.info("TESTING DIRECTORY %s..." % walkies[0])

        # check directories
        if file_type & k.FILETYPE_DIR:
            for testdir in walkies[1]:
                fqpath = os.path.join(walkies[0], testdir)
                gdir = GlusterDir(mnode, fqpath)

                if gdir.parent_dir in layout_cache:
                    layout = layout_cache[gdir.parent_dir]
                else:
                    layout = Layout(gdir.parent_dir_pathinfo)
                    layout_cache[gdir.parent_dir] = layout

                    run_layout_tests(mnode, gdir.parent_dir, layout, test_type)

                if test_type & k.TEST_FILE_EXISTS_ON_HASHED_BRICKS:
                    run_hashed_bricks_test(gdir)

        # check files
        if file_type & k.FILETYPE_FILE:
            for file in walkies[2]:
                fqpath = os.path.join(walkies[0], file)
                gfile = GlusterFile(mnode, fqpath)

                if gfile.parent_dir in layout_cache:
                    layout = layout_cache[gfile.parent_dir]
                else:
                    layout = Layout(gfile.parent_dir_pathinfo)
                    layout_cache[gfile.parent_dir] = layout

                    run_layout_tests(mnode, gfile.parent_dir, layout,
                                     test_type)

                if test_type & k.TEST_FILE_EXISTS_ON_HASHED_BRICKS:
                    run_hashed_bricks_test(gfile)
    return True


def create_brickobjectlist(subvols, path):
    '''
        Args:
            subvols : list of subvols (output of get_subvols)

        Return Value:
            List of Brickdir Object representing path from each brick.
            Note: Only one brick is accounted from one subvol.
    '''
    secondary_bricks = []
    for subvol in subvols:
        secondary_bricks.append(subvol[0])

    for subvol in secondary_bricks:
        g.log.debug("secondary bricks %s", subvol)

    brickobject = []
    for item in secondary_bricks:
        temp = BrickDir(item + "/" + path)
        brickobject.append(temp)

    return brickobject


def find_hashed_subvol(subvols, parent_path, name):
    '''
        Args:
            subvols:  subvol list
            parent_path: Immediate parent path of "name" relative from
                         mount point
                         e.g. if your mount is "/mnt" and the path from mount
                         is "/mnt/directory" then just pass "directory" as
                         parent_path

            name: file or directory name

        Return Values:
            hashed_subvol object: An object of type BrickDir type representing
                                  the hashed subvolume

            subvol_count: The subvol index in the subvol list
    '''
    # pylint: disable=protected-access
    if subvols is None or parent_path is None or name is None:
        g.log.error("empty arguments")
        return None, -1

    brickobject = create_brickobjectlist(subvols, parent_path)
    hash_num = calculate_hash(brickobject[0]._host, name)

    count = -1
    for brickdir in brickobject:
        count += 1
        ret = brickdir.hashrange_contains_hash(hash_num)
        if ret == 1:
            g.log.debug('hash subvolume is %s', brickdir._host)
            hashed_subvol = brickdir
            break

    return hashed_subvol, count


def find_nonhashed_subvol(subvols, parent_path, name):
    '''
        Args:
            subvols: subvol list
            parent_path: Immediate parent path of "name" relative from
                         mount point
                         e.g. if your mount is "/mnt" and the path from mount
                         is "/mnt/directory" then just pass "directory" as
                         parent_path

            name: file or directory name

        Return Values:
            nonhashed_subvol object: An object of type BrickDir type
                                     representing the nonhashed subvolume

            subvol_count: The subvol index in the subvol list
    '''
    # pylint: disable=protected-access
    if subvols is None or parent_path is None or name is None:
        g.log.error("empty arguments")
        return None, -1

    brickobject = create_brickobjectlist(subvols, parent_path)
    hash_num = calculate_hash(brickobject[0]._host, name)

    count = -1
    for brickdir in brickobject:
        count += 1
        ret = brickdir.hashrange_contains_hash(hash_num)
        if ret == 1:
            g.log.debug('hash subvolume is %s', brickdir.path)
            continue

        nonhashed_subvol = brickdir
        g.log.info('nonhashed subvol %s', brickdir._host)
        break

    return nonhashed_subvol, count


def find_new_hashed(subvols, parent_path, oldname):
    '''
        This is written for rename case so that the new name will hash to a
        different subvol than that of the the old name.
        Note: The new hash will be searched under the same parent

        Args:
            subvols = list of subvols
            parent_path = parent path (relative to mount) of "oldname"
            oldname = name of the source file for rename operation

        Return Values:
            For success returns an object of type NewHashed holding
            information pertaining to new name.

            For Failure returns None
    '''
    # pylint: disable=protected-access
    # pylint: disable=pointless-string-statement
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-return-statements
    brickobject = create_brickobjectlist(subvols, parent_path)
    if brickobject is None:
        g.log.error("could not form brickobject list")
        return None

    for bro in brickobject:
        bro._get_hashrange()
        low = bro._hashrange_low
        high = bro._hashrange_high
        g.log.debug("low hashrange %s high hashrange %s", str(low), str(high))
        g.log.debug("absoulte path %s", bro._fqpath)

    hash_num = calculate_hash(brickobject[0]._host, oldname)
    oldhashed, _ = find_hashed_subvol(subvols, parent_path, oldname)
    if oldhashed is None:
        g.log.error("could not find old hashed subvol")
        return None

    g.log.debug("oldhashed: %s oldname: %s oldhash %s", oldhashed._host,
                oldname, hash_num)

    count = -1
    for item in range(1, 5000, 1):
        newhash = calculate_hash(brickobject[0]._host, str(item))
        for brickdir in brickobject:
            count += 1
            ret = brickdir.hashrange_contains_hash(newhash)
            if ret == 1:
                if oldhashed._fqpath != brickdir._fqpath:
                    g.log.debug("oldhashed %s new %s count %s",
                                oldhashed, brickdir._host, str(count))
                    return NewHashed(item, brickdir, count)

        count = -1
    return None


def find_specific_hashed(subvols, parent_path, subvol):
    """ Finds filename that hashes to a specific subvol.

    Args:
           subvols(list): list of subvols
           parent_path(str): parent path (relative to mount) of "oldname"
           subvol(str): The subvol to which the new name has to be hashed

    Returns:
             (Class Object): For success returns an object of type NewHashed
                            holding information pertaining to new name.
                            None, otherwise
     Note: The new hash will be searched under the same parent
    """
    # pylint: disable=protected-access
    brickobject = create_brickobjectlist(subvols, parent_path)
    if brickobject is None:
        g.log.error("could not form brickobject list")
        return None
    count = -1
    for item in range(1, 5000, 1):
        newhash = calculate_hash(brickobject[0]._host, str(item))
        for brickdir in brickobject:
            count += 1
            if subvol._fqpath == brickdir._fqpath:
                ret = brickdir.hashrange_contains_hash(newhash)
                if ret:
                    g.log.debug("oldhashed %s new %s count %s",
                                subvol, brickdir._host, str(count))
                    return NewHashed(item, brickdir, count)
        count = -1
    return None


class NewHashed(object):
    '''
        Helper Class to hold new hashed info
    '''
    # pylint: disable=too-few-public-methods
    def __init__(self, newname, hashedbrickobject, count):
        self.newname = newname
        self.hashedbrickobject = hashedbrickobject
        self.subvol_count = count


def is_layout_complete(mnode, volname, dirpath):
    """This function reads the subvols in the given volume and checks whether
       layout is complete or not.
       Layout starts at zero,
       ends at 32-bits high,
        and has no holes or overlaps

    Args:
        volname (str): volume name
        mnode (str): Node on which cmd has to be executed.
        dirpath (str): directory path; starting from root of mount point.

    Returns (bool): True if layout is complete
                    False if layout has any holes or overlaps

    Example:
        is_layout_complete("abc.xyz.com", "testvol", "/")
        is_layout_complete("abc.xyz.com", "testvol", "/dir1/dir2/dir3")
    """

    subvols_list = get_subvols(mnode, volname)['volume_subvols']
    trim_subvols_list = [y for x in subvols_list for y in x]

    # append the dirpath to the elements in the list
    final_subvols_list = [x + dirpath for x in trim_subvols_list]

    complete_hash_list = []
    for fqpath in final_subvols_list:
        hash_list = BrickDir(fqpath).hashrange
        complete_hash_list.append(hash_list)
    joined_hashranges = [y for x in complete_hash_list for y in x]
    g.log.debug("joined range list: %s" % joined_hashranges)

    # remove duplicate hashes
    collapsed_ranges = list(set(joined_hashranges))

    # sort the range list for good measure
    collapsed_ranges.sort()

    # first hash in the list is 0?
    if collapsed_ranges[0] != 0:
        g.log.error('First hash in range (%d) is not zero' %
                    collapsed_ranges[0])
        return False

    # last hash in the list is 32-bits high?
    if collapsed_ranges[-1] != int(0xffffffff):
        g.log.error('Last hash in ranges (%s) is not 0xffffffff' %
                    hex(collapsed_ranges[-1]))
        return False

    # remove the first and last hashes
    clipped_ranges = collapsed_ranges[1:-1]
    g.log.debug('clipped: %s' % clipped_ranges)

    # walk through the list in pairs and look for diff == 1
    iter_ranges = iter(clipped_ranges)
    for first in iter_ranges:
        second = next(iter_ranges)
        hash_difference = second - first
        g.log.debug('%d - %d = %d' % (second, first, hash_difference))
        if hash_difference > 1:
            g.log.error("Layout has holes")

            return False
        elif hash_difference < 1:
            g.log.error("Layout has overlaps")
