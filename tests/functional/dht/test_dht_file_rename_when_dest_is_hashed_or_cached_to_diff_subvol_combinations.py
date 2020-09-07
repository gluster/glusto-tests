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

import re
from glusto.core import Glusto as g
from glustolibs.gluster.glusterfile import get_file_stat
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.dht_test_utils import (find_hashed_subvol,
                                               create_brickobjectlist,
                                               find_new_hashed,
                                               find_specific_hashed)
from glustolibs.gluster.volume_libs import get_subvols, parse_vol_file
from glustolibs.gluster.glusterfile import (move_file,
                                            is_linkto_file,
                                            get_dht_linkto_xattr)


@runs_on([['distributed-arbiter', 'distributed',
           'distributed-replicated',
           'distributed-dispersed'],
          ['glusterfs']])
class DhtFileRenameWithDestFile(GlusterBaseClass):

    def setUp(self):
        """
        Setup Volume and Mount Volume
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Change the dist count to 4 in case of 'distributed-replicated' ,
        # 'distributed-dispersed' and 'distributed-arbiter'
        if self.volume_type in ("distributed-replicated",
                                "distributed-dispersed",
                                "distributed-arbiter"):
            self.volume['voltype']['dist_count'] = 4

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

        self.mount_point = self.mounts[0].mountpoint

        self.subvols = (get_subvols(
            self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(self.subvols, "failed to get subvols")

    def tearDown(self):
        """
        Unmount Volume and Cleanup Volume
        """
        # Unmount Volume and Cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Unmount Volume and Cleanup Volume: Fail")
        g.log.info("Unmount Volume and Cleanup Volume: Success")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _create_file_and_get_hashed_subvol(self, file_name):
        """ Creates a file and return its hashed subvol

        Args:
               file_name(str): name of the file to be created
        Returns:
                hashed_subvol object: An object of type BrickDir type
                                    representing the hashed subvolume

                subvol_count: The subvol index in the subvol list

                source_file: Path to the file created

        """
        # pylint: disable=unsubscriptable-object

        # Create Source File
        source_file = "{}/{}".format(self.mount_point, file_name)
        ret, _, err = g.run(self.mounts[0].client_system,
                            ("touch %s" % source_file))
        self.assertEqual(ret, 0, ("Failed to create {} : err {}"
                                  .format(source_file, err)))
        g.log.info("Successfully created the source file")

        # Find the hashed subvol for source file
        source_hashed_subvol, count = find_hashed_subvol(self.subvols,
                                                         "/",
                                                         file_name)
        self.assertIsNotNone(source_hashed_subvol,
                             "Couldn't find hashed subvol for the source file")
        return source_hashed_subvol, count, source_file

    @staticmethod
    def _verify_link_file_exists(brickdir, file_name):
        """ Verifies whether a file link is present in given subvol
        Args:
               brickdir(Class Object): BrickDir object containing data about
                                       bricks under a specific subvol
        Returns:
                (bool): True if link file exists else false
        """
        # pylint: disable=protected-access
        # pylint: disable=unsubscriptable-object
        file_path = brickdir._fqpath + file_name
        file_stat = get_file_stat(brickdir._host, file_path)
        if file_stat is None:
            g.log.error("Failed to get File stat for %s", file_path)
            return False
        if not file_stat['access'] == "1000":
            g.log.error("Access value not 1000 for %s", file_path)
            return False

        # Check for file type to be'sticky empty', have size of 0 and
        # have the glusterfs.dht.linkto xattr set.
        ret = is_linkto_file(brickdir._host, file_path)
        if not ret:
            g.log.error("%s is not a linkto file", file_path)
            return False
        return True

    @staticmethod
    def _verify_file_exists(brick_dir, file_name):
        """ Verifies whether a file is present in given subvol or not
        Args:
               brick_dir(Class Object): BrickDir object containing data about
                                       bricks under a specific subvol
               file_name(str): Name of the file to be searched
        Returns:
                (bool): True if link file exists else false
        """
        # pylint: disable=protected-access

        cmd = "[ -f {} ]".format(brick_dir._fqpath + (str(file_name)))
        ret, _, _ = g.run(brick_dir._host, cmd)
        if ret:
            return False
        return True

    @staticmethod
    def _get_remote_subvolume(vol_file_data, brick_name):
        """ Verifies whether a file is present in given subvol or not
        Args:
               vol_file_data(dict): Dictionary containing data of .vol file
               brick_name(str): Brick path
        Returns:
                (str): Remote subvol name
                (None): If error occurred
        """
        try:
            brick_name = re.search(r'[a-z0-9\-\_]*', brick_name).group()
            remote_subvol = (vol_file_data[
                brick_name]['option']['remote-subvolume'])
        except KeyError:
            return None
        return remote_subvol

    def _verify_file_links_to_specified_destination(self, host, file_path,
                                                    dest_file):
        """ Verifies whether a file link points to the specified destination
        Args:
               host(str): Host at which commands are to be executed
               file_path(str): path to the link file
               dest_file(str): path to the dest file to be pointed at
        Returns:
                (bool) : Based on whether the given file points to dest or not
        """
        link_to_xattr = get_dht_linkto_xattr(host, file_path)
        # Remove unexpected chars in the value, if any
        link_to_xattr = re.search(r'[a-z0-9\-\_]*', link_to_xattr).group()
        if link_to_xattr is None:
            g.log.error("Failed to get trusted.glusterfs.dht.linkto")
            return False

        # Get the remote-subvolume for the corresponding linkto xattr
        path = ("/var/lib/glusterd/vols/{}/{}.tcp-fuse.vol"
                .format(self.volname, self.volname))
        vol_data = parse_vol_file(self.mnode, path)
        if not vol_data:
            g.log.error("Failed to parse the file %s", path)
            return False

        remote_subvol = self._get_remote_subvolume(vol_data, link_to_xattr)
        if remote_subvol is None:
            # In case, failed to find the remote subvol, get all the
            # subvolumes and then check whether the file is present in
            # any of those sunbol
            subvolumes = vol_data[link_to_xattr]['subvolumes']
            for subvol in subvolumes:
                remote_subvol = self._get_remote_subvolume(vol_data,
                                                           subvol)
                if remote_subvol:
                    subvol = re.search(r'[a-z0-9\-\_]*', subvol).group()
                    remote_host = (
                        vol_data[subvol]['option']['remote-host'])
                    # Verify the new file is in the remote-subvol identified
                    cmd = "[ -f {}/{} ]".format(remote_subvol, dest_file)
                    ret, _, _ = g.run(remote_host, cmd)
                    if not ret:
                        return True
            g.log.error("The given link file doesn't point to any of "
                        "the subvolumes")
            return False
        else:
            remote_host = vol_data[link_to_xattr]['option']['remote-host']
            # Verify the new file is in the remote-subvol identified
            cmd = "[ -f {}/{} ]".format(remote_subvol, dest_file)
            ret, _, _ = g.run(remote_host, cmd)
            if not ret:
                return True
        return False

    def test_file_rename_when_dest_doesnt_hash_src_cached_or_hashed(self):
        """
        - Destination file should exist
        - Source file is hashed on sub volume(s1) and cached on
          another subvolume(s2)
        - Destination file should be hased to subvolume(s3) other
          than above two subvolumes
        - Destination file hased on subvolume(s3) but destination file
          should be cached on same subvolume(s2) where source file is stored
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination file hashed on subvolume and should link
          to new destination file
        - source link file should be removed
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals

        # Create source file and Get hashed subvol (s2)
        src_subvol, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find a new file name for destination file, which hashes
        # to another subvol (s1)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed,
                             "couldn't find new hashed for destination file")

        # Rename the source file to the new file name
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, dest_file)))

        # Verify the Source link file is stored on hashed sub volume(s1)
        src_link_subvol = new_hashed.hashedbrickobject
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))

        # Identify a file name for dest to get stored in S2
        dest_cached_subvol = find_specific_hashed(self.subvols,
                                                  "/",
                                                  src_subvol)
        # Create the file with identified name
        _, _, dst_file = (
            self._create_file_and_get_hashed_subvol(
                str(dest_cached_subvol.newname)))
        # Verify its in S2 itself
        self.assertEqual(dest_cached_subvol.subvol_count, src_count,
                         ("The subvol found for destination is not same as "
                          "that of the source file cached subvol"))

        # Find a subvol (s3) for dest file to linkto, other than S1 and S2
        brickobject = create_brickobjectlist(self.subvols, "/")
        self.assertIsNotNone(brickobject, "Failed to get brick object list")
        br_count = -1
        subvol_new = None
        for brickdir in brickobject:
            br_count += 1
            if br_count not in (src_count, new_hashed.subvol_count):
                subvol_new = brickdir
                break

        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           subvol_new)
        self.assertIsNotNone(new_hashed2,
                             "could not find new hashed for dstfile")

        # Verify the subvol is not same as S1(src_count) and S2(dest_count)
        self.assertNotEqual(new_hashed2.subvol_count, src_count,
                            ("The subvol found for destination is same as that"
                             " of the source file cached subvol"))
        self.assertNotEqual(new_hashed2.subvol_count, new_hashed.subvol_count,
                            ("The subvol found for destination is same as that"
                             " of the source file hashed subvol"))

        # Rename the dest file to the new file name
        dst_file_ln = "{}/{}".format(self.mount_point,
                                     str(new_hashed2.newname))
        ret = move_file(self.mounts[0].client_system, dst_file, dst_file_ln)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(dst_file, dst_file_ln)))

        # Verify the Dest link file is stored on hashed sub volume(s3)
        dest_link_subvol = new_hashed2.hashedbrickobject
        ret = self._verify_link_file_exists(dest_link_subvol,
                                            str(new_hashed2.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(dest_link_subvol._fqpath,
                                      str(new_hashed2.newname))))

        # Move/Rename Source File to Dest
        src_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, src_file, dst_file)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(src_file, dst_file)))

        # Verify Source file is removed
        ret = self._verify_file_exists(src_subvol, "test_source_file")
        self.assertFalse(ret, "The source file is still present in {}"
                         .format(src_subvol._fqpath))

        # Verify Source link is removed
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, "The source link file is still present in {}"
                         .format(src_link_subvol._fqpath))

        # Verify the Destination link is on hashed subvolume
        ret = self._verify_link_file_exists(dest_link_subvol,
                                            str(new_hashed2.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(dest_link_subvol._fqpath,
                                      str(new_hashed2.newname))))

        # Verify the dest link file points to new destination file
        file_path = dest_link_subvol._fqpath + str(new_hashed2.newname)
        ret = (self._verify_file_links_to_specified_destination(
            dest_link_subvol._host, file_path,
            str(dest_cached_subvol.newname)))
        self.assertTrue(ret, "The dest link file not pointing towards "
                             "the desired file")
        g.log.info("The Destination link file is pointing to new file"
                   " as expected")

    def test_file_rename_when_dest_hash_src_cached(self):
        """
        - Destination file should exist
        - Source file hashed sub volume(s1) and cached on another subvolume(s2)
        - Destination file should be hased to subvolume where source file is
          stored(s2)
        - Destination file hased on subvolume(s2) but should be cached on
          some other subvolume(s3) than this two subvolume
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination link file should be removed
        - source link file should be removed
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals

        # Create source file and Get hashed subvol (s2)
        src_subvol, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find a new file name for destination file, which hashes
        # to another subvol (s2)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed,
                             "couldn't find new hashed for destination file")

        # Rename the source file to the new file name
        src_hashed = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, src_hashed)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, src_hashed)))

        # Verify the Source link file is stored on hashed sub volume(s1)
        src_link_subvol = new_hashed.hashedbrickobject
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))

        # Find a subvol (s3) for dest file to linkto, other than S1 and S2
        brickobject = create_brickobjectlist(self.subvols, "/")
        self.assertIsNotNone(brickobject, "Failed to get brick object list")
        br_count = -1
        subvol_new = None
        for brickdir in brickobject:
            br_count += 1
            if br_count not in (src_count, new_hashed.subvol_count):
                subvol_new = brickdir
                break

        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           subvol_new)
        self.assertIsNotNone(new_hashed2,
                             "could not find new hashed for dstfile")

        # Create a file in the subvol S3
        dest_subvol, count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed2.newname)))

        # Verify the subvol is not same as S1 and S2
        self.assertNotEqual(count, src_count,
                            ("The subvol found for destination is same as that"
                             " of the source file cached subvol"))
        self.assertNotEqual(count, new_hashed.subvol_count,
                            ("The subvol found for destination is same as that"
                             " of the source file hashed subvol"))

        # Find a file name that hashes to S2
        dest_hashed = find_specific_hashed(self.subvols,
                                           "/",
                                           src_subvol)
        self.assertIsNotNone(dest_hashed,
                             "could not find new hashed for dstfile")

        # Rename destination to hash to S2 and verify
        dest = "{}/{}".format(self.mount_point, str(dest_hashed.newname))
        ret = move_file(self.mounts[0].client_system, dest_file, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(dest_file, dest)))

        # Rename Source File to Dest
        ret = move_file(self.mounts[0].client_system, src_hashed, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(src_hashed, dest)))

        # Verify Destination File is removed
        ret = self._verify_file_exists(new_hashed2.hashedbrickobject,
                                       str(new_hashed2.newname))
        self.assertFalse(ret, "The Destination file is still present in {}"
                         .format(dest_subvol._fqpath))

        # Verify Source link is removed
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, "The source link file is still present in {}"
                         .format(src_link_subvol._fqpath))

        # Verify Destination Link is removed
        ret = self._verify_link_file_exists(dest_hashed.hashedbrickobject,
                                            str(dest_hashed.newname))
        self.assertFalse(ret, "The Dest link file is still present in {}"
                         .format(dest_hashed.hashedbrickobject._fqpath))

    def test_file_rename_when_src_linked_and_dest_hash_other(self):
        """
        - Destination file should exist
        - Source link file hashed on sub volume(s1) and cached on another
          subvolume(s2)
        - Destination file should be hased to some other
          subvolume(s3)(neither s1 nor s2)
        - Destination file hased on subvolume(s3) but cached on
          subvolume(s1) where source file is hashed
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination link file should be there on hashed subvolume
          and should link to new destination file
        - source link file should be removed
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals

        # Create source file and Get hashed subvol (s2)
        _, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find a new file name for destination file, which hashes
        # to another subvol (s2)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed,
                             "couldn't find new hashed for destination file")

        # Rename the source file to the new file name
        src_hashed = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, src_hashed)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, src_hashed)))

        # Verify the Source link file is stored on hashed sub volume(s1)
        src_link_subvol = new_hashed.hashedbrickobject
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))

        # Find a file name that hashes to S1
        dest_hashed = find_specific_hashed(self.subvols,
                                           "/",
                                           new_hashed.hashedbrickobject,
                                           new_hashed.newname)
        self.assertIsNotNone(dest_hashed,
                             "could not find new hashed for dstfile")

        # Create a file in the subvol S1
        dest_subvol, count, _ = self._create_file_and_get_hashed_subvol(
            str(dest_hashed.newname))

        # Verify the subvol is S1
        self.assertEqual(count, new_hashed.subvol_count,
                         ("The subvol found for destination is not same as"
                          " that of the source file hashed subvol"))

        # Find a subvol (s3) for dest file to linkto, other than S1 and S2
        brickobject = create_brickobjectlist(self.subvols, "/")
        self.assertIsNotNone(brickobject, "Failed to get brick object list")
        br_count = -1
        subvol_new = None
        for brickdir in brickobject:
            br_count += 1
            if br_count not in (src_count, new_hashed.subvol_count):
                subvol_new = brickdir
                break

        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           subvol_new)
        self.assertIsNotNone(new_hashed2,
                             "could not find new hashed for dstfile")

        # Rename destination to hash to S3 and verify
        dest_src = "{}/{}".format(self.mount_point, str(dest_hashed.newname))
        dest = "{}/{}".format(self.mount_point, str(new_hashed2.newname))
        ret = move_file(self.mounts[0].client_system, dest_src, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(dest_src, dest)))

        # Rename Source File to Dest
        ret = move_file(self.mounts[0].client_system, src_hashed, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(src_hashed, dest)))

        # Verify Destination File is removed
        ret = self._verify_file_exists(dest_hashed.hashedbrickobject,
                                       str(dest_hashed.newname))
        self.assertFalse(ret, "The Destination file is still present in {}"
                         .format(dest_subvol._fqpath))

        # Verify Source link is removed
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, "The source link file is still present in {}"
                         .format(src_link_subvol._fqpath))

        # Verify Destination Link is present and points to new dest file
        ret = self._verify_link_file_exists(new_hashed2.hashedbrickobject,
                                            str(new_hashed2.newname))
        self.assertTrue(ret, "The Dest link file is not present in {}"
                        .format(new_hashed2.hashedbrickobject._fqpath))

        file_path = new_hashed2.hashedbrickobject._fqpath + str(
            new_hashed2.newname)
        ret = (self._verify_file_links_to_specified_destination(
            new_hashed2.hashedbrickobject._host, file_path,
            str(new_hashed2.newname)))
        self.assertTrue(ret, "The dest link file not pointing towards "
                             "the desired file")
        g.log.info("The Destination link file is pointing to new file"
                   " as expected")

    def test_file_rename_when_dest_hash_src_cached_but_hash_other(self):
        """
        - Destination file should exist
        - Source file hashed on sub volume(s1) and cached
          on another subvolume(s2)
        - Destination file should be hased to same subvolume(s1)
          where source file is hashed
        - Destination hased on subvolume(s1) but cached on some other
          subvolume(s3)(neither s1 nor s2)
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination link file should be there on hashed subvolume
          and should link to new destination file
        - source link file should be removed
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals

        # Create source file and Get hashed subvol (s2)
        _, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find a new file name for destination file, which hashes
        # to another subvol (s2)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed,
                             "couldn't find new hashed for destination file")

        # Rename the source file to the new file name
        src_hashed = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, src_hashed)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, src_hashed)))

        # Verify the Source link file is stored on hashed sub volume(s1)
        src_link_subvol = new_hashed.hashedbrickobject
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))

        # Destination file cached on S3.
        # Find a subvol (s3) for dest file to linkto, other than S1 and S2
        brickobject = create_brickobjectlist(self.subvols, "/")
        self.assertIsNotNone(brickobject, "Failed to get brick object list")
        br_count = -1
        subvol_new = None
        for brickdir in brickobject:
            br_count += 1
            if br_count not in (src_count, new_hashed.subvol_count):
                subvol_new = brickdir
                break

        dest_cached = find_specific_hashed(self.subvols,
                                           "/",
                                           subvol_new)
        self.assertIsNotNone(dest_cached,
                             "could not find new hashed for dstfile")

        # Create a file in S3
        _, count, dest_src = self._create_file_and_get_hashed_subvol(
            str(dest_cached.newname))

        # Verify the subvol is not S2 and S1
        self.assertNotEqual(count, new_hashed.subvol_count,
                            ("The subvol found for destination is same as "
                             "that of the source file hashed subvol"))
        self.assertNotEqual(count, src_count,
                            ("The subvol found for destination is same as "
                             "that of the source file cached subvol"))

        # Rename Destination file such that it hashes to S1
        dest_hashed = find_specific_hashed(self.subvols,
                                           "/",
                                           new_hashed.hashedbrickobject,
                                           new_hashed.newname)
        # Verify its S1
        self.assertEqual(dest_hashed.subvol_count, new_hashed.subvol_count,
                         ("The subvol found for destination is not same as "
                          "that of the source file hashed subvol"))

        # Move dest to new name
        dest = "{}/{}".format(self.mount_point, str(dest_hashed.newname))
        ret = move_file(self.mounts[0].client_system, dest_src, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(dest_src, dest)))

        # Move Source file to Dest
        src = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, src, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(src, dest)))

        # Verify Destination File is removed
        ret = self._verify_file_exists(dest_cached.hashedbrickobject,
                                       str(dest_cached.newname))
        self.assertFalse(ret, "The Dest file is still present in {}"
                         .format(dest_cached.hashedbrickobject._fqpath))

        # Verify Source link is removed
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, "The source link file is still present in {}"
                         .format(src_link_subvol._fqpath))

        # Verify Destination Link is present and points to new dest file
        ret = self._verify_link_file_exists(dest_hashed.hashedbrickobject,
                                            str(dest_hashed.newname))
        self.assertTrue(ret, "The Dest link file is not present in {}"
                        .format(dest_hashed.hashedbrickobject._fqpath))

        file_path = dest_hashed.hashedbrickobject._fqpath + str(
            dest_hashed.newname)
        ret = (self._verify_file_links_to_specified_destination(
            dest_hashed.hashedbrickobject._host, file_path,
            str(dest_hashed.newname)))
        self.assertTrue(ret, "The dest link file not pointing towards "
                             "the desired file")
        g.log.info("The Destination link file is pointing to new file"
                   " as expected")

    def test_file_rename_when_dest_neither_hash_cache_to_src_subvols(self):
        """
        - Destination file should exist
        - Source file hashed on sub volume(s1) and cached on
          another subvolume(s2)
        - Destination file should be hased to some other subvolume(s3)
          (neither s1 nor s2)
        - Destination file hased on subvolume(s3) but cached on
          remaining subvolume(s4)
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination link file should be there on hashed subvolume
          and should link to new destination file
        - source link file should be removed
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals

        # Create source file and Get hashed subvol (s2)
        _, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find a new file name for destination file, which hashes
        # to another subvol (s2)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed,
                             "couldn't find new hashed for destination file")

        # Rename the source file to the new file name
        src_hashed = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, src_hashed)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, src_hashed)))

        # Verify the Source link file is stored on hashed sub volume(s1)
        src_link_subvol = new_hashed.hashedbrickobject
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))

        # Destination file cached on S4.
        # Find a subvol (s4) for dest file to linkto, other than S1 and S2
        brickobject = create_brickobjectlist(self.subvols, "/")
        self.assertIsNotNone(brickobject, "Failed to get brick object list")
        br_count = -1
        subvol_new = None
        for brickdir in brickobject:
            br_count += 1
            if br_count not in (src_count, new_hashed.subvol_count):
                subvol_new = brickdir
                break

        dest_cached = find_specific_hashed(self.subvols,
                                           "/",
                                           subvol_new)
        self.assertIsNotNone(dest_cached,
                             "could not find new hashed for dstfile")
        # Create a file in S3
        _, _, dest_src = self._create_file_and_get_hashed_subvol(
            str(dest_cached.newname))

        # Verify the subvol is not S2 and S1
        self.assertNotEqual(dest_cached.subvol_count, new_hashed.subvol_count,
                            ("The subvol found for destination is same as "
                             "that of the source file hashed subvol"))
        self.assertNotEqual(dest_cached.subvol_count, src_count,
                            ("The subvol found for destination is same as "
                             "that of the source file cached subvol"))

        # Identify a name for dest that hashes to another subvol S3
        # Find a subvol (s3) for dest file to linkto, other than S1 and S2 and
        # S4
        brickobject = create_brickobjectlist(self.subvols, "/")
        self.assertIsNotNone(brickobject, "Failed to get brick object list")
        br_count = -1
        subvol_new = None
        for brickdir in brickobject:
            br_count += 1
            if br_count not in (src_count, new_hashed.subvol_count,
                                dest_cached.subvol_count):
                subvol_new = brickdir
                break

        dest_hashed = find_specific_hashed(self.subvols,
                                           "/",
                                           subvol_new)

        # Move dest to new name
        dest = "{}/{}".format(self.mount_point, str(dest_hashed.newname))
        ret = move_file(self.mounts[0].client_system, dest_src, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(dest_src, dest)))

        # Move Source file to Dest
        src = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, src, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(src, dest)))

        # Verify Destination File is removed
        ret = self._verify_file_exists(dest_cached.hashedbrickobject,
                                       str(dest_cached.newname))
        self.assertFalse(ret, "The Source file is still present in {}"
                         .format(dest_cached.hashedbrickobject._fqpath))

        # Verify Source link is removed
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, "The source link file is still present in {}"
                         .format(src_link_subvol._fqpath))

        # Verify Destination Link is present and points to new dest file
        ret = self._verify_link_file_exists(dest_hashed.hashedbrickobject,
                                            str(dest_hashed.newname))
        self.assertTrue(ret, "The Dest link file is not present in {}"
                        .format(dest_hashed.hashedbrickobject._fqpath))

        file_path = dest_hashed.hashedbrickobject._fqpath + str(
            dest_hashed.newname)
        ret = (self._verify_file_links_to_specified_destination(
            dest_hashed.hashedbrickobject._host, file_path,
            str(dest_hashed.newname)))
        self.assertTrue(ret, "The dest link file not pointing towards "
                             "the desired file")
        g.log.info("The Destination link file is pointing to new file"
                   " as expected")

    def test_file_rename_when_dest_hash_src_hashed_but_cache_diff(self):
        """
        - Destination file should exist
        - Source file is stored on hashed subvolume it self
        - Destination file should be hased to some other subvolume(s2)
        - Destination file hased on subvolume(s2) but cached on some other
          subvolume(s3)(neither s1 nor s2)
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination link file should be there on hashed subvolume and
          should link to new destination file
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals

        # Create source file and Get hashed subvol (s1)
        _, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find a new file name for destination to hash to some subvol S3
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed,
                             "couldn't find new hashed for destination file")

        # Create Dest file in S3
        dest_cached, dest_count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed.newname)))

        # Verify S1 and S3 are not same
        self.assertNotEqual(src_count, dest_count,
                            ("The destination file is cached to the source "
                             "cached subvol"))

        # Find new name for dest file, that it hashes to some other subvol S2
        brickobject = create_brickobjectlist(self.subvols, "/")
        self.assertIsNotNone(brickobject, "Failed to get brick object list")
        br_count = -1
        subvol_new = None
        for brickdir in brickobject:
            br_count += 1
            if br_count not in (src_count, dest_count):
                subvol_new = brickdir
                break

        dest_hashed = find_specific_hashed(self.subvols,
                                           "/",
                                           subvol_new)
        # Move dest to new name
        dest = "{}/{}".format(self.mount_point, str(dest_hashed.newname))
        ret = move_file(self.mounts[0].client_system, dest_file, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(dest_file, dest)))

        # Move Source file to Dest
        ret = move_file(self.mounts[0].client_system, source_file, dest)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, dest)))

        # Verify Destination File is removed
        ret = self._verify_file_exists(dest_cached,
                                       str(new_hashed.newname))
        self.assertFalse(ret, "The Source file is still present in {}"
                         .format(dest_cached._fqpath))

        # Verify Destination Link is present and points to new dest file
        ret = self._verify_link_file_exists(dest_hashed.hashedbrickobject,
                                            str(dest_hashed.newname))
        self.assertTrue(ret, "The Dest link file is not present in {}"
                        .format(dest_hashed.hashedbrickobject._fqpath))

        file_path = dest_hashed.hashedbrickobject._fqpath + str(
            dest_hashed.newname)
        ret = (self._verify_file_links_to_specified_destination(
            dest_hashed.hashedbrickobject._host, file_path,
            str(dest_hashed.newname)))
        self.assertTrue(ret, "The dest link file not pointing towards "
                             "the desired file")
        g.log.info("The Destination link file is pointing to new file"
                   " as expected")
