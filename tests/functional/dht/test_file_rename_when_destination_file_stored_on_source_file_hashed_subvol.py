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
                                               find_new_hashed,
                                               find_specific_hashed)
from glustolibs.gluster.volume_libs import get_subvols, parse_vol_file
from glustolibs.gluster.glusterfile import (move_file,
                                            is_linkto_file,
                                            get_dht_linkto_xattr)


@runs_on([['distributed-replicated', 'distributed',
           'distributed-dispersed', 'distributed-arbiter'],
          ['glusterfs']])
class DhtFileRenameWithDestFileHashed(GlusterBaseClass):

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
        self.assertEqual(ret, 0,
                         ("Failed to create %s : err %s", source_file, err))
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
                (bool): Based on existance of file link
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
               brickdir(Class Object): BrickDir object containing data about
                                       bricks under a specific subvol
               file_name(str): Name of the file to be searched
        Returns:
                (bool): Based on existance of file
        """
        # pylint: disable=protected-access

        cmd = "[ -f {} ]".format(brick_dir._fqpath +
                                 (str(file_name)))
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
                    if ret == 0:
                        return True
            g.log.error("The given link file doesn't point to any of "
                        "the subvolumes")
            return False
        else:
            remote_host = vol_data[link_to_xattr]['option']['remote-host']
            # Verify the new file is in the remote-subvol identified
            cmd = "[ -f {}/{} ]".format(remote_subvol, dest_file)
            ret, _, _ = g.run(remote_host, cmd)
            if ret == 0:
                return True
        return False

    def test_file_rename_when_source_and_dest_hash_diff_subvol(self):
        """
        - Destination file should exist
        - Source file is stored on hashed sub volume(s1) and cached on
          another subvolume(s2)
        - Destination file should be hased to subvolume where source file is
          stored(s2)
        - Destination file should hased subvolume(s2) but cached same
          subvolume(s1) where source file is hashed
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination link file should be removed
        - source link file should be removed
        """
        # pylint: disable=protected-access

        # Create soruce file and Get hashed subvol (s2)
        source_hashed_subvol, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Rename the file such that the new name hashes to a new subvol (S1)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed, ("could'nt find new hashed for {}"
                                          .format(source_file)))

        # Verify the subvols are not same for source and destination files
        self.assertNotEqual(src_count,
                            new_hashed.subvol_count,
                            "The subvols for src and dest are same.")

        # Rename/Move the file
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, "Failed to move files {} and {}".format(
            source_file, dest_file))

        # Verify the Source link file is stored on hashed sub volume(s1)
        src_link_subvol = new_hashed.hashedbrickobject
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))

        # Get a file name that stores to S1 for destination
        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           src_link_subvol,
                                           new_hashed.newname)
        self.assertIsNotNone(new_hashed2,
                             "could not find new hashed for dstfile")

        # Create destination file in subvol S1
        dest_hashed_subvol, dest_count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed2.newname)))

        # Verify the subvol is S1 itself
        self.assertEqual(new_hashed.subvol_count, dest_count,
                         "The destination file is not stored to desired "
                         "subvol :{}, instead to subvol : {}"
                         .format(new_hashed2.subvol_count, dest_count))

        # Create a linkfile to dest by renaming it to hash to S2
        dest_hashed = find_specific_hashed(self.subvols,
                                           "/",
                                           source_hashed_subvol)
        # Verify the subvol is S2
        self.assertEqual(dest_hashed.subvol_count, src_count,
                         "The destination file is not stored to desired "
                         "subvol :{}, instead to subvol : {}"
                         .format(dest_hashed.subvol_count, src_count))

        # Rename the source file to the new file name
        dest_file_2 = "{}/{}".format(self.mount_point,
                                     str(dest_hashed.newname))
        ret = move_file(self.mounts[0].client_system, dest_file, dest_file_2)
        self.assertTrue(ret, "Failed to move files {} and {}".format(
            source_file, dest_file_2))

        # Verify the Dest link file is stored on sub volume(s2)
        ret = self._verify_link_file_exists(source_hashed_subvol,
                                            str(dest_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(source_hashed_subvol._fqpath,
                                      str(dest_hashed.newname))))

        # Rename source to destination
        src = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        dest_file = "{}/{}".format(self.mount_point,
                                   str(dest_hashed.newname))
        ret = move_file(self.mounts[0].client_system, src, dest_file)
        self.assertTrue(ret, "Failed to move files {} and {}"
                        .format(src, dest_file))

        # Verify destination file is removed
        ret = self._verify_file_exists(dest_hashed_subvol,
                                       str(new_hashed2.newname))
        self.assertFalse(ret, ("Destination file : {} is not removed in subvol"
                               " : {}".format(str(new_hashed2.newname),
                                              dest_hashed_subvol._fqpath)))
        g.log.info("The destination file is removed as expected")

        # Verify the source link is removed
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, ("The hashed subvol {} still have the "
                               "expected linkto file: {}"
                               .format(src_link_subvol._fqpath,
                                       str(new_hashed.newname))))

        g.log.info("The source link file is removed as expected")

    def test_file_rename_when_source_and_dest_hash_same_subvol(self):
        """
        - Destination file should exist
        - Source file is hashed sub volume(s1) and cached on another
          subvolume(s2)
        - Destination file should be hased to same subvolume(s1) where
          source file is hased
        - Destination hashed on subvolume(s1) but should be cached on
          subvolume(s2) where source file is stored
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination link file should be there on hashed subvolume and
          should link to new destination file
        - source link file should be removed
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals

        # Create soruce file and Get hashed subvol (s2)
        source_hashed_subvol, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Rename the file such that the new name hashes to a new subvol (S1)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed, ("could'nt find new hashed for {}"
                                          .format(source_file)))

        # Verify the subvols are not same for source and destination files
        self.assertNotEqual(src_count,
                            new_hashed.subvol_count,
                            "The subvols for src and dest are same.")

        # Rename/Move the file
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, "Failed to move files {} and {}".format(
            source_file, dest_file))

        # Verify the Source link file is stored on hashed sub volume(s1)
        src_link_subvol = new_hashed.hashedbrickobject
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))

        # Get a file name that stores to S2 for destination
        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           source_hashed_subvol)
        self.assertIsNotNone(new_hashed2,
                             "could not find new hashed for dstfile")

        # Create destination file in subvol S2
        dest_hashed_subvol, dest_count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed2.newname)))

        # Verify the subvol is S2 itself
        self.assertEqual(dest_count, src_count,
                         "The destination file is not stored to desired "
                         "subvol :{}"
                         .format(dest_count))

        # Create a linkfile to dest by renaming it to hash to S1
        dest_hashed = find_specific_hashed(self.subvols,
                                           "/",
                                           src_link_subvol,
                                           new_hashed.newname)
        # Verify the subvol is S1
        self.assertEqual(dest_hashed.subvol_count, new_hashed.subvol_count,
                         "The destination file is not stored to desired "
                         "subvol :{}, instead to subvol : {}"
                         .format(dest_hashed.subvol_count, new_hashed))

        # Rename the dest file to the new file name
        dest_file_2 = "{}/{}".format(self.mount_point,
                                     str(dest_hashed.newname))
        ret = move_file(self.mounts[0].client_system, dest_file, dest_file_2)
        self.assertTrue(ret, "Failed to move files {} and {}".format(
            source_file, dest_file_2))

        # Rename source to destination
        src = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        dest_file = "{}/{}".format(self.mount_point,
                                   str(dest_hashed.newname))
        ret = move_file(self.mounts[0].client_system, src, dest_file)
        self.assertTrue(ret, "Failed to move files {} and {}"
                        .format(src, dest_file))

        # Verify destination file is removed
        ret = self._verify_file_exists(dest_hashed_subvol,
                                       str(new_hashed2.newname))
        self.assertFalse(ret, ("Destination file : {} is not removed in subvol"
                               " : {}".format(str(new_hashed2.newname),
                                              dest_hashed_subvol._fqpath)))
        g.log.info("The destination file is removed as expected")

        # Verify the source link is removed
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, ("The hashed subvol {} still have the "
                               "expected linkto file: {}"
                               .format(src_link_subvol._fqpath,
                                       str(new_hashed.newname))))

        g.log.info("The source link file is removed as expected")

        # Verify the Destination link is on hashed subvolume
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(dest_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(dest_hashed_subvol._fqpath,
                                      str(dest_hashed.newname))))

        # Verify the dest link file points to new destination file
        file_path = src_link_subvol._fqpath + str(dest_hashed.newname)
        ret = (self._verify_file_links_to_specified_destination(
            src_link_subvol._host, file_path, str(dest_hashed.newname)))
        self.assertTrue(ret, "The dest link file not pointing towards "
                             "the desired file")
        g.log.info("The Destination link file is pointing to new file"
                   " as expected")

    def test_file_rename_when_dest_hash_to_src_subvol(self):
        """
        - Destination file should exist
        - Source file is stored on hashed subvolume it self
        - Destination file should be hased to same subvolume(s1)
          where source file is
        - Destination file hased subvolume(s1) but cached onsubvolume(s2)
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination link file should be removed
        """
        # pylint: disable=protected-access

        # Create soruce file and Get hashed subvol (s1)
        source_hashed_subvol, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find a file name that hashes to another subvol (s2)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed, ("could'nt find new hashed for {}"
                                          .format(source_file)))

        # Create destination file in subvol S2
        _, dest_count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed.newname)))

        # Rename dest file such that it hashes to S1
        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           source_hashed_subvol)
        self.assertIsNotNone(new_hashed2,
                             "could not find new hashed for dstfile")

        # Verify the subvol is S1 itself
        self.assertEqual(new_hashed2.subvol_count, src_count,
                         "The destination file is not stored to desired "
                         "subvol :{}".format(dest_count))

        # Rename/Move the file
        dest_file2 = "{}/{}".format(self.mount_point, str(new_hashed2.newname))
        ret = move_file(self.mounts[0].client_system, dest_file, dest_file2)
        self.assertTrue(ret, "Failed to move files {} and {}"
                        .format(source_file, dest_file))

        # Verify the Dest link file is stored on hashed sub volume(s1)
        dest_link_subvol = new_hashed2.hashedbrickobject
        ret = self._verify_link_file_exists(dest_link_subvol,
                                            str(new_hashed2.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(dest_link_subvol._fqpath,
                                      str(new_hashed2.newname))))

        # Rename Source to Dest
        src = "{}/{}".format(self.mount_point, "test_source_file")
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed2.newname))
        ret = move_file(self.mounts[0].client_system, src, dest_file)
        self.assertTrue(ret, "Failed to move files {} and {}"
                        .format(src, dest_file))

        # Verify destination file is removed
        ret = self._verify_file_exists(new_hashed.hashedbrickobject,
                                       str(new_hashed.newname))
        self.assertFalse(ret, ("Destination file : {} is not removed in subvol"
                               " : {}".format(str(new_hashed.newname),
                                              new_hashed.hashedbrickobject
                                              ._fqpath)))
        g.log.info("The destination file is removed as expected")

        # Verify the Destination link is removed
        ret = self._verify_link_file_exists(new_hashed2.hashedbrickobject,
                                            str(new_hashed2.newname))
        self.assertFalse(ret, ("The hashed subvol {} still have the "
                               "expected linkto file: {}"
                               .format(new_hashed2.hashedbrickobject._fqpath,
                                       str(new_hashed2.newname))))

        g.log.info("The Destination link file is removed as expected")

    def test_file_rename_when_dest_cache_to_src_subvol(self):
        """
        - Destination file should exist
        - Source file is stored on hashed subvolume it self
        - Destination file should be hased to some other subvolume(s2)
        - Destination file hashed on subvolume(s2) but cached on the
          subvolume(s1) where souce file is present
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination link file should be there on hashed subvolume and
          should link to new destination file
        """
        # pylint: disable=protected-access

        # Create soruce file and Get hashed subvol (s1)
        source_hashed_subvol, src_count, _ = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find name for dest file to cache to S1
        dest_subvol = find_specific_hashed(self.subvols,
                                           "/",
                                           source_hashed_subvol)
        dest_name = str(dest_subvol.newname)

        # Create destination file in subvol S1
        _, dest_count, _ = self._create_file_and_get_hashed_subvol(dest_name)

        # Verify its subvol (s1)
        self.assertEqual(src_count, dest_count,
                         ("The newly created file falls under subvol {} "
                          "rather than {}".format(dest_count, src_count)))

        # Rename dest file such that it hashes to some other subvol S2
        dest_hashed_subvol = find_new_hashed(self.subvols,
                                             "/",
                                             dest_name)
        self.assertIsNotNone(dest_hashed_subvol,
                             "could not find new hashed for dstfile")

        # Rename/Move the file
        dest_file = "{}/{}".format(self.mount_point,
                                   dest_hashed_subvol.newname)
        src_file = "{}/{}".format(self.mount_point, dest_name)
        ret = move_file(self.mounts[0].client_system, src_file, dest_file)
        self.assertTrue(ret, "Failed to move files {} and {}"
                        .format(src_file, dest_file))

        # Verify the Dest link file is stored on hashed sub volume(s2)
        dest_link_subvol = dest_hashed_subvol.hashedbrickobject
        ret = self._verify_link_file_exists(dest_link_subvol,
                                            str(dest_hashed_subvol.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(dest_link_subvol._fqpath,
                                      str(dest_hashed_subvol.newname))))

        # Rename Source to Dest
        src = "{}/{}".format(self.mount_point, "test_source_file")
        dest_file = "{}/{}".format(self.mount_point,
                                   dest_hashed_subvol.newname)
        ret = move_file(self.mounts[0].client_system, src, dest_file)
        self.assertTrue(ret, "Failed to move files {} and {}"
                        .format(src, dest_file))

        # Verify destination file is removed
        ret = self._verify_file_exists(dest_subvol.hashedbrickobject,
                                       dest_name)
        self.assertFalse(ret, ("Destination file : {} is not removed in subvol"
                               " : {}"
                               .format(str(dest_hashed_subvol.newname),
                                       dest_link_subvol._fqpath)))
        g.log.info("The destination file is removed as expected")

        # Verify the Destination link is present
        ret = self._verify_link_file_exists(dest_link_subvol,
                                            str(dest_hashed_subvol.newname))
        self.assertTrue(ret, ("The hashed subvol {} still have the "
                              "expected linkto file: {}"
                              .format(dest_link_subvol._fqpath,
                                      str(dest_hashed_subvol.newname))))

        g.log.info("The Destination link file is present as expected")

        # Verify the dest link file points to new destination file
        file_path = dest_link_subvol._fqpath + str(dest_hashed_subvol.newname)
        ret = (self._verify_file_links_to_specified_destination(
            dest_link_subvol._host, file_path,
            str(dest_hashed_subvol.newname)))
        self.assertTrue(ret, "The dest link file not pointing towards "
                             "the desired file")
        g.log.info("The Destination link file is pointing to new file"
                   " as expected")
