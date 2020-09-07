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

from glusto.core import Glusto as g
from glustolibs.gluster.glusterfile import get_file_stat
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.dht_test_utils import (find_hashed_subvol,
                                               create_brickobjectlist,
                                               find_new_hashed,
                                               find_specific_hashed)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.glusterfile import move_file, is_linkto_file


@runs_on([['distributed', 'distributed-replicated',
           'distributed-dispersed', 'distributed-arbiter'],
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
                True/False(bool): Based on existance of file link
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
                True/False(bool): Based on existance of file
        """
        # pylint: disable=protected-access

        cmd = "[ -f {} ]".format(brick_dir._fqpath + (str(file_name)))
        ret, _, _ = g.run(brick_dir._host, cmd)
        if ret != 0:
            return False
        return True

    def test_dht_file_rename_dest_exists_src_and_dest_hash_diff(self):
        """
        case 6 :
        - Destination file should exist
        - Source file is stored on hashed subvolume(s1) it self
        - Destination file should be hashed to some other subvolume(s2)
        - Destination file is stored on hashed subvolume
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Destination hashed file should be created on its hashed
          subvolume(s2)
        """
        # pylint: disable=protected-access
        # pylint: disable=unsubscriptable-object

        # Create source file and Get hashed subvol (s1)
        _, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find a new file name for destination file, which hashes
        # to another subvol (s2)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed,
                             "could'nt find new hashed for destination file")

        # create destination_file and get its hashed subvol (s2)
        dest_hashed_subvol, dest_count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed.newname)))

        # Verify the subvols are not same for source and destination files
        self.assertNotEqual(src_count,
                            dest_count,
                            "The subvols for src and dest are same.")

        # Rename the source file to the destination file
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, "Failed to move files {} and {}".format(
            source_file, dest_file))

        # Verify destination file is removed
        ret = self._verify_file_exists(dest_hashed_subvol,
                                       str(new_hashed.newname))
        self.assertTrue(ret, ("Destination file : {} is not removed in subvol"
                              " : {}".format(str(new_hashed.newname),
                                             dest_hashed_subvol._fqpath)))
        g.log.info("The destination file is removed as expected")

        # Verify the Destination link is found in new subvol (s2)
        ret = self._verify_link_file_exists(dest_hashed_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The New hashed volume {} doesn't have the "
                              "expected linkto file {}"
                              .format(str(new_hashed.newname),
                                      dest_hashed_subvol._fqpath)))
        g.log.info("New hashed volume has the expected linkto file")

    def test_dht_file_rename_dest_exists_src_and_dest_hash_same(self):
        """
        Case 7:
        - Destination file should exist
        - Source file is stored on hashed subvolume(s1) it self
        - Destination file should be hashed to same subvolume(s1)
        - Destination file is stored on hashed subvolume
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed to destination file
        """
        # pylint: disable=protected-access
        # pylint: disable=unsubscriptable-object

        # Create soruce file and Get hashed subvol (s1)
        source_hashed_subvol, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Find a new file name for destination file that hashes
        # to same subvol (s1)
        new_hashed = find_specific_hashed(self.subvols,
                                          "/",
                                          source_hashed_subvol)
        self.assertIsNotNone(new_hashed, "Couldn't find a new hashed subvol "
                                         "for destination file")

        # Create destination_file and get its hashed subvol (should be s1)
        dest_hashed_subvol, dest_count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed.newname)))

        # Verify the subvols are not same for source and destination files
        self.assertEqual(src_count, dest_count,
                         "The subvols for src and dest are not same.")

        # Rename the source file to the destination file
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, ("Failed to move files {} and {}"
                              .format(source_file, dest_file)))

        # Verify the file move and the destination file is hashed to
        # same subvol or not
        _, rename_count = find_hashed_subvol(self.subvols,
                                             "/",
                                             str(new_hashed.newname))
        self.assertEqual(dest_count,
                         rename_count,
                         ("The subvols for source : {} and dest : {} are "
                          "not same.".format(source_hashed_subvol._fqpath,
                                             dest_hashed_subvol._fqpath)))

        # Verify destination file is removed
        ret = self._verify_file_exists(dest_hashed_subvol,
                                       str(new_hashed.newname))
        self.assertTrue(ret, ("Destination file : {} is not removed in subvol"
                              " : {}".format(str(new_hashed.newname),
                                             dest_hashed_subvol._fqpath)))
        g.log.info("The destination file is removed as expected")

    def test_file_rename_dest_exist_and_not_hash_src_srclink_subvol(self):
        """
        Case 8:
        - Destination file should exist
        - Source file is hashed sub volume(s1) and
          cached on another subvolume(s2)
        - Destination file should be hashed to some other subvolume(s3)
          (should not be same subvolumes mentioned in above condition)
             mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file should be renamed as destination file
        - Souce hashed file should be removed
        - Destination hashed file should be created on its hashed subvolume(s3)
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals
        # pylint: disable=unsubscriptable-object

        # Find a non hashed subvolume(or brick)
        # Create soruce file and Get hashed subvol (s2)
        _, count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Rename the file to create link in hashed subvol -(s1)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed,
                             "could not find new hashed for dstfile")
        count2 = new_hashed.subvol_count
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

        # Find a subvol (s3) other than S1 and S2
        brickobject = create_brickobjectlist(self.subvols, "/")
        self.assertIsNotNone(brickobject, "Failed to get brick object list")
        br_count = -1
        subvol_new = None
        for brickdir in brickobject:
            br_count += 1
            if br_count not in (count, count2):
                subvol_new = brickdir
                break

        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           subvol_new)
        self.assertIsNotNone(new_hashed2,
                             "could not find new hashed for dstfile")

        # Create destination file in a new subvol (s3)
        dest_hashed_subvol, dest_count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed2.newname)))

        # Verify the subvol is not same as S1 or S2
        self.assertNotEqual(count2, dest_count,
                            ("The subvols for src :{} and dest : {} are same."
                             .format(count2, dest_count)))
        # Verify the subvol is not same as S1 or S2
        self.assertNotEqual(count, dest_count,
                            ("The subvols for src :{} and dest : {} are same."
                             .format(count, dest_count)))

        # Rename the source file to the destination file
        source_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, dest_file)))

        # Verify destination file is removed
        ret = self._verify_file_exists(dest_hashed_subvol,
                                       str(new_hashed2.newname))
        self.assertTrue(ret, ("Destination file : {} is not removed in subvol"
                              " : {}".format(str(new_hashed.newname),
                                             dest_hashed_subvol._fqpath)))
        g.log.info("The destination file is removed as expected")

        # Check that the source link file is removed.
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, ("The New hashed volume {} still have the "
                               "expected linkto file {}"
                               .format(src_link_subvol._fqpath,
                                       str(new_hashed.newname))))
        g.log.info("The source link file is removed")

        # Check Destination link file is created on its hashed sub-volume(s3)
        ret = self._verify_link_file_exists(dest_hashed_subvol,
                                            str(new_hashed2.newname))
        self.assertTrue(ret, ("The New hashed volume {} doesn't have the "
                              "expected linkto file {}"
                              .format(dest_hashed_subvol._fqpath,
                                      str(new_hashed2.newname))))
        g.log.info("Destinaion link is created in desired subvol")

    def test_file_rename_dest_exist_and_hash_to_src_subvol(self):
        """
       Case 9:
       - Destination file should exist
       - Source file is hashed sub volume(s1) and
         cached on another subvolume(s2)
       - Destination file should be hashed to subvolume where source file
         is cached(s2)
            mv <source_file> <destination_file>
       - Destination file is removed.
       - Source file should be renamed as destination file
       - Souce hashed file should be removed
        """
        # pylint: disable=protected-access
        # pylint: disable=unsubscriptable-object

        # Get hashed subvol (S2)
        source_hashed_subvol, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Rename the file to create link in hashed subvol -(s1)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed, ("could not find new hashed for {}"
                                          .format(source_file)))

        # Rename the source file to the new file name
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, dest_file)))

        # Verify the Source link file is stored on hashed sub volume(s1)
        src_link_subvol = new_hashed.hashedbrickobject
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The New hashed volume {} doesn't have the "
                              "expected linkto file {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))

        # Get a file name for dest file to hash to the subvol s2
        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           source_hashed_subvol)
        self.assertIsNotNone(new_hashed2, "Could not find a name hashed"
                                          "to the given subvol")

        # Create destination file in the subvol (s2)
        dest_hashed_subvol, dest_count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed2.newname)))

        # Verify the subvol is same as S2
        self.assertEqual(src_count, dest_count,
                         "The subvols for src and dest are not same.")

        # Move the source file to the new file name
        source_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, dest_file)))

        # Verify destination file is removed
        ret = self._verify_file_exists(dest_hashed_subvol,
                                       str(new_hashed2.newname))
        self.assertTrue(ret, ("Destination file : {} is not removed in subvol"
                              " : {}".format(str(new_hashed.newname),
                                             dest_hashed_subvol._fqpath)))
        g.log.info("The destination file is removed as expected")

        # Check that the source link file is removed.
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, ("The New hashed volume {} still have the "
                               "expected linkto file {}"
                               .format(src_link_subvol._fqpath,
                                       str(new_hashed.newname))))
        g.log.info("The source link file is removed")

    def test_file_rename_dest_exist_and_hash_to_srclink_subvol(self):
        """
        Case 10:
        - Destination file should exist
        - Source file is hashed sub volume(s1) and
          cached on another subvolume(s2)
        - Destination file should be hashed to same subvolume(s1) where source
          file is hashed.
            mv <source_file> <destination_file>
        - Destination file is removed.
        - Source file(cached) should be renamed to destination file
        - Source file(hashed) should be removed.
        - Destination hahshed file should be created on its
          hashed subvolume(s1)
        """
        # pylint: disable=protected-access
        # pylint: disable=unsubscriptable-object

        # Get hashed subvol s2)
        _, src_count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Rename the file to create link in another subvol - (s1)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed, ("could not find new hashed subvol "
                                          "for {}".format(source_file)))

        self.assertNotEqual(src_count,
                            new_hashed.subvol_count,
                            "New file should hash to different sub-volume")

        # Rename the source file to the new file name
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, dest_file)))

        # Verify the Source link file is stored on hashed sub volume(s1)
        src_link_subvol = new_hashed.hashedbrickobject
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The New hashed volume {} doesn't have the "
                              "expected linkto file {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))

        # Get a file name for dest to hash to the subvol s1
        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           src_link_subvol,
                                           new_hashed.newname)
        self.assertIsNotNone(new_hashed2, ("Couldn't find a name hashed to the"
                                           " given subvol {}"
                                           .format(src_link_subvol)))
        # Create destination file in the subvol (s2)
        dest_hashed_subvol, dest_count, dest_file = (
            self._create_file_and_get_hashed_subvol(str(new_hashed2.newname)))

        # Verify the subvol is same as S1
        self.assertEqual(new_hashed.subvol_count, dest_count,
                         "The subvols for src and dest are not same.")

        # Move the source file to the new file name
        source_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed2.newname))
        ret = move_file(self.mounts[0].client_system, source_file, dest_file)
        self.assertTrue(ret, "Failed to move file")

        # Verify destination file is removed
        ret = self._verify_file_exists(dest_hashed_subvol,
                                       str(new_hashed2.newname))
        self.assertTrue(ret, ("Destination file : {} is not removed in subvol"
                              " : {}".format(str(new_hashed.newname),
                                             dest_hashed_subvol._fqpath)))
        g.log.info("The destination file is removed as expected")

        # Check that the source link file is removed.
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, ("The hashed volume {} still have the "
                               "expected linkto file {}"
                               .format(src_link_subvol._fqpath,
                                       str(new_hashed.newname))))
        g.log.info("The source link file is removed")

        # Check Destination link file is created on its hashed sub-volume(s1)
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed2.newname))
        self.assertTrue(ret, ("The New hashed volume {} doesn't have the "
                              "expected linkto file {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed2.newname))))
        g.log.info("Destinaion link is created in desired subvol")
