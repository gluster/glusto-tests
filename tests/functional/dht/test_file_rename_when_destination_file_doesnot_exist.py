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
class DhtFileRenameVerification(GlusterBaseClass):

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
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")

        mount_obj = self.mounts[0]
        self.mount_point = mount_obj.mountpoint

        self.subvols = (get_subvols(
            self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(self.subvols, "failed to get subvols")

    def tearDown(self):
        """
        Unmount Volume and Cleanup Volume
        """
        # Unmount Volume and Cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
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
        ret, _, err = g.run(self.clients[0], ("touch %s" % source_file))
        self.assertEqual(ret, 0, ("Failed to create {} : err {}"
                                  .format(source_file, err)))
        g.log.info("Successfully created the source file")

        # Find the hashed subvol for source file
        source_hashed_subvol, count = find_hashed_subvol(self.subvols,
                                                         "/",
                                                         file_name)
        self.assertIsNotNone(source_hashed_subvol, ("Couldn't find hashed "
                                                    "subvol for the {}"
                                                    .format(source_file)))
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

    def test_file_rename_when_source_and_dest_hash_diff_subvol(self):
        """
        case 1 :
        - Destination file does not exist
        - Source file is stored on hashed subvolume(s1) it self
        - Destination file should be hashed to some other subvolume(s2)
            mv <source_file> <destination_file>
        - Source file should be renamed to to Destination file.
        - Destination link file should be created on its hashed
          subvolume(s2)
        """
        # pylint: disable=protected-access
        # pylint: disable=unsubscriptable-object

        # Create soruce file and Get hashed subvol (s2)
        _, count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Rename the file such that the new name hashes to a new subvol (S1)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed, ("could'nt find new hashed for {}"
                                          .format(source_file)))
        src_link_subvol = new_hashed.hashedbrickobject

        # Verify the subvols are not same for source and destination files
        self.assertNotEqual(count,
                            new_hashed.subvol_count,
                            "The subvols for src and dest are same.")

        # Rename the source file to the destination file
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.clients[0], source_file, dest_file)
        self.assertTrue(ret, ("Failed to move files {} and {}"
                              .format(source_file, dest_file)))

        # Verify the link file is found in new subvol
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertTrue(ret, ("The hashed subvol {} doesn't have the "
                              "expected linkto file: {}"
                              .format(src_link_subvol._fqpath,
                                      str(new_hashed.newname))))
        g.log.info("New hashed volume has the expected linkto file")

    def test_file_rename_when_source_and_dest_hash_same_subvol(self):
        """
        Case 2:
        - Destination file does not exist
        - Source file is stored on hashed subvolume(s1) it self
        - Destination file should be hashed to same subvolume(s1)
            mv <source_file> <destination_file>
        - Source file should be renamed to destination file
        """
        # pylint: disable=protected-access
        # pylint: disable=unsubscriptable-object

        # Create soruce file and Get hashed subvol (s1)
        source_hashed_subvol, count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Rename the file such that the new name hashes to a new subvol
        new_hashed = find_specific_hashed(self.subvols,
                                          "/",
                                          source_hashed_subvol)
        self.assertIsNotNone(new_hashed,
                             "could not find new hashed for destination file")

        # Rename the source file to the destination file
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.clients[0], source_file, dest_file)
        self.assertTrue(ret, "Failed to move files {} and {}".format(
            source_file, dest_file))

        _, rename_count = find_hashed_subvol(self.subvols,
                                             "/",
                                             str(new_hashed.newname))
        self.assertEqual(count, rename_count,
                         "The hashed subvols for src and dest are not same.")

    def test_file_rename_when_dest_not_hash_to_src_or_src_link_subvol(self):
        """
        Case 3:
        - Destination file does not exist
        - Source link file is stored on hashed sub volume(s1) and Source
          file is stored on another subvolume(s2)
        - Destination file should be hashed to some other subvolume(s3)
          (should not be same subvolumes mentioned in above condition)
             mv <source_file> <destination_file>
        - Source file should be ranamed to destination file
        - source link file should be removed.
        - Destination link file should be created on its hashed
          subvolume(s3)
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
        ret = move_file(self.clients[0], source_file, dest_file)
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

        # find a subvol (s3) other than S1 and S2
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

        # Rename the source file to the destination file
        source_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed2.newname))
        ret = move_file(self.clients[0], source_file, dest_file)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, dest_file)))

        hashed_subvol_after_rename, rename_count = (
            find_hashed_subvol(self.subvols,
                               "/",
                               str(new_hashed2.newname)))
        self.assertNotEqual(count2, rename_count,
                            "The subvols for src and dest are same.")

        # check that the source link file is removed.
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, ("The New hashed volume {} still have the "
                               "expected linkto file {}"
                               .format(src_link_subvol._fqpath,
                                       str(new_hashed.newname))))
        g.log.info("The source link file is removed")

        # Check Destination link file is created on its hashed sub-volume(s3)
        ret = self._verify_link_file_exists(hashed_subvol_after_rename,
                                            str(new_hashed2.newname))
        self.assertTrue(ret, ("The New hashed volume {} doesn't have the "
                              "expected linkto file {}"
                              .format(hashed_subvol_after_rename._fqpath,
                                      str(new_hashed2.newname))))
        g.log.info("Destinaion link is created in desired subvol")

    def test_file_rename_when_src_file_and_dest_file_hash_same_subvol(self):
        """
       Case 4:
       - Destination file does not exist
       - Source link file is stored on hashed sub volume(s1) and Source
         file is stored on another subvolume(s2)
       - Destination file should be hashed to same subvolume(s2)
            mv <source_file> <destination_file>
       - Source file should be ranamed to destination file
       - source link file should be removed.
        """
        # pylint: disable=protected-access
        # pylint: disable=unsubscriptable-object

        # Get hashed subvol (S2)
        source_hashed_subvol, count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Rename the file to create link in hashed subvol -(s1)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed, ("could not find new hashed for {}"
                                          .format(source_file)))

        # Rename the source file to the new file name
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.clients[0], source_file, dest_file)
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

        # Get a file name to hash to the subvol s2
        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           source_hashed_subvol)
        self.assertIsNotNone(new_hashed2, "Could not find a name hashed"
                                          "to the given subvol")

        _, rename_count = (
            find_hashed_subvol(self.subvols, "/", str(new_hashed2.newname)))
        self.assertEqual(count, rename_count,
                         "The subvols for src and dest are not same.")

        # Move the source file to the new file name
        source_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed2.newname))
        ret = move_file(self.clients[0], source_file, dest_file)
        self.assertTrue(ret, ("Failed to move file {} and {}"
                              .format(source_file, dest_file)))

        # check that the source link file is removed.
        ret = self._verify_link_file_exists(src_link_subvol,
                                            str(new_hashed.newname))
        self.assertFalse(ret, ("The New hashed volume {} still have the "
                               "expected linkto file {}"
                               .format(src_link_subvol._fqpath,
                                       str(new_hashed.newname))))
        g.log.info("The source link file is removed")

    def test_file_rename_when_src_link_and_dest_file_hash_same_subvol(self):
        """
        Case 5:
       - Destination file does not exist
       - Source link file is stored on hashed sub volume(s1) and Source
         file is stored on another subvolume(s2)
       - Destination file should be hashed to same subvolume(s1)
            mv <source_file> <destination_file>
       - Source file should be renamed to destination file
       - Source link file should be removed.
       - Destination link file should be created on its
         hashed subvolume(s1)
        """
        # pylint: disable=protected-access
        # pylint: disable=unsubscriptable-object

        # Get hashed subvol s2)
        _, count, source_file = (
            self._create_file_and_get_hashed_subvol("test_source_file"))

        # Rename the file to create link in another subvol - (s1)
        new_hashed = find_new_hashed(self.subvols, "/", "test_source_file")
        self.assertIsNotNone(new_hashed, ("could not find new hashed subvol "
                                          "for {}".format(source_file)))

        self.assertNotEqual(count,
                            new_hashed.subvol_count,
                            "New file should hash to different sub-volume")

        # Rename the source file to the new file name
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        ret = move_file(self.clients[0], source_file, dest_file)
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

        # Get a file name to hash to the subvol s1
        new_hashed2 = find_specific_hashed(self.subvols,
                                           "/",
                                           src_link_subvol,
                                           new_hashed.newname)
        self.assertIsNotNone(new_hashed2, ("Couldn't find a name hashed to the"
                                           " given subvol {}"
                                           .format(src_link_subvol)))

        _, rename_count = (
            find_hashed_subvol(self.subvols, "/", str(new_hashed2.newname)))
        self.assertEqual(new_hashed.subvol_count, rename_count,
                         "The subvols for src and dest are not same.")

        # Move the source file to the new file name
        source_file = "{}/{}".format(self.mount_point, str(new_hashed.newname))
        dest_file = "{}/{}".format(self.mount_point, str(new_hashed2.newname))
        ret = move_file(self.clients[0], source_file, dest_file)
        self.assertTrue(ret, "Failed to move file")

        # check that the source link file is removed.
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
