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

# pylint: disable=protected-access
# pylint: disable=too-many-statements
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.brickdir import BrickDir
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.dht_test_utils import (find_hashed_subvol,
                                               find_new_hashed,
                                               find_specific_hashed)
from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.gluster.glusterfile import move_file


@runs_on([['distributed', 'distributed-dispersed',
           'distributed-arbiter', 'distributed-replicated'],
          ['glusterfs']])
class TestCopyFileSubvolDown(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

        # Override the default dist_count value
        cls.default_volume_type_config[cls.voltype]['dist_count'] = 4

    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        self.client, self.m_point = (self.mounts[0].client_system,
                                     self.mounts[0].mountpoint)

        self.subvols = (get_subvols(
            self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(self.subvols, "Failed to get subvols")

    def tearDown(self):

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _create_src_file(self):
        """Create a srcfile"""
        cmd = "touch {}/srcfile".format(self.m_point)
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, "Failed to create srcfile")
        g.log.info("Successfully created srcfile")

    def _find_hash_for_src_file(self):
        """Find a new hashsubvol which is different from hash of srcfile"""
        src_hash_subvol = find_new_hashed(self.subvols, "/", "srcfile")
        new_src_name = str(src_hash_subvol.newname)
        src_hash_subvol_count = src_hash_subvol.subvol_count
        return new_src_name, src_hash_subvol_count

    def _find_cache_for_src_file(self):
        """Find out hash subvol for srcfile which after rename will become
        cache subvol"""
        src_cache_subvol, src_cache_subvol_count = find_hashed_subvol(
            self.subvols, "/", "srcfile")
        self.assertIsNotNone(src_cache_subvol, "Could not find src cached")
        g.log.info("Cached subvol for srcfile is %s", src_cache_subvol._path)
        return src_cache_subvol_count

    def _rename_src(self, new_src_name):
        """Rename the srcfile to a new name such that it hashes and
        caches to different subvols"""
        ret = move_file(self.client, "{}/srcfile".format(self.m_point),
                        ("{}/".format(self.m_point) + new_src_name))
        self.assertTrue(ret, ("Failed to move file srcfile and {}".format(
            new_src_name)))

    def _create_dest_file_find_hash(
            self, src_cache_subvol_count, src_hash_subvol_count):
        """Find a name for dest file such that it hashed to a subvol different
        from the src file's hash and cache subvol"""
        # Get subvol list
        subvol_list = (get_subvols(self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(subvol_list, "Failed to get subvols")
        for item in (subvol_list[src_hash_subvol_count],
                     subvol_list[src_cache_subvol_count]):
            subvol_list.remove(item)

        # Find name for dest file
        dest_subvol = BrickDir(subvol_list[0][0] + "/" + "/")
        dest_file = find_specific_hashed(self.subvols, "/", dest_subvol)
        self.assertIsNotNone(dest_file, "Could not find hashed for destfile")

        # Create dest file
        cmd = "touch {}/{}".format(self.m_point, dest_file.newname)
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, "Failed to create destfile")
        g.log.info("Successfully created destfile")
        return dest_file.newname, dest_file.subvol_count

    def _kill_subvol(self, subvol_count):
        """Bring down the subvol as the subvol_count"""
        ret = bring_bricks_offline(
            self.volname, self.subvols[subvol_count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              self.subvols[subvol_count]))
        g.log.info('DHT subvol %s is offline',
                   self.subvols[subvol_count])

    def _copy_src_file_to_dest_file(
            self, src_file, dest_file, expected="pass"):
        """
        Copy src file to dest dest, it will either pass or
        fail; as per the scenario
        """
        command = "cd {}; cp -r  {} {}".format(
            self.m_point, src_file, dest_file)
        expected_ret = 0 if expected == "pass" else 1
        ret, _, _ = g.run(self.client, command)
        self.assertEqual(ret, expected_ret,
                         "Unexpected, Copy of Src file to dest "
                         "file status : %s" % (expected))
        g.log.info("Copy of src file to dest file returned as expected")

    def test_copy_srchash_up_desthash_up(self):
        """
        Case 1:
        1) Create a volume and start it
        2) Create a src file and a dest file
        3) All subvols are up
        4) Copy src file to dest file
        """
        # Create a src file
        self._create_src_file()

        # Find out cache subvol for src file
        src_cache_count = self._find_cache_for_src_file()

        # Find new hash for src file
        src_file_new, src_hash_count = self._find_hash_for_src_file()

        # Rename src file so it hash and cache to different subvol
        self._rename_src(src_file_new)

        # Create dest file and find its hash subvol
        dest_file, _ = self._create_dest_file_find_hash(
            src_cache_count, src_hash_count)

        # Copy src file to dest file
        self._copy_src_file_to_dest_file(src_file_new, dest_file)

    def test_copy_srccache_down_srchash_up_desthash_down(self):
        """
        Case 2:
        1) Create a volume and start it
        2) Create a src file and a dest file
        3) Bring down the cache subvol for src file
        4) Bring down the hash subvol for dest file
        5) Copy src file to dest file
        """
        # Create a src file
        self._create_src_file()

        # Find out cache subvol for src file
        src_cache_count = self._find_cache_for_src_file()

        # Find new hash for src file
        src_file_new, src_hash_count = self._find_hash_for_src_file()

        # Rename src file so it hash and cache to different subvol
        self._rename_src(src_file_new)

        # Create dest file and find its hash subvol
        dest_file, dest_hash_count = self._create_dest_file_find_hash(
            src_cache_count, src_hash_count)

        # kill src cache subvol
        self._kill_subvol(src_cache_count)

        # Kill dest hash subvol
        self._kill_subvol(dest_hash_count)

        # Copy src file to dest file
        self._copy_src_file_to_dest_file(src_file_new, dest_file,
                                         expected="fail")

    def test_copy_srccache_down_srchash_up_desthash_up(self):
        """
        Case 3:
        1) Create a volume and start it
        2) Create a src file and a dest file
        3) Bring down the cache subvol for src file
        4) Copy src file to dest file
        """
        # Create a src file
        self._create_src_file()

        # Find out cache subvol for src file
        src_cache_count = self._find_cache_for_src_file()

        # Find new hash for src file
        src_file_new, src_hash_count = self._find_hash_for_src_file()

        # Rename src file so it hash and cache to different subvol
        self._rename_src(src_file_new)

        # Create dest file and find its hash subvol
        dest_file, _ = self._create_dest_file_find_hash(
            src_cache_count, src_hash_count)

        # kill src cache subvol
        self._kill_subvol(src_cache_count)

        # Copy src file to dest file
        self._copy_src_file_to_dest_file(src_file_new, dest_file,
                                         expected="fail")

    def test_copy_srchash_down_desthash_down(self):
        """
        Case 4:
        1) Create a volume and start it
        2) Create a src file and a dest file
        3) Bring down the hash subvol for src file
        4) Bring down the hash subvol for dest file
        5) Copy src file to dest file
        """
        # Create a src file
        self._create_src_file()

        # Find out cache subvol for src file
        src_cache_count = self._find_cache_for_src_file()

        # Find new hash for src file
        src_file_new, src_hash_count = self._find_hash_for_src_file()

        # Rename src file so it hash and cache to different subvol
        self._rename_src(src_file_new)

        # Create dest file and find its hash subvol
        dest_file, dest_hash_count = self._create_dest_file_find_hash(
            src_cache_count, src_hash_count)

        # Kill the hashed subvol for src file
        self._kill_subvol(src_hash_count)

        # Kill the hashed subvol for dest file
        self._kill_subvol(dest_hash_count)

        # Copy src file to dest file
        self._copy_src_file_to_dest_file(src_file_new, dest_file,
                                         expected="fail")

    def test_copy_srchash_down_desthash_up(self):
        """
        Case 5:
        1) Create a volume and start it
        2) Create a src file and a dest file
        3) Bring down the hash subvol for src file
        4) Copy src file to dest file
        """
        # Create a src file
        self._create_src_file()

        # Find out cache subvol for src file
        src_cache_count = self._find_cache_for_src_file()

        # Find new hash for src file
        src_file_new, src_hash_count = self._find_hash_for_src_file()

        # Rename src file so it hash and cache to different subvol
        self._rename_src(src_file_new)

        # Create dest file and find its hash subvol
        dest_file, _ = self._create_dest_file_find_hash(
            src_cache_count, src_hash_count)

        # Kill the hashed subvol for src file
        self._kill_subvol(src_hash_count)

        # Copy src file to dest file
        self._copy_src_file_to_dest_file(src_file_new, dest_file)

    def test_copy_srchash_up_desthash_down(self):
        """
        Case 6:
        1) Create a volume and start it
        2) Create a src file and a dest file
        3) Bring down the hash subvol for dest file
        4) Copy src file to dest file
        """
        # Create a src file
        self._create_src_file()

        # Find out cache subvol for src file
        src_cache_count = self._find_cache_for_src_file()

        # Find new hash for src file
        src_file_new, src_hash_count = self._find_hash_for_src_file()

        # Rename src file so it hash and cache to different subvol
        self._rename_src(src_file_new)

        # Create dest file and find its hash subvol
        dest_file, dest_hash_count = self._create_dest_file_find_hash(
            src_cache_count, src_hash_count)

        # Kill the hashed subvol for dest file
        self._kill_subvol(dest_hash_count)

        # Copy src file to dest file
        self._copy_src_file_to_dest_file(src_file_new, dest_file,
                                         expected="fail")
