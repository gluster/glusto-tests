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
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.io.utils import collect_mounts_arequal, validate_io_procs
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.dht_test_utils import (find_hashed_subvol,
                                               find_new_hashed)
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.brick_libs import bring_bricks_offline


@runs_on([['distributed', 'distributed-replicated',
           'distributed-arbiter', 'distributed-dispersed'],
          ['glusterfs']])
class TestCopyDirSubvolDown(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        cls.get_super_method(cls, 'setUpClass')()

        # Check for the default dist_count value and override it if required
        if cls.default_volume_type_config['distributed']['dist_count'] <= 2:
            cls.default_volume_type_config['distributed']['dist_count'] = 4
        else:
            cls.default_volume_type_config[cls.voltype]['dist_count'] = 3

        # Upload io scripts for running IO on mounts
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts "
                                 "to clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume([self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=[self.mounts[0]])
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def _create_src(self, m_point):
        """
        Create the source directory and files under the
        source directory.
        """
        # Create source dir
        ret = mkdir(self.mounts[0].client_system, "{}/src_dir".format(m_point))
        self.assertTrue(ret, "mkdir of src_dir failed")

        # Create files inside source dir
        cmd = ("/usr/bin/env python %s create_files "
               "-f 100 %s/src_dir/" % (
                   self.script_upload_path, m_point))
        proc = g.run_async(self.mounts[0].client_system,
                           cmd, user=self.mounts[0].user)
        g.log.info("IO on %s:%s is started successfully",
                   self.mounts[0].client_system, m_point)

        # Validate IO
        self.assertTrue(
            validate_io_procs([proc], self.mounts[0]),
            "IO failed on some of the clients"
        )

    def _copy_files_check_contents(self, m_point, dest_dir):
        """
        Copy files from source directory to destination
        directory when it hashes to up-subvol and check
        if all the files are copied properly.
        """
        # pylint: disable=protected-access
        # collect arequal checksum on src dir
        ret, src_checksum = collect_mounts_arequal(
            self.mounts[0], '{}/src_dir'.format(m_point))
        self.assertTrue(ret, ("Failed to get arequal on client"
                              " {}".format(self.clients[0])))

        # copy src_dir to dest_dir
        command = "cd {}; cp -r  src_dir {}".format(m_point, dest_dir)
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to copy of src dir to"
                                 " dest dir")
        g.log.info("Successfully copied src dir to dest dir.")

        # collect arequal checksum on destination dir
        ret, dest_checksum = collect_mounts_arequal(
            self.mounts[0], '{}/{}'.format(m_point, dest_dir))
        self.assertTrue(ret, ("Failed to get arequal on client"
                              " {}".format(self.mounts[0])))

        # Check if the contents of src dir are copied to
        # dest dir
        self.assertEqual(src_checksum,
                         dest_checksum,
                         'All the contents of src dir are not'
                         ' copied to dest dir')
        g.log.info('Successfully copied the contents of src dir'
                   ' to dest dir')

    def _copy_when_dest_hash_down(self, m_point, dest_dir):
        """
        Copy files from source directory to destination
        directory when it hashes to down-subvol.
        """
        # pylint: disable=protected-access
        # copy src_dir to dest_dir (should fail as hash subvol for dest
        # dir is down)
        command = "cd {}; cp -r  src_dir {}".format(m_point, dest_dir)
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 1, "Unexpected : Copy of src dir to"
                                 " dest dir passed")
        g.log.info("Copy of src dir to dest dir failed as expected.")

    def test_copy_existing_dir_dest_subvol_down(self):
        """
        Case 1:
        - Create directory from mount point.
        - Copy dir ---> Bring down dht sub-volume where destination
          directory hashes to down sub-volume.
        - Copy directory and make sure destination dir does not exist
        """
        # pylint: disable=protected-access
        m_point = self.mounts[0].mountpoint

        # Create source dir
        ret = mkdir(self.mounts[0].client_system, "{}/src_dir".format(m_point))
        self.assertTrue(ret, "mkdir of src_dir failed")
        g.log.info("Directory src_dir created successfully")

        # Get subvol list
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(subvols, "Failed to get subvols")

        # Find out the destination dir name such that it hashes to
        # different subvol
        newdir = find_new_hashed(subvols, "/", "src_dir")
        dest_dir = str(newdir.newname)
        dest_count = newdir.subvol_count

        # Kill the brick/subvol to which the destination dir hashes
        ret = bring_bricks_offline(
            self.volname, subvols[dest_count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[dest_count]))
        g.log.info('DHT subvol %s is offline', subvols[dest_count])

        # Copy src_dir to dest_dir (should fail as hash subvol for dest
        # dir is down)
        self._copy_when_dest_hash_down(m_point, dest_dir)

    def test_copy_existing_dir_dest_subvol_up(self):
        """
        Case 2:
        - Create files and directories from mount point.
        - Copy dir ---> Bring down dht sub-volume where destination
          directory should not hash to down sub-volume
        - copy dir and make sure destination dir does not exist
        """
        # pylint: disable=protected-access
        m_point = self.mounts[0].mountpoint

        # Create source dir and create files inside it
        self._create_src(m_point)

        # Get subvol list
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(subvols, "Failed to get subvols")

        # Find out hashed brick/subvol for src dir
        src_subvol, src_count = find_hashed_subvol(subvols, "/", "src_dir")
        self.assertIsNotNone(src_subvol, "Could not find srchashed")
        g.log.info("Hashed subvol for src_dir is %s", src_subvol._path)

        # Find out the destination dir name such that it hashes to
        # different subvol
        newdir = find_new_hashed(subvols, "/", "src_dir")
        dest_dir = str(newdir.newname)
        dest_count = newdir.subvol_count

        # Remove the hashed subvol for dest and src dir from the
        # subvol list
        for item in (subvols[src_count], subvols[dest_count]):
            subvols.remove(item)

        # Bring down a DHT subvol
        ret = bring_bricks_offline(self.volname, subvols[0])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[0]))
        g.log.info('DHT subvol %s is offline', subvols[0])

        # Create files on source dir and
        # perform copy of src_dir to dest_dir
        self._copy_files_check_contents(m_point, dest_dir)

    def test_copy_new_dir_dest_subvol_up(self):
        """
        Case 3:
        - Copy dir ---> Bring down dht sub-volume where destination
          directory should not hash to down sub-volume
        - Create files and directories from mount point.
        - copy dir and make sure destination dir does not exist
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-statements
        m_point = self.mounts[0].mountpoint

        # Get subvols
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(subvols, "Failed to get subvols")

        # Find out hashed brick/subvol for src dir
        src_subvol, src_count = find_hashed_subvol(
            subvols, "/", "src_dir")
        self.assertIsNotNone(src_subvol, "Could not find srchashed")
        g.log.info("Hashed subvol for src_dir is %s", src_subvol._path)

        # Find out the destination dir name such that it hashes to
        # different subvol
        newdir = find_new_hashed(subvols, "/", "src_dir")
        dest_dir = str(newdir.newname)
        dest_count = newdir.subvol_count

        # Remove the hashed subvol for dest and src dir from the
        # subvol list
        for item in (subvols[src_count], subvols[dest_count]):
            subvols.remove(item)

        # Bring down a dht subvol
        ret = bring_bricks_offline(self.volname, subvols[0])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[0]))
        g.log.info('DHT subvol %s is offline', subvols[0])

        # Create source dir and create files inside it
        self._create_src(m_point)

        # Create files on source dir and
        # perform copy of src_dir to dest_dir
        self._copy_files_check_contents(m_point, dest_dir)

    def test_copy_new_dir_dest_subvol_down(self):
        """
         Case 4:
        - Copy dir ---> Bring down dht sub-volume where destination
          directory hashes to down sub-volume
        - Create directory from mount point.
        - Copy dir and make sure destination dir does not exist
        """
        # pylint: disable=protected-access
        m_point = self.mounts[0].mountpoint

        # Get subvol list
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(subvols, "Failed to get subvols")

        # Find out the destination dir name such that it hashes to
        # different subvol
        newdir = find_new_hashed(subvols, "/", "src_dir")
        dest_dir = str(newdir.newname)
        dest_count = newdir.subvol_count

        # Bring down the hashed-subvol for dest dir
        ret = bring_bricks_offline(self.volname, subvols[dest_count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[dest_count]))
        g.log.info('DHT subvol %s is offline', subvols[dest_count])

        # Create source dir
        ret = mkdir(self.mounts[0].client_system, "{}/src_dir".format(m_point))
        self.assertTrue(ret, "mkdir of src_dir failed")
        g.log.info("Directory src_dir created successfully")

        # Copy src_dir to dest_dir (should fail as hash subvol for dest
        # dir is down)
        self._copy_when_dest_hash_down(m_point, dest_dir)
