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
"""
Description:
    Test cases in this module tests directory rmdir with subvol down
"""

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.gluster.volume_libs import get_subvols, volume_start
from glustolibs.gluster.glusterfile import file_exists
from glustolibs.gluster.dht_test_utils import (find_hashed_subvol,
                                               find_nonhashed_subvol,
                                               create_brickobjectlist)
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.constants import (TEST_LAYOUT_IS_COMPLETE as
                                          LAYOUT_IS_COMPLETE)
from glustolibs.gluster.glusterdir import mkdir, rmdir
from glustolibs.gluster.mount_ops import umount_volume, mount_volume


@runs_on([['distributed-replicated', 'distributed',
           'distributed-dispersed', 'distributed-arbiter'],
          ['glusterfs']])
class TestLookupDir(GlusterBaseClass):
    # Create Volume and mount according to config file
    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()
        # Change the dist count to 4 in case of 'distributed-replicated' ,
        # 'distributed-dispersed' and 'distributed-arbiter'
        if self.volume_type in ("distributed-replicated",
                                "distributed-dispersed",
                                "distributed-arbiter"):
            self.volume['voltype']['dist_count'] = 4

        # Setup Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup and Mount Volume")
        mount_obj = self.mounts[0]
        self.mountpoint = mount_obj.mountpoint

        # Collect subvols
        self.subvols = (get_subvols
                        (self.mnode, self.volname))['volume_subvols']

    def test_rmdir_child_when_nonhash_vol_down(self):
        """
        case -1:
        - create parent
        - bring down a non-hashed subvolume for directory child
        - create parent/child
        - rmdir /mnt/parent will fail with ENOTCONN
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-statements
        # pylint: disable=unsubscriptable-object
        # Find a non hashed subvolume(or brick)

        # Create parent dir
        parent_dir = self.mountpoint + '/parent'
        child_dir = parent_dir + '/child'
        ret = mkdir(self.clients[0], parent_dir)
        self.assertTrue(ret, "mkdir failed")
        g.log.info("mkdir of parent directory %s successful", parent_dir)

        # Find a non hashed subvolume(or brick)
        nonhashed_subvol, count = find_nonhashed_subvol(self.subvols,
                                                        "parent", "child")
        self.assertIsNotNone("Error in finding nonhashed value")
        g.log.info("nonhashed_subvol %s", nonhashed_subvol._host)

        # Bring nonhashed_subbvol offline
        ret = bring_bricks_offline(self.volname, self.subvols[count])
        self.assertTrue(ret, ("Error in bringing down subvolume %s"
                              % self.subvols[count]))
        g.log.info('target subvol %s is offline', self.subvols[count])

        # Create child-dir
        ret = mkdir(self.clients[0], child_dir)
        self.assertTrue(ret, ('mkdir failed for %s ' % child_dir))
        g.log.info("mkdir of child directory %s successful", child_dir)

        # 'rmdir' on parent should fail with ENOTCONN
        ret = rmdir(self.clients[0], parent_dir)
        self.assertFalse(ret, ('Expected rmdir to fail for %s' % parent_dir))
        g.log.info("rmdir of parent directory %s failed as expected",
                   parent_dir)

        # Cleanup
        # Bring up the subvol - restart volume
        ret = volume_start(self.mnode, self.volname, force=True)
        self.assertTrue(ret, "Error in force start the volume")
        g.log.info('Volume restart success')
        sleep(10)

        # Delete parent_dir
        ret = rmdir(self.clients[0], parent_dir, force=True)
        self.assertTrue(ret, ('rmdir failed for %s ' % parent_dir))
        g.log.info("rmdir of directory %s successful", parent_dir)

    def test_rmdir_dir_when_hash_nonhash_vol_down(self):
        """
        case -2:
        - create dir1 and dir2
        - bring down hashed subvol for dir1
        - bring down a non-hashed subvol for dir2
        - rmdir dir1 should fail with ENOTCONN
        - rmdir dir2 should fail with ENOTCONN
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-statements
        # pylint: disable=unsubscriptable-object

        # Create dir1 and dir2
        directory_list = []
        for number in range(1, 3):
            directory_list.append('{}/dir{}'.format(self.mountpoint, number))
            ret = mkdir(self.clients[0], directory_list[-1])
            self.assertTrue(ret, ('mkdir failed for %s '
                                  % directory_list[-1]))
            g.log.info("mkdir of directory %s successful",
                       directory_list[-1])

        # Find a non hashed subvolume(or brick)
        nonhashed_subvol, count = find_nonhashed_subvol(self.subvols, "/",
                                                        "dir1")
        self.assertIsNotNone(nonhashed_subvol,
                             "Error in finding nonhashed value")
        g.log.info("nonhashed_subvol %s", nonhashed_subvol._host)

        # Bring nonhashed_subbvol offline
        ret = bring_bricks_offline(self.volname, self.subvols[count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s'
                              % self.subvols[count]))
        g.log.info('target subvol %s is offline', self.subvols[count])

        # 'rmdir' on dir1 should fail with ENOTCONN
        ret = rmdir(self.clients[0], directory_list[0])
        self.assertFalse(ret, ('Expected rmdir to fail for %s'
                               % directory_list[0]))
        g.log.info("rmdir of directory %s failed as expected",
                   directory_list[0])

        # Bring up the subvol - restart volume
        ret = volume_start(self.mnode, self.volname, force=True)
        self.assertTrue(ret, "Error in force start the volume")
        g.log.info('Volume restart success')
        sleep(10)

        # Unmounting and Mounting the volume back to Heal
        ret, _, err = umount_volume(self.clients[1], self.mountpoint)
        self.assertFalse(ret, "Error in creating temp mount %s" % err)

        ret, _, err = mount_volume(self.volname,
                                   mtype='glusterfs',
                                   mpoint=self.mountpoint,
                                   mserver=self.servers[0],
                                   mclient=self.clients[1])
        self.assertFalse(ret, "Error in creating temp mount")

        ret, _, _ = g.run(self.clients[1], ("ls %s/dir1" % self.mountpoint))
        self.assertEqual(ret, 0, "Error in lookup for dir1")
        g.log.info("lookup successful for dir1")

        # This confirms that healing is done on dir1
        ret = validate_files_in_dir(self.clients[0],
                                    directory_list[0],
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "validate_files_in_dir for dir1 failed")
        g.log.info("healing successful for dir1")

        # Bring down the hashed subvol
        # Find a hashed subvolume(or brick)
        hashed_subvol, count = find_hashed_subvol(self.subvols, "/", "dir2")
        self.assertIsNotNone(hashed_subvol,
                             "Error in finding nonhashed value")
        g.log.info("hashed_subvol %s", hashed_subvol._host)

        # Bring hashed_subbvol offline
        ret = bring_bricks_offline(self.volname, self.subvols[count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              self.subvols[count]))
        g.log.info('target subvol %s is offline', self.subvols[count])

        # 'rmdir' on dir2 should fail with ENOTCONN
        ret = rmdir(self.clients[0], directory_list[1])
        self.assertFalse(ret, ('Expected rmdir to fail for %s'
                               % directory_list[1]))
        g.log.info("rmdir of dir2 directory %s failed as expected",
                   directory_list[1])

        # Cleanup
        # Bring up the subvol - restart the volume
        ret = volume_start(self.mnode, self.volname, force=True)
        self.assertTrue(ret, "Error in force start the volume")
        g.log.info('Volume restart success')
        sleep(10)

        # Delete dirs
        for directory in directory_list:
            ret = rmdir(self.clients[0], directory)
            self.assertTrue(ret, ('rmdir failed for %s ' % directory))
            g.log.info("rmdir of directory %s successful", directory)

    def test_rm_file_when_nonhash_vol_down(self):
        """
        case -3:
        - create parent
        - mkdir parent/child
        - touch parent/child/file
        - bringdown a subvol where file is not present
        - rm -rf parent
            - Only file should be deleted
            - rm -rf of parent should fail with ENOTCONN
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-statements
        # pylint: disable=unsubscriptable-object

        # Find a non hashed subvolume(or brick)
        # Create parent dir
        parent_dir = self.mountpoint + '/parent'
        child_dir = parent_dir + '/child'
        ret = mkdir(self.clients[0], parent_dir)
        self.assertTrue(ret, ('mkdir failed for %s ' % parent_dir))
        g.log.info("mkdir of parent directory %s successful", parent_dir)

        # Create child dir
        ret = mkdir(self.clients[0], child_dir)
        self.assertTrue(ret, ('mkdir failed for %s ' % child_dir))
        g.log.info("mkdir of child directory %s successful", child_dir)

        # Create a file under child_dir
        file_one = child_dir + '/file_one'
        ret, _, err = g.run(self.clients[0], ("touch %s" % file_one))
        self.assertFalse(ret, ('touch failed for %s err: %s' %
                               (file_one, err)))

        # Find a non hashed subvolume(or brick)
        nonhashed_subvol, count = find_nonhashed_subvol(self.subvols,
                                                        "parent/child",
                                                        "file_one")
        self.assertIsNotNone(nonhashed_subvol,
                             "Error in finding nonhashed value")
        g.log.info("nonhashed_subvol %s", nonhashed_subvol._host)

        # Bring nonhashed_subbvol offline
        ret = bring_bricks_offline(self.volname, self.subvols[count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s'
                              % self.subvols[count]))
        g.log.info('target subvol %s is offline', self.subvols[count])

        # 'rm -rf' on parent should fail with ENOTCONN
        ret = rmdir(self.clients[0], parent_dir)
        self.assertFalse(ret, ('Expected rmdir to fail for %s' % parent_dir))
        g.log.info("rmdir of parent directory %s failed as expected"
                   " with err %s", parent_dir, err)

        brickobject = create_brickobjectlist(self.subvols, "parent/child")
        self.assertIsNotNone(brickobject,
                             "could not create brickobject list")
        # Make sure file_one is deleted
        for brickdir in brickobject:
            dir_path = "%s/parent/child/file_one" % brickdir.path
            brick_path = dir_path.split(":")
            self.assertTrue((file_exists(brickdir._host, brick_path[1])) == 0,
                            ('Expected file %s not to exist on servers'
                             % parent_dir))
        g.log.info("file is deleted as expected")

        # Cleanup
        # Bring up the subvol - restart volume
        ret = volume_start(self.mnode, self.volname, force=True)
        self.assertTrue(ret, "Error in force start the volume")
        g.log.info('Volume restart success.')
        sleep(10)

        # Delete parent_dir
        ret = rmdir(self.clients[0], parent_dir, force=True)
        self.assertTrue(ret, ('rmdir failed for %s ' % parent_dir))
        g.log.info("rmdir of directory %s successful", parent_dir)

    def test_rmdir_parent_pre_nonhash_vol_down(self):
        """
        case -4:
        - Bring down a non-hashed subvol for parent_dir
        - mkdir parent
        - rmdir parent should fails with ENOTCONN
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-statements
        # pylint: disable=unsubscriptable-object

        nonhashed_subvol, count = find_nonhashed_subvol(self.subvols,
                                                        "/", "parent")
        self.assertIsNotNone(nonhashed_subvol,
                             'Error in finding  nonhashed subvol')
        g.log.info("nonhashed subvol %s", nonhashed_subvol._host)

        # Bring nonhashed_subbvol offline
        ret = bring_bricks_offline(self.volname, self.subvols[count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s'
                              % self.subvols[count]))
        g.log.info('target subvol %s is offline', self.subvols[count])

        parent_dir = self.mountpoint + '/parent'
        ret = mkdir(self.clients[0], parent_dir)
        self.assertTrue(ret, ('mkdir failed for %s ' % parent_dir))
        g.log.info("mkdir of parent directory %s successful", parent_dir)

        # 'rmdir' on parent should fail with ENOTCONN
        ret = rmdir(self.clients[0], parent_dir)
        self.assertFalse(ret, ('Expected rmdir to fail for %s' % parent_dir))
        g.log.info("rmdir of parent directory %s failed as expected",
                   parent_dir)

        # Cleanup
        # Bring up the subvol - restart volume
        ret = volume_start(self.mnode, self.volname, force=True)
        self.assertTrue(ret, "Error in force start the volume")
        g.log.info('Volume restart success.')
        sleep(10)

        # Delete parent_dir
        ret = rmdir(self.clients[0], parent_dir, force=True)
        self.assertTrue(ret, ('rmdir failed for %s ' % parent_dir))
        g.log.info("rmdir of directory %s successful", parent_dir)

    def tearDown(self):
        """
        Unmount Volume and Cleanup Volume
        """
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
