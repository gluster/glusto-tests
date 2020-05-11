#  Copyright (C) 2017-2020 Red Hat, Inc. <http://www.redhat.com>
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
    Test File creation with hashed and cached subvol down scenarios
"""

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfile import calculate_hash
from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.gluster.brickdir import BrickDir
from glustolibs.gluster.volume_libs import get_subvols


@runs_on([['distributed-replicated', 'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class TestCreateFile(GlusterBaseClass):
    '''
    test case: (file creation)
        - Verify that the file is created on the hashed subvol alone
        - Verify that the trusted.glusterfs.pathinfo reflects the file location
        - Verify that the file creation fails if the hashed subvol is down
    '''
    # Create Volume and mount according to config file
    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError("Failed to Setup and Mount Volume")

    def test_create_file(self):
        '''
        Test file creation.
        '''
        # pylint: disable=too-many-locals
        # pylint: disable=protected-access
        # pylint: disable=too-many-statements
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # files that needs to be created
        file_one = mountpoint + '/file1'

        # hash for file_one
        filehash = calculate_hash(self.servers[0], 'file1')

        # collect subvol info
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        secondary_bricks = []
        for subvol in subvols:
            secondary_bricks.append(subvol[0])

        brickobject = []
        for item in secondary_bricks:
            temp = BrickDir(item)
            brickobject.append(temp)

        # create a file
        ret, _, _ = g.run(self.clients[0], ("touch %s" % file_one))
        self.assertEqual(ret, 0, ("File %s creation failed", file_one))

        # get pathinfo xattr on the file
        ret, out, err = g.run(self.clients[0],
                              ("getfattr -n trusted.glusterfs.pathinfo %s" %
                               file_one))
        g.log.info("pathinfo o/p %s", out)
        self.assertEqual(ret, 0, ("failed to get pathinfo on file %s err %s",
                                  file_one, err))

        vol_type = self.volume_type
        if vol_type == "distributed":
            brickhost = (out.split(":"))[3]
            brickpath = (out.split(":"))[4].split(">")[0]
        else:
            brickhost = (out.split(":"))[4]
            brickpath = (out.split(":")[5]).split(">")[0]

        g.log.debug("brickhost %s brickpath %s", brickhost, brickpath)

        # make sure the file is present only on the hashed brick
        count = -1
        for brickdir in brickobject:
            count += 1
            ret = brickdir.hashrange_contains_hash(filehash)
            if ret == 1:
                hash_subvol = subvols[count]
                ret, _, err = g.run(brickdir._host, ("stat %s/file1" %
                                                     brickdir._fqpath))
                g.log.info("Hashed subvol is %s", brickdir._host)
                self.assertEqual(ret, 0, "Expected stat to succeed for file1")
                continue

            ret, _, err = g.run(brickdir._host, ("stat %s/file1" %
                                                 brickdir._fqpath))
            self.assertEqual(ret, 1, "Expected stat to fail for file1")

        # checking if pathinfo xattr has the right value
        ret, _, _ = g.run(brickhost, ("stat %s" % brickpath))
        self.assertEqual(ret, 0, ("Expected file1 to be present on %s",
                                  brickhost))

        # get permission from mount
        ret, out, _ = g.run(self.clients[0], ("ls -l %s" % file_one))
        mperm = (out.split(" "))[0]
        self.assertIsNotNone(mperm, "Expected stat to fail for file1")
        g.log.info("permission on mount %s", mperm)

        # get permission from brick
        ret, out, _ = g.run(brickhost, ("ls -l %s" % brickpath))
        bperm = (out.split(" "))[0]
        self.assertIsNotNone(bperm, "Expected stat to fail for file1")
        g.log.info("permission on brick %s", bperm)

        # check if the permission matches
        self.assertEqual(mperm, bperm, "Expected permission to match")

        # check that gfid xattr is present on the brick
        ret, _, _ = g.run(brickhost, ("getfattr -n trusted.gfid %s" %
                                      brickpath))
        self.assertEqual(ret, 0, "gfid is not present on file")

        # delete the file, bring down it's hash, create the file,
        ret, _, _ = g.run(self.clients[0], ("rm -f %s" % file_one))
        self.assertEqual(ret, 0, "file deletion for file1 failed")

        ret = bring_bricks_offline(self.volname, hash_subvol)
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              hash_subvol))

        # check file creation should fail
        ret, _, _ = g.run(self.clients[0], ("touch %s" % file_one))
        self.assertTrue(ret, "Expected file creation to fail")

    def tearDown(self):

        # Unmount and cleanup original volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
