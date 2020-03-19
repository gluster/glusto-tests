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
"""
Description:
    Test file access with subvol down
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfile import get_file_stat
from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.gluster.brick_libs import bring_bricks_online
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.dht_test_utils import (
    find_new_hashed,
    find_hashed_subvol)
from glustolibs.gluster.mount_ops import mount_volume, umount_volume


@runs_on([['distributed-dispersed', 'distributed', 'distributed-replicated'],
          ['glusterfs']])
class TestFileAccessSubvolDown(GlusterBaseClass):
    """
    test case: (file access)
        - rename the file so that the hashed and cached are different
        - make sure file can be accessed as long as cached is up
    """
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

    def test_file_access(self):
        """
        Test file access.
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # get subvol list
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(subvols, "failed to get subvols")

        # create a file
        srcfile = mountpoint + '/testfile'
        ret, _, err = g.run(self.clients[0], ("touch %s" % srcfile))
        self.assertEqual(ret, 0, ("File creation failed for %s err %s",
                                  srcfile, err))
        g.log.info("testfile creation successful")

        # find hashed subvol
        srchashed, scount = find_hashed_subvol(subvols, "/", "testfile")
        self.assertIsNotNone(srchashed, "could not find srchashed")
        g.log.info("hashed subvol for srcfile %s subvol count %s",
                   srchashed._host, str(scount))

        # rename the file such that the new name hashes to a new subvol
        tmp = find_new_hashed(subvols, "/", "testfile")
        self.assertIsNotNone(tmp, "could not find new hashed for dstfile")
        g.log.info("dst file name : %s dst hashed_subvol : %s "
                   "subvol count : %s", tmp.newname,
                   tmp.hashedbrickobject._host, str(tmp.subvol_count))

        dstname = str(tmp.newname)
        dstfile = mountpoint + "/" + dstname
        dsthashed = tmp.hashedbrickobject
        dcount = tmp.subvol_count
        ret, _, err = g.run(self.clients[0], ("mv %s %s" %
                                              (srcfile, dstfile)))
        self.assertEqual(ret, 0, ("rename failed for %s err %s",
                                  srcfile, err))
        g.log.info("cmd: mv srcfile dstfile successful")

        # check that on dsthash_subvol the file is a linkto file
        filepath = dsthashed._fqpath + "/" + dstname
        file_stat = get_file_stat(dsthashed._host, filepath)
        self.assertEqual(file_stat['access'], "1000", ("Expected file "
                                                       "permission to be 1000"
                                                       " on subvol %s",
                                                       dsthashed._host))
        g.log.info("dsthash_subvol has the expected linkto file")

        # check on srchashed the file is a data file
        filepath = srchashed._fqpath + "/" + dstname
        file_stat = get_file_stat(srchashed._host, filepath)
        self.assertNotEqual(file_stat['access'], "1000", ("Expected file "
                                                          "permission not to"
                                                          "be 1000 on subvol"
                                                          "%s",
                                                          srchashed._host))

        # Bring down the hashed subvol of dstfile(linkto file)
        ret = bring_bricks_offline(self.volname, subvols[dcount])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[dcount]))
        g.log.info('dst subvol %s is offline', subvols[dcount])

        # Need to access the file through a fresh lookup through a new mount
        # create a new dir(choosing server to do a mount)
        ret, _, _ = g.run(self.mnode, ("mkdir -p /mnt"))
        self.assertEqual(ret, 0, ('mkdir of mount dir failed'))
        g.log.info("mkdir of mount dir succeeded")

        # do a temp mount
        ret = mount_volume(self.volname, self.mount_type, "/mnt",
                           self.mnode, self.mnode)
        self.assertTrue(ret, ('temporary mount failed'))
        g.log.info("temporary mount succeeded")

        # check that file is accessible (stat)
        ret, _, _ = g.run(self.mnode, ("stat /mnt/%s" % dstname))
        self.assertEqual(ret, 0, ('stat error on for dst file %s', dstname))
        g.log.info("stat on /mnt/%s successful", dstname)

        # cleanup temporary mount
        ret = umount_volume(self.mnode, "/mnt")
        self.assertTrue(ret, ('temporary mount failed'))
        g.log.info("umount successful")

        # Bring up the hashed subvol
        ret = bring_bricks_online(self.mnode, self.volname, subvols[dcount],
                                  bring_bricks_online_methods=None)
        self.assertTrue(ret, "Error in bringing back subvol online")
        g.log.info('Subvol is back online')

        # now bring down the cached subvol
        ret = bring_bricks_offline(self.volname, subvols[scount])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[scount]))
        g.log.info('target subvol %s is offline', subvols[scount])

        # file access should fail
        ret, _, _ = g.run(self.clients[0], ("stat %s" % dstfile))
        self.assertEqual(ret, 1, ('stat error on for file %s', dstfile))
        g.log.info("dstfile access failed as expected")

    @classmethod
    def tearDownClass(cls):
        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        cls.get_super_method(cls, 'tearDownClass')()
