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
    Test for rename of directory with subvol down
"""

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.dht_test_utils import (find_new_hashed,
                                               find_hashed_subvol)
from glustolibs.gluster.volume_libs import get_subvols


@runs_on([['distributed-replicated', 'distributed-dispersed'], ['glusterfs']])
class TestRenameDir(GlusterBaseClass):

    def setUp(self):
        """
        Setup and mount volume or raise ExecutionError
        """
        self.get_super_method(self, 'setUp')()

        # Setup Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            g.log.error("Failed to Setup and Mount Volume")
            raise ExecutionError("Failed to Setup and Mount Volume")

        # Setting up variables needed by the tests.
        self.mountpoint = self.mounts[0].mountpoint
        self.srcdir = self.mountpoint + '/srcdir'

    def create_src_and_dst_dir(self):
        """
        A function to create src and dst directory such that
        src and dst directories hashes to different sub volumes.
        """
        # pylint: disable=protected-access

        # Getting all the subvols.
        self.subvols = get_subvols(self.mnode,
                                   self.volname)['volume_subvols']

        # Create srcdir
        ret = mkdir(self.clients[0], self.srcdir, parents=True)
        self.assertTrue(ret, "mkdir srcdir failed")
        g.log.info("mkdir of srcdir successful")

        # Find hashed subvol
        self.srchashed, self.srccount = find_hashed_subvol(self.subvols,
                                                           "srcdir",
                                                           "srcdir")
        self.assertIsNotNone(self.srchashed,
                             "Could not find hashed subvol for srcdir")
        g.log.info("Hashed subvol for srcdir %s", self.srchashed._host)

        newhash = find_new_hashed(self.subvols, "srcdir", "srcdir")
        self.assertIsNotNone(newhash, "Could not find new hashed for dstdir")
        g.log.info("dstdir name : %s dst hashed_subvol : %s",
                   newhash.newname, newhash.hashedbrickobject._host)

        self.dstcount = newhash.subvol_count

        # Create dstdir
        self.dstdir = self.mountpoint + "/" + str(newhash.newname)

        ret = mkdir(self.clients[0], self.dstdir, parents=True)
        self.assertTrue(ret, "mkdir distdir failed")
        g.log.info("mkdir for dstdir successful")

    def test_rename_dir_src_hashed_down(self):
        """
        Case 1:
        1.mkdir srcdir and dstdir(such that srcdir and dstdir
        hashes to different subvols)
        2.Bring down srcdir hashed subvol
        3.mv srcdir dstdir (should fail)
        """
        # Create source and destination dir.
        self.create_src_and_dst_dir()

        # Bring down srchashed
        ret = bring_bricks_offline(self.volname, self.subvols[
            self.srccount])
        self.assertTrue(ret, 'Error in bringing down subvolume %s' %
                        self.subvols[self.srccount])
        g.log.info('target subvol %s is offline', self.subvols[self.srccount])

        # Rename the directory
        ret, _, err = g.run(self.clients[0], ("mv %s %s"
                                              % (self.srcdir, self.dstdir)))
        self.assertEqual(
            ret, 1,
            "Expected rename from %s to %s to fail" % (self.srcdir,
                                                       self.dstdir)
            )
        g.log.info("rename from %s to %s to failed as expected err %s",
                   self.srcdir, self.dstdir, err)

        # Bring up the subvol
        ret = bring_bricks_online(self.mnode, self.volname,
                                  self.subvols[self.srccount],
                                  bring_bricks_online_methods=[
                                      'volume_start_force'])
        self.assertTrue(ret, "Error in bringing back subvol online")
        g.log.info('Subvol is back online')

    def test_rename_dir_src_dst_hashed_down(self):
        """
        Case 2:
        1.mkdir srcdir dstdir (different hashes)
        2.Bring down srcdir hashed
        3.Bring down dstdir hashed
        4.mv srcdir dstdir (should fail)
        """
        # Create source and destination dir.
        self.create_src_and_dst_dir()

        # Bring down srchashed
        ret = bring_bricks_offline(self.volname,
                                   self.subvols[self.srccount])
        self.assertTrue(ret, 'Error in bringing down subvolume %s' %
                        self.subvols[self.srccount])
        g.log.info('target subvol %s is offline', self.subvols[
            self.srccount])

        # Bring down dsthashed
        ret = bring_bricks_offline(self.volname,
                                   self.subvols[self.dstcount])
        self.assertTrue(ret, 'Error in bringing down subvolume %s' %
                        self.subvols[self.dstcount])
        g.log.info('target subvol %s is offline', self.subvols[
            self.dstcount])

        # Rename the directory (should fail)
        ret, _, err = g.run(self.clients[0], ("mv %s %s"
                                              % (self.srcdir,
                                                 self.dstdir)))
        self.assertEqual(
            ret, 1,
            "Expected rename from %s to %s to fail" % (self.srcdir,
                                                       self.dstdir)
            )
        g.log.info("rename from %s to %s to failed as expected err %s",
                   self.srcdir, self.dstdir, err)

        # Bring up the subvol
        both_subvols = (self.subvols[self.srccount] +
                        self.subvols[self.dstcount])
        ret = bring_bricks_online(self.mnode, self.volname, both_subvols,
                                  bring_bricks_online_methods=[
                                      'volume_start_force'])
        self.assertTrue(ret, "Error in bringing back subvol online")
        g.log.info('Subvol is back online')

    def test_rename_dir_dst_hashed_down(self):
        """
        case - 3:
        1.mkdir srcdir dstdir
        2.Bring down dstdir hashed subvol
        3.mv srcdir dstdir (should fail)
        """
        # Create source and destination dir.
        self.create_src_and_dst_dir()

        # Bring down srchashed
        ret = bring_bricks_offline(self.volname, self.subvols[
            self.dstcount])
        self.assertTrue(ret, 'Error in bringing down subvolume %s' %
                        self.subvols[self.dstcount])
        g.log.info('target subvol %s is offline', self.subvols[
            self.dstcount])

        # Rename the directory
        ret, _, err = g.run(self.clients[0], ("mv %s %s"
                                              % (self.srcdir,
                                                 self.dstdir)))
        self.assertEqual(
            ret, 1,
            "Expected rename from %s to %s to fail" % (self.srcdir,
                                                       self.dstdir)
            )
        g.log.info("rename from %s to %s to failed as expected err %s",
                   self.srcdir, self.dstdir, err)

        # Bring up the subvol
        ret = bring_bricks_online(self.mnode, self.volname,
                                  self.subvols[self.dstcount],
                                  bring_bricks_online_methods=[
                                      'volume_start_force'])
        self.assertTrue(ret, "Error in bringing back subvol online")
        g.log.info('Subvol is back online')

    def tearDown(self):

        # Delete parent_dir
        ret, _, _ = g.run(self.clients[0], ("rm -rf %s/*" % self.mountpoint))
        if ret:
            raise ExecutionError('rm -rf * failed on mount')
        g.log.info("rm -rf * of directory mount successful")

        # Unmount Volume and Cleanup Volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()
