#  Copyright (C) 2020 Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY :or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Description:
    Test Glusterfind when brick is down
"""

from random import choice
from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.peer_ops import wait_for_peers_to_connect
from glustolibs.gluster.lib_utils import list_files
from glustolibs.gluster.volume_libs import volume_start
from glustolibs.gluster.glusterfile import (
    file_exists,
    remove_file,
    check_if_pattern_in_file)
from glustolibs.gluster.glusterfind_ops import (
    gfind_create,
    gfind_list,
    gfind_pre,
    gfind_post,
    gfind_delete)
from glustolibs.gluster.brick_libs import (
    get_all_bricks,
    bring_bricks_offline)


@runs_on([["replicated", "distributed-replicated", "dispersed",
           "distributed", "distributed-dispersed"],
          ["glusterfs"]])
class TestGlusterFindBrickDown(GlusterBaseClass):
    """
    Test glusterfind operation when a brick is down.
    """

    def setUp(self):
        """
        setup volume and mount volume
        Initiate necessary variables
        """

        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.file_limit = 0

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume %s" % self.volname)
        g.log.info("Successful in Setup Volume %s", self.volname)
        self.session = "test-session-%s" % self.volname
        self.outfiles = [("/tmp/test-outfile-%s-%s.txt"
                          % (self.volname, i))for i in range(0, 2)]

        # Set the changelog rollover-time to 1 second
        # This needs to be done in order for glusterfind to keep checking
        # for changes in the mount point
        option = {'changelog.rollover-time': '1'}
        ret = set_volume_options(self.mnode, self.volname, option)
        if not ret:
            raise ExecutionError("Failed to set the volume option %s for %s"
                                 % (option, self.volname))
        g.log.info("Successfully set the volume option for the volume %s",
                   self.volname)

    def _perform_io_and_validate_presence_of_files(self):
        """
        Function to perform the IO and validate the presence of files.
        """
        self.file_limit += 10
        # Starting IO on the mounts
        cmd = ("cd %s ; touch file{%d..%d}" % (self.mounts[0].mountpoint,
                                               self.file_limit-10,
                                               self.file_limit))

        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create files on mountpoint")
        g.log.info("Files created successfully on mountpoint")

        # Gather the list of files from the mount point
        files = list_files(self.mounts[0].client_system,
                           self.mounts[0].mountpoint)
        self.assertIsNotNone(files, "Failed to get the list of files")
        g.log.info("Successfully gathered the list of files from mount point")

        # Check if the files exist
        for filename in files:
            ret = file_exists(self.mounts[0].client_system, filename)
            self.assertTrue(ret, ("Unexpected: File '%s' does not exist"
                                  % filename))
            g.log.info("Successfully validated existence of '%s'", filename)

    def _perform_glusterfind_pre_and_validate_outfile(self):
        """
        Function to perform glusterfind pre and validate outfile
        """
        # Perform glusterfind pre for the session
        ret, _, _ = gfind_pre(self.mnode, self.volname, self.session,
                              self.outfiles[0], full=True, noencode=True,
                              debug=True)
        self.assertEqual(ret, 0, ("Failed to perform glusterfind pre"))
        g.log.info("Successfully performed glusterfind pre")

        # Check if the outfile exists
        ret = file_exists(self.mnode, self.outfiles[0])
        self.assertTrue(ret, ("Unexpected: File '%s' does not exist"
                              % self.outfiles[0]))
        g.log.info("Successfully validated existence of '%s'",
                   self.outfiles[0])

        # Check if all the files are listed in the outfile
        for i in range(1, self.file_limit+1):
            ret = check_if_pattern_in_file(self.mnode, "file%s" % i,
                                           self.outfiles[0])
            self.assertEqual(ret, 0, ("File 'file%s' not listed in %s"
                                      % (i, self.outfiles[0])))
            g.log.info("File 'file%s' listed in %s", i, self.outfiles[0])

    def test_gfind_when_brick_down(self):
        """
        Verifying the glusterfind functionality when a brick is down.

        1. Create a volume
        2. Create a session on the volume
        3. Create various files from mount point
        4. Bring down brick process on one of the node
        5. Perform glusterfind pre
        6. Perform glusterfind post
        7. Check the contents of outfile
        """

        # pylint: disable=too-many-statements
        # Create a session for the volume
        ret, _, _ = gfind_create(self.mnode, self.volname, self.session)
        self.assertEqual(ret, 0, ("Unexpected: Creation of a session for the "
                                  "volume %s failed" % self.volname))
        g.log.info("Successfully created a session for the volume %s",
                   self.volname)

        # Perform glusterfind list to check if session exists
        _, out, _ = gfind_list(self.mnode, volname=self.volname,
                               sessname=self.session)
        self.assertNotEqual(out, "No sessions found.",
                            "Failed to list the glusterfind session")
        g.log.info("Successfully listed the glusterfind session")

        self._perform_io_and_validate_presence_of_files()

        # Wait for changelog to get updated
        sleep(2)

        # Bring one of the brick down.
        brick_list = get_all_bricks(self.mnode, self.volname)
        ret = bring_bricks_offline(self.volname, choice(brick_list))
        self.assertTrue(ret, "Failed to bring down the brick.")
        g.log.info("Succesfully brought down one brick.")

        self._perform_glusterfind_pre_and_validate_outfile()

        # Perform glusterfind post for the session
        ret, _, _ = gfind_post(self.mnode, self.volname, self.session)
        self.assertEqual(ret, 0, ("Failed to perform glusterfind post"))
        g.log.info("Successfully performed glusterfind post")

        # Bring the brick process up.
        ret = volume_start(self.mnode, self.volname, force=True)
        self.assertTrue(ret, "Failed to start the volume.")
        g.log.info("Successfully started the volume.")

    def tearDown(self):
        """
        tearDown for every test
        Clean up and unmount the volume
        """
        # calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

        # Delete the glusterfind sessions
        ret, _, _ = gfind_delete(self.mnode, self.volname, self.session)
        if ret:
            raise ExecutionError("Failed to delete session %s" % self.session)
        g.log.info("Successfully deleted session %s", self.session)

        # Remove the outfiles created during 'glusterfind pre'
        for out in self.outfiles:
            ret = remove_file(self.mnode, out, force=True)
            if not ret:
                raise ExecutionError("Failed to remove the outfile %s" % out)
        g.log.info("Successfully removed the outfiles")

        # Wait for the peers to be connected.
        ret = wait_for_peers_to_connect(self.mnode, self.servers, 100)
        if not ret:
            raise ExecutionError("Peers are not in connected state.")

        # Cleanup the volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")
