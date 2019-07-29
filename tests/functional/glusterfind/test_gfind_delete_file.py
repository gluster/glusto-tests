#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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

from time import sleep
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_ops import set_volume_options, volume_reset
from glustolibs.gluster.glusterfind_ops import (gfind_create,
                                                gfind_list,
                                                gfind_pre,
                                                gfind_post,
                                                gfind_delete)
from glustolibs.gluster.glusterfile import (file_exists, remove_file,
                                            check_if_pattern_in_file)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class TestGlusterFindDeletes(GlusterBaseClass):
    """
    TestGlusterFindDeletes contains tests which verifies the
    glusterfind functionality with renames of files.
    """

    def setUp(self):
        """
        setup volume and mount volume
        Initiate necessary variables
        """

        # calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup %s", self.volname)
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume %s" % self.volname)
        g.log.info("Successful in Setup Volume %s", self.volname)
        self.session = 'test-session-%s' % self.volname
        self.outfiles = [('/tmp/test-outfile-%s-%s.txt'
                          % (self.volname, i))for i in range(0, 2)]

        # Set the changelog rollover-time to 1 second
        g.log.info("Setting the changelog rollover-time to 1 second")
        option = {'changelog.rollover-time': '1'}
        ret = set_volume_options(self.mnode, self.volname, option)
        if not ret:
            raise ExecutionError("Failed to set the volume option %s for %s"
                                 % (option, self.volname))
        g.log.info("Successfully set the volume option for the volume %s",
                   self.volname)

    def tearDown(self):
        """
        tearDown for every test
        Clean up and unmount the volume
        """

        ret, _, _ = gfind_delete(self.mnode, self.volname, self.session)
        if ret != 0:
            raise ExecutionError("Failed to delete session %s" % self.session)
        g.log.info("Successfully deleted session %s", self.session)

        g.log.info("Removing the outfiles created during 'glusterfind pre'")
        for out in self.outfiles:
            ret = remove_file(self.mnode, out, force=True)
            if not ret:
                raise ExecutionError("Failed to remove the outfile %s" % out)
        g.log.info("Successfully removed the outfiles")

        # Reset the volume
        g.log.info("Reset the volume")
        ret, _, _ = volume_reset(self.mnode, self.volname)
        if ret != 0:
            raise ExecutionError("Failed to reset the volume %s"
                                 % self.volname)
        g.log.info("Successfully reset the volume %s", self.volname)

        # Cleanup the volume
        g.log.info("Starting to Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

        # calling GlusterBaseClass tearDownClass
        GlusterBaseClass.tearDown.im_func(self)

    def test_gfind_deletes(self):
        """
        Verifying the glusterfind functionality with deletion of files.

        * Create a volume
        * Create a session on the volume
        * Create various files from mount point
        * Perform glusterfind pre
        * Perform glusterfind post
        * Check the contents of outfile
        * Delete the files created from mount point
        * Perform glusterfind pre
        * Perform glusterfind post
        * Check the contents of outfile
          Files deleted must be listed
        """

        # pylint: disable=too-many-statements
        # Creating a session for the volume
        g.log.info("Creating a session for the volume %s", self.volname)
        ret, _, _ = gfind_create(self.mnode, self.volname, self.session)
        self.assertEqual(ret, 0, ("Unexpected: Creation of a session for the "
                                  "volume %s failed" % self.volname))
        g.log.info("Successfully created a session for the volume %s",
                   self.volname)

        # Perform glusterfind list to check if session exists
        g.log.info("Performing glusterfind list to check if the session is "
                   "created")
        ret, _, _ = gfind_list(self.mnode, volname=self.volname,
                               sessname=self.session)
        self.assertEqual(ret, 0, "Failed to list the glusterfind session")
        g.log.info("Successfully listed the glusterfind session")

        # Starting IO on the mounts
        g.log.info("Creating Files on %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        cmd = ("cd %s ; for i in `seq 1 10` ; "
               "do dd if=/dev/urandom of=file$i bs=1M count=1 ; "
               "done" % self.mounts[0].mountpoint)
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertEqual(ret, 0, "Failed to create files on mountpoint")
        g.log.info("Files created successfully on mountpoint")

        # Check if the files exist
        g.log.info("Checking the existence of files created during IO")
        for i in range(1, 11):
            ret = file_exists(self.mounts[0].client_system,
                              '%s/file%s' % (self.mounts[0].mountpoint, i))
            self.assertTrue(ret, "Unexpected: File 'file%s' does not exist"
                            % i)
            g.log.info("Successfully validated existence of 'file%s'", i)

        sleep(5)

        # Perform glusterfind pre for the session
        g.log.info("Performing glusterfind pre for the session %s",
                   self.session)
        ret, _, _ = gfind_pre(self.mnode, self.volname, self.session,
                              self.outfiles[0], full=True, noencode=True,
                              debug=True)
        self.assertEqual(ret, 0, ("Failed to perform glusterfind pre"))
        g.log.info("Successfully performed glusterfind pre")

        # Check if the outfile exists
        g.log.info("Checking if outfile created during glusterfind pre command"
                   " exists")
        ret = file_exists(self.mnode, self.outfiles[0])
        self.assertTrue(ret, "Unexpected: File '%s' does not exist"
                        % self.outfiles[0])
        g.log.info("Successfully validated existence of '%s'",
                   self.outfiles[0])

        # Check if all the files are listed in the outfile
        for i in range(1, 11):
            ret = check_if_pattern_in_file(self.mnode, 'file%s' % i,
                                           self.outfiles[0])
            self.assertEqual(ret, 0, ("File 'file%s' not listed in %s"
                                      % (i, self.outfiles[0])))
            g.log.info("File 'file%s' listed in %s", i, self.outfiles[0])

        # Perform glusterfind post for the session
        g.log.info("Performing glusterfind post for the session %s",
                   self.session)
        ret, _, _ = gfind_post(self.mnode, self.volname, self.session)
        self.assertEqual(ret, 0, ("Failed to perform glusterfind post"))
        g.log.info("Successfully performed glusterfind post")

        # Delete the files created from mount point
        g.log.info("Deleting the Files on %s:%s",
                   self.mounts[0].client_system, self.mounts[0].mountpoint)
        for i in range(1, 11):
            ret = remove_file(self.mounts[0].client_system,
                              "%s/file%s" % (self.mounts[0].mountpoint, i),
                              force=True)
            self.assertTrue(ret, "Failed to delete file%s" % i)
        g.log.info("Successfully deleted all the files")

        # Check if the files deleted exist from mount point
        g.log.info("Checking the existence of files that were deleted "
                   "(must not be present)")
        for i in range(1, 11):
            ret = file_exists(self.mounts[0].client_system,
                              '%s/file%s' % (self.mounts[0].mountpoint, i))
            self.assertFalse(ret, "Unexpected: File 'file%s' exists even after"
                             " being deleted" % i)
            g.log.info("Successfully validated 'file%s' does not exist", i)

        sleep(5)

        # Perform glusterfind pre for the session
        g.log.info("Performing glusterfind pre for the session %s",
                   self.session)
        ret, _, _ = gfind_pre(self.mnode, self.volname, self.session,
                              self.outfiles[1], debug=True)
        self.assertEqual(ret, 0, ("Failed to perform glusterfind pre"))
        g.log.info("Successfully performed glusterfind pre")

        # Check if the outfile exists
        g.log.info("Checking if outfile created during glusterfind pre command"
                   " exists")
        ret = file_exists(self.mnode, self.outfiles[1])
        self.assertTrue(ret, "Unexpected: File '%s' does not exist"
                        % self.outfiles[1])
        g.log.info("Successfully validated existence of '%s'",
                   self.outfiles[1])

        # Check if all the files are listed in the outfile
        for i in range(1, 11):
            pattern = "DELETE file%s" % i
            ret = check_if_pattern_in_file(self.mnode, pattern,
                                           self.outfiles[1])
            self.assertEqual(ret, 0, ("File 'file%s' not listed in %s"
                                      % (i, self.outfiles[1])))
            g.log.info("File 'file%s' listed in %s", i, self.outfiles[1])
