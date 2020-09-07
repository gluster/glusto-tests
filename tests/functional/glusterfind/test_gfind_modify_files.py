#  Copyright (C) 2019-2020  Red Hat, Inc. <http://www.redhat.com>
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
from glustolibs.gluster.volume_ops import (
    set_volume_options,
    volume_reset)
from glustolibs.gluster.lib_utils import (
    append_string_to_file,
    list_files)
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


@runs_on([["replicated", "distributed-replicated", "dispersed",
           "distributed", "distributed-dispersed"],
          ["glusterfs"]])
class TestGlusterFindModify(GlusterBaseClass):
    """
    TestGlusterFindModify contains tests which verifies the
    glusterfind functionality with modification of files.
    """

    def setUp(self):
        """
        setup volume and mount volume
        Initiate necessary variables
        """

        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup %s", self.volname)
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

        # Reset the volume
        ret, _, _ = volume_reset(self.mnode, self.volname)
        if ret:
            raise ExecutionError("Failed to reset the volume %s"
                                 % self.volname)
        g.log.info("Successfully reset the volume %s", self.volname)

        # Cleanup the volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

    def test_gfind_modify(self):
        """
        Verifying the glusterfind functionality with deletion of files.

        * Create a volume
        * Create a session on the volume
        * Create various files from mount point
        * Perform glusterfind pre
        * Perform glusterfind post
        * Check the contents of outfile
        * Modify the contents of the files from mount point
        * Perform glusterfind pre
        * Perform glusterfind post
        * Check the contents of outfile
          Files modified must be listed
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

        # Starting IO on the mounts
        cmd = "cd %s ; touch file{1..10}" % self.mounts[0].mountpoint
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

        # Wait for changelog to get updated
        sleep(2)

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
        for i in range(1, 11):
            ret = check_if_pattern_in_file(self.mnode, "file%s" % i,
                                           self.outfiles[0])
            self.assertEqual(ret, 0, ("File 'file%s' not listed in %s"
                                      % (i, self.outfiles[0])))
            g.log.info("File 'file%s' listed in %s", i, self.outfiles[0])

        # Perform glusterfind post for the session
        ret, _, _ = gfind_post(self.mnode, self.volname, self.session)
        self.assertEqual(ret, 0, ("Failed to perform glusterfind post"))
        g.log.info("Successfully performed glusterfind post")

        # Modify the files created from mount point
        mod_string = "this is a test string\n"
        for filenum in files:
            ret = append_string_to_file(self.mounts[0].client_system, filenum,
                                        mod_string)
            self.assertTrue(ret, "Failed to append to file '%s'" % filenum)
        g.log.info("Successfully modified all the files")

        # Check if the files modified exist from mount point
        for filenum in files:
            ret = check_if_pattern_in_file(self.mounts[0].client_system,
                                           mod_string, filenum)
            self.assertEqual(ret, 0, ("Unexpected: File '%s' does not contain"
                                      " the string '%s' after being modified"
                                      % (filenum, mod_string)))
            g.log.info("Successfully validated '%s' is modified", filenum)

        # Wait for changelog to get updated
        sleep(2)

        # Perform glusterfind pre for the session
        ret, _, _ = gfind_pre(self.mnode, self.volname, self.session,
                              self.outfiles[1], debug=True)
        self.assertEqual(ret, 0, ("Failed to perform glusterfind pre"))
        g.log.info("Successfully performed glusterfind pre")

        # Check if the outfile exists
        ret = file_exists(self.mnode, self.outfiles[1])
        self.assertTrue(ret, ("Unexpected: File '%s' does not exist"
                              % self.outfiles[1]))
        g.log.info("Successfully validated existence of outfile '%s'",
                   self.outfiles[1])

        # Check if all the files are listed in the outfile
        for num in range(1, 11):
            pattern = "MODIFY file%s" % num
            ret = check_if_pattern_in_file(self.mnode, pattern,
                                           self.outfiles[1])
            self.assertEqual(ret, 0, ("File 'file%s' not listed in '%s'"
                                      % (num, self.outfiles[1])))
            g.log.info("File 'file%s' listed in '%s'", num, self.outfiles[1])
