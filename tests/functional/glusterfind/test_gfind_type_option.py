#  Copyright (C) 2020  Red Hat, Inc. <http://www.redhat.com>
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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfile import (
    file_exists,
    remove_file,
    check_if_pattern_in_file)
from glustolibs.gluster.glusterfind_ops import (
    gfind_create,
    gfind_list,
    gfind_pre,
    gfind_query,
    gfind_delete)


@runs_on([["replicated", "distributed-replicated", "dispersed",
           "distributed", "distributed-dispersed", "arbiter",
           "distributed-arbiter"], ["glusterfs"]])
class TestGlusterfindTypeOption(GlusterBaseClass):
    """
    TestGlusterfindTypeOption contains tests which verifies the
    glusterfind functionality with --full --type options.
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
        self.outfile = "/tmp/test-outfile-%s.txt" % self.volname

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

        # Remove the outfile created during 'glusterfind pre and query'
        ret = remove_file(self.mnode, self.outfile, force=True)
        if not ret:
            raise ExecutionError("Failed to remove the outfile")
        g.log.info("Successfully removed the outfile")

        # Cleanup the volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

    def _check_contents_of_outfile(self, gftype):
        """Check contents of outfile created by query and pre"""
        if gftype == 'f':
            content = self.list_of_files
        elif gftype == 'd':
            content = self.list_of_dirs
        else:
            content = self.list_of_files + self.list_of_dirs

        # Check if outfile is created or not
        ret = file_exists(self.mnode, self.outfile)
        self.assertTrue(ret, "Unexpected: File '%s' does not exist"
                        % self.outfile)

        for value in content:
            ret = check_if_pattern_in_file(self.mnode, value, self.outfile)
            self.assertEqual(ret, 0, "Entry for '%s' not listed in %s"
                             % (value, self.outfile))

    def test_gfind_full_type(self):
        """
        Verifying the glusterfind --full functionality with --type f,
        --type f and --type both

        * Create a volume
        * Create a session on the volume
        * Create various files on mount point
        * Create various directories on point
        * Perform glusterfind pre with --full --type f --regenerate-outfile
        * Check the contents of outfile
        * Perform glusterfind pre with --full --type d --regenerate-outfile
        * Check the contents of outfile
        * Perform glusterfind pre with --full --type both --regenerate-outfile
        * Check the contents of outfile
        * Perform glusterfind query with --full --type f
        * Check the contents of outfile
        * Perform glusterfind query with --full --type d
        * Check the contents of outfile
        * Perform glusterfind query with --full --type both
        * Check the contents of outfile
        """

        # Create some files and directories from the mount point
        cmd = ("cd {}; mkdir dir;mkdir .hiddendir;touch file;touch .hiddenfile"
               ";mknod blockfile b 1 5;mknod charfile b 1 5; mkfifo pipefile;"
               "touch fileforhardlink;touch fileforsoftlink;"
               "ln fileforhardlink hardlinkfile;ln -s fileforsoftlink "
               "softlinkfile".format(self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)

        # Create list of files and dir to be used for checking
        self.list_of_files = ['file', '.hiddenfile', 'blockfile', 'charfile',
                              'pipefile', 'fileforhardlink', 'fileforsoftlink',
                              'hardlinkfile', 'softlinkfile']
        self.list_of_dirs = ['dir', '.hiddendir']

        self.assertEqual(ret, 0, "Failed to create files and dirs")
        g.log.info("Files and Dirs created successfully on mountpoint")

        # Create session for volume
        ret, _, _ = gfind_create(self.mnode, self.volname, self.session)
        self.assertEqual(ret, 0, ("Unexpected: Creation of a session for the"
                                  " volume %s failed" % self.volname))
        g.log.info("Successfully created a session for the volume %s",
                   self.volname)

        # Perform glusterfind list to check if session exists
        _, out, _ = gfind_list(self.mnode, volname=self.volname,
                               sessname=self.session)
        self.assertNotEqual(out, "No sessions found.",
                            "Failed to list the glusterfind session")
        g.log.info("Successfully listed the glusterfind session")

        # Perform glusterfind full pre for the session with --type option
        for gftype in ('f', 'd', 'both'):
            ret, _, _ = gfind_pre(
                self.mnode, self.volname, self.session, self.outfile,
                full=True, gftype=gftype, regenoutfile=True)
            self.assertEqual(ret, 0, "glusterfind pre command successful "
                             "with --type %s" % gftype)

            # Check the contents of the outfile
            self._check_contents_of_outfile(gftype)

        # Perform glusterfind full query with the --type option
        for gftype in ('f', 'd', 'both'):
            ret, _, _ = gfind_query(self.mnode, self.volname, self.outfile,
                                    full=True, gftype=gftype)
            self.assertEqual(ret, 0, "glusterfind query command successful "
                             "with --type %s" % gftype)

            # Check the contents of the outfile
            self._check_contents_of_outfile(gftype)
