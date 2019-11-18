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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.glusterfind_ops import gfind_query
from glustolibs.gluster.glusterfile import (file_exists, remove_file,
                                            check_if_pattern_in_file)


@runs_on([['replicated', 'distributed-replicated', 'dispersed',
           'distributed', 'distributed-dispersed'],
          ['glusterfs']])
class TestGlusterFindQueryCLI(GlusterBaseClass):
    """
    GlusterFindQueryCLI contains tests which verifies the glusterfind query
    command functionality.
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
        self.outfile = '/tmp/test-outfile-%s.txt' % self.volname

    def tearDown(self):
        """
        tearDown for every test and Clean up and unmount the volume
        """

        g.log.info("Removing the outfile created during 'glusterfind pre'")
        ret = remove_file(self.mnode, self.outfile, force=True)
        if not ret:
            raise ExecutionError("Failed to remove the outfile")
        g.log.info("Successfully removed the outfile")

        # stopping the volume
        g.log.info("Starting to Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Cleanup Volume")
        g.log.info("Successful in Cleanup Volume")

        # calling GlusterBaseClass tearDownClass
        GlusterBaseClass.tearDown.im_func(self)

    def test_gfind_query_cli(self):
        """
        Verifying the glusterfind query command functionality with valid
        and invalid values for the required and optional parameters.

        * Create a volume
        * Perform some I/O from the mount point
        * Perform glusterfind query with the following combinations:
            - Valid values for required parameters
            - Invalid values for required parameters
            - Valid values for optional parameters
            - Invalid values for optional parameters

            Where
            Required parameters: volname and sessname
            Optional parameters: debug
        """

        # pylint: disable=too-many-statements
        # Starting IO on the mounts
        mount_obj = self.mounts[0]
        mount_dir = mount_obj.mountpoint
        client = mount_obj.client_system

        g.log.info("Creating Files on %s:%s", client, mount_dir)
        cmd = ("cd %s ; for i in `seq 1 10` ; "
               "do dd if=/dev/urandom of=file$i bs=1M count=1 ; "
               "done" % mount_dir)
        ret, _, _ = g.run(client, cmd)
        self.assertEqual(ret, 0, "Failed to create files on mountpoint")
        g.log.info("Files created successfully on mountpoint")

        # Check if the files exist
        g.log.info("Checking the existence of files created during IO")
        for i in range(1, 11):
            ret = file_exists(client, '%s/file%s' % (mount_dir, i))
            self.assertTrue(ret, "Unexpected: File 'file%s' does not exist"
                            % i)
            g.log.info("Successfully validated existence of 'file%s'", i)

        # Perform glusterfind query for the volume
        g.log.info("Performing glusterfind query using valid values for the "
                   "required parameters")
        ret, _, _ = gfind_query(self.mnode, self.volname, self.outfile,
                                full=True)
        self.assertEqual(ret, 0, "Failed to perform glusterfind query")
        g.log.info("Successfully performed glusterfind query")

        # Check if the outfile exists
        g.log.info("Checking if outfile created during glusterfind query "
                   "command exists")
        ret = file_exists(self.mnode, self.outfile)
        self.assertTrue(ret, "Unexpected: File '%s' does not exist"
                        % self.outfile)
        g.log.info("Successfully validated existence of '%s'", self.outfile)

        # Check if all the files are listed in the outfile
        for i in range(1, 11):
            ret = check_if_pattern_in_file(self.mnode,
                                           'file%s' % i, self.outfile)
            self.assertEqual(ret, 0,
                             ("File 'file%s' not listed in %s"
                              % (i, self.outfile)))
            g.log.info("File 'file%s' listed in %s", i, self.outfile)

        # Perform glusterfind query using the invalid values for required
        # parameters
        not_volume = 'invalid-volume-name-for-glusterfind-query'
        g.log.info("Performing glusterfind query using invalid values for "
                   "required parameters")
        ret, _, _ = gfind_query(self.mnode, not_volume, self.outfile,
                                since='none')
        self.assertNotEqual(ret, 0, "Unexpected: glusterfind query Successful "
                            "even with invalid values for required parameters")
        g.log.info("Successful: glusterfind query failed with invalid values "
                   "for required parameters")

        # Perform glusterfind query using the invalid values for optional
        # parameters
        g.log.info("Performing glusterfind query using invalid values for the "
                   "optional parameters")
        invalid_options = [' --dbug', ' --noencod', ' --type n', ' --fll',
                           ' --tagforfullfind', ' --disablepartial',
                           ' --outprefix none', ' --namespc']
        for opt in invalid_options:
            ret, _, _ = g.run(self.mnode, ("glusterfind query %s %s %s"
                                           % (self.volname, self.outfile,
                                              opt)))
            self.assertNotEqual(ret, 0, "Unexpected: glusterfind query "
                                " Successful for option %s which is invalid"
                                % opt)
        g.log.info("Successful: glusterfind query failed with invalid value "
                   "for optional parameters")
