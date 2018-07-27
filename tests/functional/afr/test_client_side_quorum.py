#  Copyright (C) 2016-2017  Red Hat, Inc. <http://www.redhat.com>
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

""" Description:
        Test Cases in this module tests the client side quorum.
"""

import tempfile

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (
    set_volume_options, get_subvols,
    setup_volume, cleanup_volume)
from glustolibs.gluster.volume_ops import get_volume_list, get_volume_options
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           get_all_bricks,
                                           are_bricks_offline,
                                           bring_bricks_online)
from glustolibs.io.utils import (validate_io_procs,
                                 is_io_procs_fail_with_rofs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.gluster.mount_ops import mount_volume, umount_volume


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class ClientSideQuorumTests(GlusterBaseClass):
    """
    ClientSideQuorumTests contains tests which verifies the
    client side quorum Test Cases
    """
    @classmethod
    def setUpClass(cls):
        """
        Upload the necessary scripts to run tests.
        """

        # calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_abs_path = "/usr/share/glustolibs/io/scripts/file_dir_ops.py"
        cls.script_upload_path = "/usr/share/glustolibs/io/scripts/" \
                                 "file_dir_ops.py"
        ret = upload_scripts(cls.clients, script_abs_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    def setUp(self):
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        tearDown for every test
        """

        # stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown

        GlusterBaseClass.tearDown.im_func(self)

    def test_client_side_quorum_with_auto_option(self):
        """
        Test Script to verify the Client Side Quorum with auto option

        * set cluster.quorum-type to auto.
        * start I/O from the mount point.
        * kill 2 of the brick process from the each and every replica set
        * perform ops

        """
        # pylint: disable=too-many-branches,too-many-statements
        # set cluster.quorum-type to auto
        options = {"cluster.quorum-type": "auto"}
        g.log.info("setting cluster.quorum-type to auto on "
                   "volume %s", self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for"
                              "volume %s" % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # write files on all mounts
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name file %s" % (self.script_upload_path,
                                                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s:", num_subvols)

        # bring bricks offline( 2 bricks ) for all the subvolumes
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            g.log.info("sub-volume %s brick list : %s", i, subvol_brick_list)
            # For volume type: 1 * 2, bring 1 brick offline
            if len(subvol_brick_list) == 2:
                bricks_to_bring_offline = subvol_brick_list[0:1]
            else:
                bricks_to_bring_offline = subvol_brick_list[0:2]
            g.log.info("Going to bring down the brick process "
                       "for %s", bricks_to_bring_offline)
            ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
            self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                                  "check the log file for more details."))
            g.log.info("Brought down the brick process "
                       "for %s successfully", bricks_to_bring_offline)

        # create 2 files named newfile0.txt and newfile1.txt
        g.log.info("Start creating 2 files on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 2 --base-file-name newfile %s" %
                   (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with read-only filesystem")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on read-only filesystem"))
        g.log.info("EXPECTED: Read-only file system in IO while creating file")

        # create directory user1
        g.log.info("Start creating directory on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_deep_dir "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with read-only filesystem")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on read-only filesystem"))
        g.log.info("EXPECTED: Read-only file system in IO while"
                   " creating directory")

        # create h/w link to file
        g.log.info("Start creating hard link for file0.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = "ln %s/file0.txt %s/file0.txt_hwlink" \
                  % (mount_obj.mountpoint, mount_obj.mountpoint)
            ret, _, err = g.run(mount_obj.client_system, cmd)
            self.assertTrue(ret, ("Unexpected error and creating hard link"
                                  " successful on read-only filesystem"))
            self.assertIn("Read-only file system",
                          err, "Read-only filesystem not found in "
                               "IO while truncating file")
            g.log.info("EXPECTED: Read-only file system in IO")

        # create s/w link
        g.log.info("Start creating soft link for file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = "ln -s %s/file1.txt %s/file1.txt_swlink" %\
                  (mount_obj.mountpoint, mount_obj.mountpoint)
            ret, _, err = g.run(mount_obj.client_system, cmd)
            self.assertTrue(ret, ("Unexpected error and creating soft link"
                                  " successful on read-only filesystem"))
            self.assertIn("Read-only file system",
                          err, "Read-only filesystem not found in "
                               "IO while truncating file")
            g.log.info("EXPECTED: Read-only file system in IO")

        # append to file
        g.log.info("Appending to file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = "cat %s/file0.txt >> %s/file1.txt" %\
                  (mount_obj.mountpoint, mount_obj.mountpoint)
            ret, _, err = g.run(mount_obj.client_system, cmd)
            self.assertTrue(ret, ("Unexpected error and append successful"
                                  " on read-only filesystem"))
            self.assertIn("Read-only file system",
                          err, "Read-only filesystem not found in "
                               "IO while truncating file")
            g.log.info("EXPECTED: Read-only file system in IO")

        # modify the file
        g.log.info("Modifying file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = "echo 'Modify Contents' > %s/file1.txt"\
                  % (mount_obj.mountpoint)
            ret, _, err = g.run(mount_obj.client_system, cmd)
            self.assertTrue(ret, ("Unexpected error and modifying successful"
                                  " on read-only filesystem"))
            self.assertIn("Read-only file system",
                          err, "Read-only filesystem not found in "
                               "IO while truncating file")
            g.log.info("EXPECTED: Read-only file system in IO")

        # truncate the file
        g.log.info("Truncating file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = "truncate -s 0 %s/file1.txt" % (mount_obj.mountpoint)
            ret, _, err = g.run(mount_obj.client_system, cmd)
            self.assertTrue(ret, ("Unexpected error and truncating file"
                                  " successful on read-only filesystem"))
            self.assertIn("Read-only file system",
                          err, "Read-only filesystem not found in "
                               "IO while truncating file")
            g.log.info("EXPECTED: Read-only file system in IO")

        # read the file
        g.log.info("Starting reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # stat on file
        g.log.info("stat on file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = "stat %s/file1.txt" % (mount_obj.mountpoint)
            ret, _, err = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, ("Unexpected error and stat on file fails"
                                   " on read-only filesystem"))
            g.log.info("stat on file is successful on read-only filesystem")

        # stat on dir
        g.log.info("stat on directory on all mounts")
        for mount_obj in self.mounts:
            cmd = ("python %s stat %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            ret, _, err = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, ("Unexpected error and stat on directory"
                                   " fails on read-only filesystem"))
            g.log.info("stat on dir is successful on read-only filesystem")

        # ls on mount point
        g.log.info("ls on mount point on all mounts")
        for mount_obj in self.mounts:
            cmd = ("python %s ls %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            ret, _, err = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, ("Unexpected error and listing file fails"
                                   " on read-only filesystem"))
            g.log.info("listing files is successful on read-only filesystem")

    def test_client_side_quorum_with_fixed_validate_max_bricks(self):
        """
        Test Script with Client Side Quorum with fixed should validate
        maximum number of bricks to accept

        * set cluster quorum to fixed
        * set cluster.quorum-count to higher number which is greater than
          number of replicas in a sub-voulme
        * Above step should fail

        """

        # set cluster.quorum-type to fixed
        options = {"cluster.quorum-type": "fixed"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s is %s", self.volname,
                   num_subvols)

        # get the number of bricks in replica set
        num_bricks_in_subvol = len(subvols_dict['volume_subvols'][0])
        g.log.info("Number of bricks in each replica set : %s",
                   num_bricks_in_subvol)

        # set cluster.quorum-count to higher value than the number of bricks in
        # repliac set
        start_range = num_bricks_in_subvol + 1
        end_range = num_bricks_in_subvol + 30
        for i in range(start_range, end_range):
            options = {"cluster.quorum-count": "%s" % i}
            g.log.info("setting %s for the volume %s", options, self.volname)
            ret = set_volume_options(self.mnode, self.volname, options)
            self.assertFalse(ret, ("Able to set %s for volume %s, quorum-count"
                                   " should not be greater than number of"
                                   " bricks in replica set"
                                   % (options, self.volname)))
        g.log.info("Expected: Unable to set %s for volume %s, "
                   "quorum-count should be less than number of bricks "
                   "in replica set", options, self.volname)

    def test_client_side_quorum_with_auto_option_overwrite_fixed(self):
        """
        Test Script to verify the Client Side Quorum with auto option

        * check the default value of cluster.quorum-type
        * try to set any junk value to cluster.quorum-type
          other than {none,auto,fixed}
        * check the default value of cluster.quorum-count
        * set cluster.quorum-type to fixed and cluster.quorum-count to 1
        * start I/O from the mount point
        * kill 2 of the brick process from the each replica set.
        * set cluster.quorum-type to auto

        """
        # pylint: disable=too-many-locals,too-many-lines,too-many-statements
        # check the default value of cluster.quorum-type
        option = "cluster.quorum-type"
        g.log.info("Getting %s for the volume %s", option, self.volname)
        option_dict = get_volume_options(self.mnode, self.volname, option)
        self.assertIsNotNone(option_dict, ("Failed to get %s volume option"
                                           " for volume %s"
                                           % (option, self.volname)))
        self.assertEqual(option_dict['cluster.quorum-type'], 'auto',
                         ("Default value for %s is not auto"
                          " for volume %s" % (option, self.volname)))
        g.log.info("Succesfully verified default value of %s for volume %s",
                   option, self.volname)

        # set the junk value to cluster.quorum-type
        junk_values = ["123", "abcd", "fixxed", "Aauto"]
        for each_junk_value in junk_values:
            options = {"cluster.quorum-type": "%s" % each_junk_value}
            g.log.info("setting %s for the volume "
                       "%s", options, self.volname)
            ret = set_volume_options(self.mnode, self.volname, options)
            self.assertFalse(ret, ("Able to set junk value %s for "
                                   "volume %s" % (options, self.volname)))
            g.log.info("Expected: Unable to set junk value %s "
                       "for volume %s", options, self.volname)

        # check the default value of cluster.quorum-count
        option = "cluster.quorum-count"
        g.log.info("Getting %s for the volume %s", option, self.volname)
        option_dict = get_volume_options(self.mnode, self.volname, option)
        self.assertIsNotNone(option_dict, ("Failed to get %s volume option"
                                           " for volume %s"
                                           % (option, self.volname)))
        self.assertEqual(option_dict['cluster.quorum-count'], '(null)',
                         ("Default value for %s is not null"
                          " for volume %s" % (option, self.volname)))
        g.log.info("Successful in getting %s for the volume %s",
                   option, self.volname)

        # set cluster.quorum-type to fixed and cluster.quorum-count to 1
        options = {"cluster.quorum-type": "fixed",
                   "cluster.quorum-count": "1"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # create files
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # get the subvolumes
        g.log.info("starting to get subvolumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s is %s",
                   self.volname, num_subvols)

        # bring bricks offline( 2 bricks ) for all the subvolumes
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            g.log.info("sub-volume %s brick list : %s",
                       i, subvol_brick_list)
            bricks_to_bring_offline = subvol_brick_list[0:2]
            g.log.info("Going to bring down the brick process "
                       "for %s", bricks_to_bring_offline)
            ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
            self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                                  "check the log file for more details."))
            g.log.info("Brought down the brick process "
                       "for %s successfully", bricks_to_bring_offline)

        # create files
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name second_file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # set cluster.quorum-type to auto
        options = {"cluster.quorum-type": "auto"}
        g.log.info("setting %s for volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for "
                              "volume %s" % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # create files
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_deep_dirs_with_files --dir-depth 2 "
                   "--dir-length 2 --max-num-of-dirs 3 --num-of-files 7 %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # check IO failed with Read Only File System error
        g.log.info("Wait for IO to complete and validate IO.....")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected error and IO successful "
                              "on Read-only file system. Please check the "
                              "logs for more details"))
        g.log.info("EXPECTED : Read-only file system in IO while "
                   "creating files")


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs', 'cifs']])
class ClientSideQuorumCross2Tests(GlusterBaseClass):
    """
    ClientSideQuorumTests contains tests which verifies the
    client side quorum Test Cases
    """

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [script_local_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        cls.counter = 1
        # int: Value of counter is used for dirname-start-num argument for
        # file_dir_ops.py create_deep_dirs_with_files.

        # The --dir-length argument value for file_dir_ops.py
        # create_deep_dirs_with_files is set to 10 (refer to the cmd in setUp
        # method). This means every mount will create
        # 10 top level dirs. For every mountpoint/testcase to create new set of
        # dirs, we are incrementing the counter by --dir-length value i.e 10 in
        # this test suite.

        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

        # Override Volumes
        if cls.volume_type == "distributed-replicated":
            # Define 2x2 distributed-replicated volume
            cls.volume['voltype'] = {
                'type': 'distributed-replicated',
                'replica_count': 2,
                'dist_count': 2,
                'transport': 'tcp'}

        if cls.volume_type == "replicated":
            # Define x2 replicated volume
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 2,
                'transport': 'tcp'}

    def setUp(self):
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        tearDown for every test
        """

        # stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_client_side_quorum_with_auto_option_cross2(self):
        """
        Test Script to verify the Client Side Quorum with auto option

        * set cluster.quorum-type to auto.
        * start I/O from the mount point.
        * kill 2-nd brick process from the each and every replica set
        * perform ops

        """
        # pylint: disable=too-many-branches,too-many-statements
        # set cluster.quorum-type to auto
        options = {"cluster.quorum-type": "auto"}
        g.log.info("setting cluster.quorum-type to auto on "
                   "volume %s", self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for"
                              "volume %s" % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s", options, self.volname)

        # Start IO on mounts
        g.log.info("Starting IO on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name file %s" % (self.script_upload_path,
                                                       mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s:", num_subvols)

        # bring 2-nd bricks offline for all the subvolumes
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            g.log.info("sub-volume %s brick list : %s", i, subvol_brick_list)
            bricks_to_bring_offline = subvol_brick_list[1]
            g.log.info("Going to bring down the brick process "
                       "for %s", bricks_to_bring_offline)
            ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
            self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                                  "check the log file for more details."))
            g.log.info("Brought down the brick process "
                       "for %s successfully", bricks_to_bring_offline)

        # create new file named newfile0.txt
        g.log.info("Start creating new file on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 1 --base-file-name newfile %s" %
                   (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # create directory user1
        g.log.info("Start creating directory on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_deep_dir %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # create h/w link to file
        g.log.info("Start creating hard link for file0.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = ("ln %s/file0.txt %s/file0.txt_hwlink"
                   % (mount_obj.mountpoint, mount_obj.mountpoint))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, 'Failed to create hard link '
                                  'for file0.txt on %s'
                             % mount_obj.mountpoint)
            g.log.info("Hard link for file0.txt on %s is created successfully",
                       mount_obj.mountpoint)

        # create s/w link
        g.log.info("Start creating soft link for file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = ("ln -s %s/file1.txt %s/file1.txt_swlink"
                   % (mount_obj.mountpoint, mount_obj.mountpoint))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, 'Failed to create soft link '
                                  'for file1.txt on %s'
                             % mount_obj.mountpoint)
            g.log.info("Soft link for file1.txt on %s is created successfully",
                       mount_obj.mountpoint)

        # append to file
        g.log.info("Appending to file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = ("cat %s/file0.txt >> %s/file1.txt"
                   % (mount_obj.mountpoint, mount_obj.mountpoint))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, 'Failed to append file1.txt on %s'
                             % mount_obj.mountpoint)
            g.log.info("Appending for file1.txt on %s is successful",
                       mount_obj.mountpoint)

        # modify the file
        g.log.info("Modifying file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = ("echo 'Modify Contents' > %s/file1.txt"
                   % mount_obj.mountpoint)
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, 'Failed to modify file1.txt on %s'
                             % mount_obj.mountpoint)
            g.log.info("Modifying for file1.txt on %s is successful",
                       mount_obj.mountpoint)

        # truncate the file
        g.log.info("Truncating file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = "truncate -s 0 %s/file1.txt" % mount_obj.mountpoint
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, 'Failed to truncate file1.txt on %s'
                             % mount_obj.mountpoint)
            g.log.info("Truncating for file1.txt on %s is successful",
                       mount_obj.mountpoint)

        # read the file
        g.log.info("Starting reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # stat on file
        g.log.info("stat on file1.txt on all mounts")
        for mount_obj in self.mounts:
            cmd = "stat %s/file1.txt" % mount_obj.mountpoint
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, 'Failed to stat file1.txt on %s'
                             % mount_obj.mountpoint)
            g.log.info("Stat for file1.txt on %s is successful",
                       mount_obj.mountpoint)

        # stat on dir
        g.log.info("stat on directory on all mounts")
        for mount_obj in self.mounts:
            cmd = ("python %s stat %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, 'Failed to stat directory on %s'
                             % mount_obj.mountpoint)
            g.log.info("Stat for directory on %s is successful",
                       mount_obj.mountpoint)

        # ls on mount point
        g.log.info("ls on mount point on all mounts")
        for mount_obj in self.mounts:
            cmd = ("python %s ls %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            ret, _, _ = g.run(mount_obj.client_system, cmd)
            self.assertFalse(ret, 'Failed to ls on %s'
                             % mount_obj.mountpoint)
            g.log.info("ls for %s is successful", mount_obj.mountpoint)

    def test_client_side_quorum_with_fixed_for_cross2(self):
        """
        Test Script to verify the Client Side Quorum with fixed
        for cross 2 volume

        * Disable self heal daemom
        * set cluster.quorum-type to fixed.
        * start I/O( write and read )from the mount point - must succeed
        * Bring down brick1
        * start I/0 ( write and read ) - must succeed
        * set the cluster.quorum-count to 1
        * start I/0 ( write and read ) - must succeed
        * set the cluster.quorum-count to 2
        * start I/0 ( write and read ) - read must pass, write will fail
        * bring back the brick1 online
        * start I/0 ( write and read ) - must succeed
        * Bring down brick2
        * start I/0 ( write and read ) - read must pass, write will fail
        * set the cluster.quorum-count to 1
        * start I/0 ( write and read ) - must succeed
        * cluster.quorum-count back to 2 and cluster.quorum-type to auto
        * start I/0 ( write and read ) - must succeed
        * Bring back brick2 online
        * Bring down brick1
        * start I/0 ( write and read ) - read must pass, write will fail
        * set the quorum-type to none
        * start I/0 ( write and read ) - must succeed

        """
        # pylint: disable=too-many-branches,too-many-statements
        # Disable self heal daemon
        options = {"cluster.self-heal-daemon": "off"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # set cluster.quorum-type to fixed
        options = {"cluster.quorum-type": "fixed"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/O( write ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s:", num_subvols)

        # Bring down brick1 for all the subvolumes
        subvolumes_first_brick_list = []
        subvolumes_second_brick_list = []
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            g.log.info("sub-volume %s brick list : %s", i, subvol_brick_list)
            subvolumes_first_brick_list.append(subvol_brick_list[0])
            subvolumes_second_brick_list.append(subvol_brick_list[1])

        g.log.info("Going to bring down the brick process "
                   "for %s", subvolumes_first_brick_list)
        ret = bring_bricks_offline(self.volname, subvolumes_first_brick_list)
        self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                              "check the log file for more details."))
        g.log.info("Brought down the brick process "
                   "for %s successfully", subvolumes_first_brick_list)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name second_file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set the cluster.quorum-count to 1
        options = {"cluster.quorum-count": "1"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name third_file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set the cluster.quorum-count to 2
        options = {"cluster.quorum-count": "2"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - read must pass, write will fail
        g.log.info("Starting IO on all mounts......")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name fourth_file %s" %
                   (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with Read Only File System")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected Error and IO successful"
                              " on Read-Only File System"))
        g.log.info("EXPECTED Read-only file system in IO while creating file")

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # bring back the brick1 online for all subvolumes
        g.log.info("bringing up the bricks : %s online",
                   subvolumes_first_brick_list)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  subvolumes_first_brick_list)
        self.assertTrue(ret, ("Failed to brought the bricks %s online"
                              % subvolumes_first_brick_list))
        g.log.info("Successfully brought the bricks %s online",
                   subvolumes_first_brick_list)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name fifth_file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # Bring down brick2 for all the subvolumes
        g.log.info("Going to bring down the brick process "
                   "for %s", subvolumes_second_brick_list)
        ret = bring_bricks_offline(self.volname, subvolumes_second_brick_list)
        self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                              "check the log file for more details."))
        g.log.info("Brought down the brick process "
                   "for %s successfully", subvolumes_second_brick_list)

        # start I/0 ( write and read ) - read must pass, write will fail
        g.log.info("Start creating files on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name sixth_file %s" %
                   (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with Read Only File System")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected Error and IO successful"
                              " on Read-Only File System"))
        g.log.info("EXPECTED Read-only file system in IO while creating file")

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set the cluster.quorum-count to 1
        options = {"cluster.quorum-count": "1"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name seventh_file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set cluster.quorum-type to auto and cluster.quorum-count back to 2
        options = {"cluster.quorum-type": "auto",
                   "cluster.quorum-count": "2"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name eigth_file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # Bring back brick2 online for all the subvolumes
        g.log.info("bringing up the bricks : %s online",
                   subvolumes_second_brick_list)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  subvolumes_second_brick_list)
        self.assertTrue(ret, ("Failed to brought the brick %s online"
                              % subvolumes_second_brick_list))
        g.log.info("Successfully brought the brick %s online",
                   subvolumes_second_brick_list)

        # Bring down brick1 again for all the subvolumes
        g.log.info("Going to bring down the brick process "
                   "for %s", subvolumes_first_brick_list)
        ret = bring_bricks_offline(self.volname, subvolumes_first_brick_list)
        self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                              "check the log file for more details."))
        g.log.info("Brought down the brick process "
                   "for %s successfully", subvolumes_first_brick_list)

        # start I/0 ( write and read ) - read must pass, write will fail
        g.log.info("Start creating files on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name ninth_file %s" %
                   (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with Read Only File System")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected Error and IO successful"
                              " on Read-Only File System"))
        g.log.info("EXPECTED Read-only file system in IO while creating file")

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set the quorum-type to none
        options = {"cluster.quorum-type": "none"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name tenth_file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )


@runs_on([['distributed-replicated'],
          ['glusterfs']])
class ClientSideQuorumTestsMultipleVols(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        script_local_path = ("/usr/share/glustolibs/io/scripts/"
                             "file_dir_ops.py")
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, [script_local_path])
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients %s"
                                 % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

        cls.counter = 1
        # int: Value of counter is used for dirname-start-num argument for
        # file_dir_ops.py create_deep_dirs_with_files.

        # The --dir-length argument value for file_dir_ops.py
        # create_deep_dirs_with_files is set to 10 (refer to the cmd in setUp
        # method). This means every mount will create
        # 10 top level dirs. For every mountpoint/testcase to create new set of
        # dirs, we are incrementing the counter by --dir-length value i.e 10 in
        # this test suite.

        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

        # Setup Volumes
        if cls.volume_type == "distributed-replicated":
            cls.volume_configs = []

            # Define two 2x2 distributed-replicated volumes
            for i in range(1, 3):
                cls.volume['voltype'] = {
                    'type': 'distributed-replicated',
                    'replica_count': 2,
                    'dist_count': 2,
                    'transport': 'tcp',
                }
                cls.volume_configs.append(
                    {'name': 'testvol_%s_%d'
                             % (cls.volume['voltype']['type'], i),
                     'servers': cls.servers,
                     'voltype': cls.volume['voltype']})

            # Define two 2x3 distributed-replicated volumes
            for i in range(1, 3):
                cls.volume['voltype'] = {
                    'type': 'distributed-replicated',
                    'replica_count': 3,
                    'dist_count': 2,
                    'transport': 'tcp',
                }
                cls.volume_configs.append(
                    {'name': 'testvol_%s_%d'
                             % (cls.volume['voltype']['type'], i+2),
                     'servers': cls.servers,
                     'voltype': cls.volume['voltype']})

            # Define distributed volume
            cls.volume['voltype'] = {
                'type': 'distributed',
                'dist_count': 3,
                'transport': 'tcp',
            }
            cls.volume_configs.append(
                {'name': 'testvol_%s'
                         % cls.volume['voltype']['type'],
                 'servers': cls.servers,
                 'voltype': cls.volume['voltype']})

            # Create and mount volumes
            cls.mount_points = []
            cls.mount_points_and_volnames = {}
            cls.client = cls.clients[0]
            for volume_config in cls.volume_configs:
                # Setup volume
                ret = setup_volume(mnode=cls.mnode,
                                   all_servers_info=cls.all_servers_info,
                                   volume_config=volume_config,
                                   force=False)
                if not ret:
                    raise ExecutionError("Failed to setup Volume"
                                         " %s" % volume_config['name'])
                g.log.info("Successful in setting volume %s",
                           volume_config['name'])

                # Mount volume
                mount_point = tempfile.mkdtemp()
                cls.mount_points.append(mount_point)
                cls.mount_points_and_volnames[volume_config['name']] = \
                    mount_point
                ret, _, _ = mount_volume(volume_config['name'],
                                         cls.mount_type,
                                         mount_point,
                                         cls.mnode,
                                         cls.client)
                if ret:
                    raise ExecutionError(
                        "Failed to do gluster mount on volume %s "
                        % cls.volname)
                g.log.info("Successfully mounted %s on client %s",
                           cls.volname, cls.client)

    def setUp(self):
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        self.all_mounts_procs = []
        self.io_validation_complete = False

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Cleanup and umount volume
        """
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

            # List all files and dirs created
            g.log.info("List all files and directories:")
            ret = list_all_files_and_dirs_mounts(self.mounts)
            if not ret:
                raise ExecutionError("Failed to list all files and dirs")
            g.log.info("Listing all files and directories is successful")

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the volume and umount volume from client
        """
        # stopping all volumes
        g.log.info("Starting to Cleanup all Volumes")
        volume_list = get_volume_list(cls.mnode)
        for volume in volume_list:
            ret = cleanup_volume(cls.mnode, volume)
            if not ret:
                raise ExecutionError("Failed to cleanup Volume %s" % volume)
            g.log.info("Volume: %s cleanup is done", volume)
        g.log.info("Successfully Cleanedup all Volumes")

        # umount all volumes
        for mount_point in cls.mount_points:
            ret, _, _ = umount_volume(cls.client, mount_point)
            if ret:
                raise ExecutionError(
                    "Failed to umount on volume %s "
                    % cls.volname)
            g.log.info("Successfully umounted %s on client %s", cls.volname,
                       cls.client)

        # calling GlusterBaseClass tearDownClass
        GlusterBaseClass.tearDownClass.im_func(cls)

    def test_client_side_quorum_auto_local_to_volume_not_cluster(self):
        """
        - create four volume as below
            vol1->2x2
            vol2->2x2
            vol3->2x3
            vol4->2x3
            vol5->a pure distribute volume
        - do IO to all vols
        - set client side quorum to auto for vol1 and vol3
        - get the client side quorum value for all vols and check for result
        - bring down b0 on vol1 and b0 and b1 on vol3
        - try to create files on all vols and check for result
        """
        # pylint: disable=too-many-locals,too-many-statements
        # Creating files for all volumes
        for mount_point in self.mount_points:
            self.all_mounts_procs = []
            for mount_obj in self.mounts:
                g.log.info("Generating data for %s:%s",
                           mount_obj.client_system, mount_point)
                # Create files
                g.log.info('Creating files...')
                command = ("python %s create_files -f 50 "
                           "--fixed-file-size 1k %s"
                           % (self.script_upload_path, mount_point))

                proc = g.run_async(mount_obj.client_system, command,
                                   user=mount_obj.user)
                self.all_mounts_procs.append(proc)
            self.io_validation_complete = False

            # Validate IO
            self.assertTrue(
                validate_io_procs(self.all_mounts_procs, self.mounts),
                "IO failed on some of the clients"
            )
            self.io_validation_complete = True

        volumes_to_change_options = ['1', '3']
        # set cluster.quorum-type to auto
        for vol_number in volumes_to_change_options:
            vol_name = ('testvol_distributed-replicated_%s'
                        % vol_number)
            options = {"cluster.quorum-type": "auto"}
            g.log.info("setting cluster.quorum-type to auto on "
                       "volume testvol_distributed-replicated_%s", vol_number)
            ret = set_volume_options(self.mnode, vol_name, options)
            self.assertTrue(ret, ("Unable to set volume option %s for "
                                  "volume %s" % (options, vol_name)))
            g.log.info("Successfully set %s for volume %s", options, vol_name)

        # check is options are set correctly
        volume_list = get_volume_list(self.mnode)
        for volume in volume_list:
            g.log.info('Checking for cluster.quorum-type option for %s',
                       volume)
            volume_options_dict = get_volume_options(self.mnode,
                                                     volume,
                                                     'cluster.quorum-type')
            if (volume == 'testvol_distributed-replicated_1' or
                    volume == 'testvol_distributed-replicated_3'):
                self.assertEqual(volume_options_dict['cluster.quorum-type'],
                                 'auto',
                                 'Option cluster.quorum-type '
                                 'is not AUTO for %s'
                                 % volume)
                g.log.info('Option cluster.quorum-type is AUTO for %s', volume)
            else:
                self.assertEqual(volume_options_dict['cluster.quorum-type'],
                                 'none',
                                 'Option cluster.quorum-type '
                                 'is not NONE for %s'
                                 % volume)
                g.log.info('Option cluster.quorum-type is NONE for %s', volume)

        # Get first brick server and brick path
        # and get first file from filelist then delete it from volume
        vols_file_list = {}
        for volume in volume_list:
            brick_list = get_all_bricks(self.mnode, volume)
            brick_server, brick_path = brick_list[0].split(':')
            ret, file_list, _ = g.run(brick_server, 'ls %s' % brick_path)
            self.assertFalse(ret, 'Failed to ls files on %s' % brick_server)
            file_from_vol = file_list.splitlines()[0]
            ret, _, _ = g.run(brick_server, 'rm -rf %s/%s'
                              % (brick_path, file_from_vol))
            self.assertFalse(ret, 'Failed to rm file on %s' % brick_server)
            vols_file_list[volume] = file_from_vol

        # bring bricks offline
        # bring first brick for testvol_distributed-replicated_1
        volname = 'testvol_distributed-replicated_1'
        brick_list = get_all_bricks(self.mnode, volname)
        bricks_to_bring_offline = brick_list[0:1]
        g.log.info('Bringing bricks %s offline...', bricks_to_bring_offline)
        ret = bring_bricks_offline(volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # bring first two bricks for testvol_distributed-replicated_3
        volname = 'testvol_distributed-replicated_3'
        brick_list = get_all_bricks(self.mnode, volname)
        bricks_to_bring_offline = brick_list[0:2]
        g.log.info('Bringing bricks %s offline...', bricks_to_bring_offline)
        ret = bring_bricks_offline(volname, bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s offline' %
                        bricks_to_bring_offline)

        ret = are_bricks_offline(self.mnode, volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not offline'
                        % bricks_to_bring_offline)
        g.log.info('Bringing bricks %s offline is successful',
                   bricks_to_bring_offline)

        # merge two dicts (volname: file_to_delete) and (volname: mountpoint)
        temp_dict = [vols_file_list, self.mount_points_and_volnames]
        file_to_delete_to_mountpoint_dict = {}
        for k in vols_file_list.iterkeys():
            file_to_delete_to_mountpoint_dict[k] = (
                tuple(file_to_delete_to_mountpoint_dict[k]
                      for file_to_delete_to_mountpoint_dict in
                      temp_dict))

        # create files on all volumes and check for result
        for volname, file_and_mountpoint in \
                file_to_delete_to_mountpoint_dict.iteritems():
            filename, mountpoint = file_and_mountpoint

            # check for ROFS error for read-only file system for
            # testvol_distributed-replicated_1 and
            # testvol_distributed-replicated_3
            if (volname == 'testvol_distributed-replicated_1' or
                    volname == 'testvol_distributed-replicated_3'):
                # create new file taken from vols_file_list
                g.log.info("Start creating new file on all mounts...")
                all_mounts_procs = []
                cmd = ("touch %s/%s" % (mountpoint, filename))

                proc = g.run_async(self.client, cmd)
                all_mounts_procs.append(proc)

                # Validate IO
                g.log.info("Validating if IO failed with read-only filesystem")
                ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                                    self.mounts)
                self.assertTrue(ret, ("Unexpected error and IO successful"
                                      " on read-only filesystem"))
                g.log.info("EXPECTED: "
                           "Read-only file system in IO while creating file")

            # check for no errors for all the rest volumes
            else:
                # create new file taken from vols_file_list
                g.log.info("Start creating new file on all mounts...")
                all_mounts_procs = []
                cmd = ("touch %s/%s" % (mountpoint, filename))

                proc = g.run_async(self.client, cmd)
                all_mounts_procs.append(proc)

                # Validate IO
                self.assertTrue(
                    validate_io_procs(all_mounts_procs, self.mounts),
                    "IO failed on some of the clients"
                )


@runs_on([['replicated', 'distributed-replicated'], ['glusterfs']])
class ClientSideQuorumTestsWithSingleVolumeCross3(GlusterBaseClass):
    """
    ClientSideQuorumTestsWithSingleVolumeCross3 contains tests which
    verifies the client side quorum Test Cases with cross 3 volume.
    """
    @classmethod
    def setUpClass(cls):
        """
        Upload the necessary scripts to run tests.
        """

        # calling GlusterBaseClass setUpClass
        GlusterBaseClass.setUpClass.im_func(cls)

        # Overriding the volume type to specifically test the volume type
        if cls.volume_type == "replicated":
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 3,
                'dist_count': 1,
                'transport': 'tcp'
            }

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        script_abs_path = "/usr/share/glustolibs/io/scripts/file_dir_ops.py"
        cls.script_upload_path = "/usr/share/glustolibs/io/scripts/" \
                                 "file_dir_ops.py"
        ret = upload_scripts(cls.clients, script_abs_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to clients")

    def setUp(self):
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume %s", self.volname)
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        tearDown for every test
        """

        # stopping the volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume "
                                 "and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        GlusterBaseClass.tearDown.im_func(self)

    def test_client_side_quorum_with_fixed_for_cross3(self):
        """
        Test Script to verify the Client Side Quorum with fixed
        for cross 3 volume

        * Disable self heal daemom
        * set cluster.quorum-type to fixed.
        * start I/O( write and read )from the mount point - must succeed
        * Bring down brick1
        * start I/0 ( write and read ) - must succeed
        * Bring down brick2
        * start I/0 ( write and read ) - must succeed
        * set the cluster.quorum-count to 1
        * start I/0 ( write and read ) - must succeed
        * set the cluster.quorum-count to 2
        * start I/0 ( write and read ) - read must pass, write will fail
        * bring back the brick1 online
        * start I/0 ( write and read ) - must succeed
        * Bring back brick2 online
        * start I/0 ( write and read ) - must succeed
        * set cluster.quorum-type to auto
        * start I/0 ( write and read ) - must succeed
        * Bring down brick1 and brick2
        * start I/0 ( write and read ) - read must pass, write will fail
        * set the cluster.quorum-count to 1
        * start I/0 ( write and read ) - read must pass, write will fail
        * set the cluster.quorum-count to 3
        * start I/0 ( write and read ) - read must pass, write will fail
        * set the quorum-type to none
        * start I/0 ( write and read ) - must succeed

        """
        # pylint: disable=too-many-locals,too-many-statements,too-many-branches
        # Disable self heal daemon
        options = {"cluster.self-heal-daemon": "off"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # set cluster.quorum-type to fixed
        options = {"cluster.quorum-type": "fixed"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/O( write ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name file %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # get the subvolumes
        g.log.info("Starting to get sub-volumes for volume %s", self.volname)
        subvols_dict = get_subvols(self.mnode, self.volname)
        num_subvols = len(subvols_dict['volume_subvols'])
        g.log.info("Number of subvolumes in volume %s:", num_subvols)

        # bring down brick1 for all the subvolumes
        offline_brick1_from_replicasets = []
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            g.log.info("sub-volume %s brick list : %s",
                       i, subvol_brick_list)
            brick_to_bring_offline1 = subvol_brick_list[0]
            g.log.info("Going to bring down the brick process "
                       "for %s", brick_to_bring_offline1)
            ret = bring_bricks_offline(self.volname, brick_to_bring_offline1)
            self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                                  "check the log file for more details."))
            g.log.info("Brought down the brick process "
                       "for %s successfully", brick_to_bring_offline1)
            offline_brick1_from_replicasets.append(brick_to_bring_offline1)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name testfile %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # bring down brick2 for all the subvolumes
        offline_brick2_from_replicasets = []
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            g.log.info("sub-volume %s brick list : %s",
                       i, subvol_brick_list)
            brick_to_bring_offline2 = subvol_brick_list[1]
            g.log.info("Going to bring down the brick process "
                       "for %s", brick_to_bring_offline2)
            ret = bring_bricks_offline(self.volname, brick_to_bring_offline2)
            self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                                  "check the log file for more details."))
            g.log.info("Brought down the brick process "
                       "for %s successfully", brick_to_bring_offline2)
            offline_brick2_from_replicasets.append(brick_to_bring_offline2)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name newfile %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set the cluster.quorum-count to 1
        options = {"cluster.quorum-count": "1"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name filename %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set the cluster.quorum-count to 2
        options = {"cluster.quorum-count": "2"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - read must pass, write will fail
        g.log.info("Starting IO on all mounts......")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name testfilename %s" %
                   (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with Read Only File System")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected Error and IO successful"
                              " on Read-Only File System"))
        g.log.info("EXPECTED Read-only file system in IO while creating file")

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # bring back the brick1 online for all subvolumes
        g.log.info("bringing up the brick : %s online",
                   offline_brick1_from_replicasets)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  offline_brick1_from_replicasets)
        self.assertTrue(ret, ("Failed to brought the brick %s online"
                              % offline_brick1_from_replicasets))
        g.log.info("Successfully brought the brick %s online",
                   offline_brick1_from_replicasets)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name newfilename %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # Bring back brick2 online
        g.log.info("bringing up the brick : %s online",
                   offline_brick2_from_replicasets)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  offline_brick2_from_replicasets)
        self.assertTrue(ret, ("Failed to brought the brick %s online"
                              % offline_brick2_from_replicasets))
        g.log.info("Successfully brought the brick %s online",
                   offline_brick2_from_replicasets)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name textfile %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set cluster.quorum-type to auto
        options = {"cluster.quorum-type": "auto"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name newtextfile %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # bring down brick1 and brick2 for all the subvolumes
        for i in range(0, num_subvols):
            subvol_brick_list = subvols_dict['volume_subvols'][i]
            g.log.info("sub-volume %s brick list : %s",
                       i, subvol_brick_list)
            bricks_to_bring_offline = subvol_brick_list[0:2]
            g.log.info("Going to bring down the brick process "
                       "for %s", bricks_to_bring_offline)
            ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
            self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                                  "check the log file for more details."))
            g.log.info("Brought down the brick process "
                       "for %s successfully", bricks_to_bring_offline)

        # start I/0 ( write and read ) - read must pass, write will fail
        g.log.info("Start creating files on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name newtestfile %s" %
                   (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with Read-only file system")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on Read-only file system"))
        g.log.info("EXPECTED: Read-only file system in IO while creating file")

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set the cluster.quorum-count to 1
        options = {"cluster.quorum-count": "1"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - read must pass, write will fail
        g.log.info("Start creating files on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name newtestfilename %s" %
                   (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with Read-only file system")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on Read-only file system"))
        g.log.info("EXPECTED: Read-only file system in IO while creating file")

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set the cluster.quorum-count to 3
        options = {"cluster.quorum-count": "3"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - read must pass, write will fail
        g.log.info("Start creating files on all mounts...")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name textfilename %s" %
                   (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        g.log.info("Validating whether IO failed with Read-only file system")
        ret, _ = is_io_procs_fail_with_rofs(self, all_mounts_procs,
                                            self.mounts)
        self.assertTrue(ret, ("Unexpected error and IO successful"
                              " on Read-only file system"))
        g.log.info("EXPECTED: Read-only file system in IO while creating file")

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )

        # set the quorum-type to none
        options = {"cluster.quorum-type": "none"}
        g.log.info("setting %s for the volume %s", options, self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set %s for volume %s"
                              % (options, self.volname)))
        g.log.info("Successfully set %s for volume %s",
                   options, self.volname)

        # start I/0 ( write and read ) - must succeed
        g.log.info("Starting IO on all mounts...")
        g.log.info("mounts: %s", self.mounts)
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s create_files "
                   "-f 10 --base-file-name lastfile %s"
                   % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # read the file
        g.log.info("Start reading files on all mounts")
        all_mounts_procs = []
        for mount_obj in self.mounts:
            cmd = ("python %s read "
                   "%s" % (self.script_upload_path, mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "Reads failed on some of the clients"
        )
