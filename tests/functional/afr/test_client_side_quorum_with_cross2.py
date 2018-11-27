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

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (
    set_volume_options, get_subvols)
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           bring_bricks_online)
from glustolibs.io.utils import (validate_io_procs,
                                 is_io_procs_fail_with_rofs)


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'nfs']])
class ClientSideQuorumCross2Tests(GlusterBaseClass):
    """
    ClientSideQuorumCross2Tests contains tests which verifies the
    client side quorum Test Cases with cross 2 volume
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
        g.log.info("Starting IO .....")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 10 --base-file-name file %s" % (self.script_upload_path,
                                                   self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        offline_bricks = []
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
            offline_bricks.append(bricks_to_bring_offline)

        # create new file named newfile0.txt
        g.log.info("Start creating new file on all mounts...")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 1 --base-file-name newfile %s" %
               (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # create directory user1
        g.log.info("Start creating directory on all mounts...")
        all_mounts_procs = []
        cmd = ("python %s create_deep_dir %s"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # create h/w link to file
        g.log.info("Start creating hard link for file0.txt on mount")
        cmd = ("ln %s/file0.txt %s/file0.txt_hwlink"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, 'Failed to create hard link '
                              'for file0.txt on %s'
                         % self.mounts[0].mountpoint)
        g.log.info("Hard link for file0.txt on %s is created successfully",
                   self.mounts[0].mountpoint)

        # create s/w link
        g.log.info("Start creating soft link for file1.txt on mount")
        cmd = ("ln -s %s/file1.txt %s/file1.txt_swlink"
               % (self.mounts[0].mountpoint, self.mounts[0].mountpoint))
        ret, _, _ = g.run(self.mounts[0].client_system, cmd)
        self.assertFalse(ret, 'Failed to create soft link '
                              'for file1.txt on %s'
                         % self.mounts[0].mountpoint)
        g.log.info("Soft link for file1.txt on %s is created successfully",
                   self.mounts[0].mountpoint)

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

        # bring back the bricks online for all subvolumes
        g.log.info("bringing up the brick : %s online",
                   offline_bricks)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  offline_bricks)
        self.assertTrue(ret, ("Failed to brought the brick %s online"
                              % offline_bricks))
        g.log.info("Successfully brought the bricks")

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
        cmd = ("python %s create_files "
               "-f 10 --base-file-name file %s"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        cmd = ("python %s create_files "
               "-f 10 --base-file-name second_file %s"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        g.log.info("Starting IO on mount.....")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 10 --base-file-name third_file %s"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        g.log.info("Starting IO on mount......")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 10 --base-file-name fourth_file %s" %
               (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        g.log.info("Starting IO on mount.....")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 10 --base-file-name fifth_file %s"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        g.log.info("Start creating files on mounts.....")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 10 --base-file-name sixth_file %s" %
               (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        g.log.info("Starting IO on mount.....")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 10 --base-file-name seventh_file %s"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        g.log.info("Starting IO on mount.....")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 10 --base-file-name eigth_file %s"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        g.log.info("Start creating files on mounts.....")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 10 --base-file-name ninth_file %s" %
               (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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
        g.log.info("Starting IO on mount.....")
        all_mounts_procs = []
        cmd = ("python %s create_files "
               "-f 10 --base-file-name tenth_file %s"
               % (self.script_upload_path, self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
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

        # bring back the bricks online for all subvolumes
        g.log.info("bringing up the brick : %s online",
                   subvolumes_first_brick_list)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  subvolumes_first_brick_list)
        self.assertTrue(ret, ("Failed to brought the brick %s online"
                              % subvolumes_first_brick_list))
        g.log.info("Successfully brought the bricks")
