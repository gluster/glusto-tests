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
