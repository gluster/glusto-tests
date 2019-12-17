#  Copyright (C) 2017-2020  Red Hat, Inc. <http://www.redhat.com>
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

""" Test Arbiter Specific Cases"""

import sys
from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.volume_libs import (
    replace_brick_from_volume, expand_volume, shrink_volume,
    wait_for_volume_process_to_be_online,
    verify_all_process_of_volume_are_online)
from glustolibs.gluster.rebalance_ops import (
    rebalance_start, rebalance_status, wait_for_rebalance_to_complete)
from glustolibs.gluster.heal_libs import (monitor_heal_completion,
                                          is_heal_complete)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.io.utils import (validate_io_procs,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'cifs', 'nfs']])
class GlusterArbiterVolumeTypeChangeClass(GlusterBaseClass):
    """Class for testing Volume Type Change from replicated to
        Arbitered volume
    """
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

        # Overriding the volume type to specifically test the volume type
        # change from replicated to arbiter
        if cls.volume_type == "replicated":
            cls.volume['voltype'] = {
                'type': 'replicated',
                'replica_count': 2,
                'dist_count': 1,
                'transport': 'tcp'}

        if cls.volume_type == "distributed-replicated":
            cls.volume['voltype'] = {
                'type': 'distributed-replicated',
                'dist_count': 2,
                'replica_count': 2,
                'transport': 'tcp'}

        # Upload io scripts for running IO on mounts
        g.log.info("Upload io scripts to clients %s for running IO on mounts",
                   cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
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
        # dirs, we are incrementing the counter by --dir-length value i.e 10
        # in this test suite.

        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

    def setUp(self):
        """
        - Setup Volume and Mount Volume
        """
        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """If test method failed before validating IO, tearDown waits for the
        IO's to complete and checks for the IO exit status

        Unmount Volume and Cleanup Volume
        """
        # Wait for IO to complete if io validation is not executed in the
        # test method
        if not self.io_validation_complete:
            g.log.info("Wait for IO to complete as IO validation did not "
                       "succeed in test method")
            ret = wait_for_io_to_complete(self.all_mounts_procs, self.mounts)
            if not ret:
                raise ExecutionError("IO failed on some of the clients")
            g.log.info("IO is successful on all mounts")

        # Unmount Volume and Cleanup Volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Unmount Volume and Cleanup Volume")
        g.log.info("Successful in Unmount Volume and Cleanup Volume")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_replicated_to_arbiter_volume_change_with_volume_ops(self):
        """
        - Change the volume type from replicated to arbiter
        - Perform add-brick, rebalance on arbitered volume
        - Perform replace-brick on arbitered volume
        - Perform remove-brick on arbitered volume
        """
        # pylint: disable=too-many-statements

        # Start IO on mounts
        self.all_mounts_procs = []
        g.log.info("Starting IO on %s:%s", self.mounts[0].client_system,
                   self.mounts[0].mountpoint)
        cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
               "--dirname-start-num 10 --dir-depth 1 --dir-length 1 "
               "--max-num-of-dirs 1 --num-of-files 5 %s" % (
                   sys.version_info.major, self.script_upload_path,
                   self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts[0]),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Adding bricks to make an Arbiter Volume
        g.log.info("Adding bricks to convert to Arbiter Volume")
        ret = expand_volume(self.mnode, self.volname, self.servers[2:],
                            self.all_servers_info, replica_count=1,
                            arbiter_count=1)
        self.assertTrue(ret, ("Failed to expand the volume  %s", self.volname))
        g.log.info("Changing volume to arbiter volume is successful %s",
                   self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verifying all bricks online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Checking for heals to finish after changing the volume type
        # from replicated to arbitered volume
        g.log.info("Wait for self-heal to complete after changing the "
                   "volume type from replicated to arbitered volume")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, ("Self heal didn't complete even after waiting "
                              "for 20 minutes."))
        g.log.info("self-heal is successful after changing the volume type "
                   "from replicated to arbitered volume")

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Start IO on mounts
        self.all_mounts_procs = []
        g.log.info("Starting IO on %s:%s", self.mounts[0].client_system,
                   self.mounts[0].mountpoint)
        cmd = ("/usr/bin/env python%d %s create_deep_dirs_with_files "
               "--dirname-start-num 10 --dir-depth 1 --dir-length 1 "
               "--max-num-of-dirs 1 --num-of-files 5 %s" % (
                   sys.version_info.major, self.script_upload_path,
                   self.mounts[0].mountpoint))
        proc = g.run_async(self.mounts[0].client_system, cmd,
                           user=self.mounts[0].user)
        self.all_mounts_procs.append(proc)
        self.io_validation_complete = False

        # Start add-brick (subvolume-increase)
        g.log.info("Start adding bricks to volume when IO in progress")
        ret = expand_volume(self.mnode, self.volname, self.servers,
                            self.all_servers_info)
        self.assertTrue(ret, ("Failed to expand the volume when IO in "
                              "progress on volume %s", self.volname))
        g.log.info("Expanding volume when IO in progress is successful on "
                   "volume %s", self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Start Rebalance
        g.log.info("Starting Rebalance on the volume")
        ret, _, _ = rebalance_start(self.mnode, self.volname)
        self.assertEqual(ret, 0, ("Failed to start rebalance on the volume "
                                  "%s", self.volname))
        g.log.info("Successfully started rebalance on the volume %s",
                   self.volname)

        # Log Rebalance status
        g.log.info("Log Rebalance status")
        _, _, _ = rebalance_status(self.mnode, self.volname)

        # Wait for rebalance to complete
        g.log.info("Waiting for rebalance to complete")
        ret = wait_for_rebalance_to_complete(self.mnode, self.volname, 600)
        self.assertTrue(ret, "Rebalance did not start "
                             "despite waiting for 5 mins")
        g.log.info("Rebalance is successfully complete on the volume %s",
                   self.volname)

        # Replace brick from a sub-volume
        g.log.info("Replace a faulty brick from the volume")
        ret = replace_brick_from_volume(self.mnode, self.volname,
                                        self.servers, self.all_servers_info)
        self.assertTrue(ret, "Failed to replace faulty brick from the volume")
        g.log.info("Successfully replaced faulty brick from the volume")

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Wait for self-heal to complete
        g.log.info("Wait for self-heal to complete")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "Self heal didn't complete even after waiting "
                        "for 20 minutes. 20 minutes is too much a time for "
                        "current test workload")
        g.log.info("self-heal is successful after replace-brick operation")

        # Check if heal is completed
        ret = is_heal_complete(self.mnode, self.volname)
        self.assertTrue(ret, 'Heal is not complete')
        g.log.info('Heal is completed successfully')

        # Shrinking volume by removing bricks from volume when IO in progress
        g.log.info("Start removing bricks from volume when IO in progress")
        ret = shrink_volume(self.mnode, self.volname, rebalance_timeout=900)
        self.assertTrue(ret, ("Failed to shrink the volume when IO in "
                              "progress on volume %s", self.volname))
        g.log.info("Shrinking volume when IO in progress is successful on "
                   "volume %s", self.volname)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Failed to wait for volume %s processes to "
                              "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online after "
                   "shrinking volume")
        ret = verify_all_process_of_volume_are_online(self.mnode, self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online",
                              self.volname))
        g.log.info("Volume %s : All process are online after shrinking volume",
                   self.volname)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts[0]),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True
