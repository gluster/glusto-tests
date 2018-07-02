#  Copyright (C) 2017-2018  Red Hat, Inc. <http://www.redhat.com>
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

from glusto.core import Glusto as g
from glustolibs.gluster.gluster_base_class import (GlusterBaseClass, runs_on)
from glustolibs.gluster.volume_libs import (
    log_volume_info_and_status)
from glustolibs.gluster.brick_libs import (
    bring_bricks_offline, bring_bricks_online,
    are_bricks_offline, are_bricks_online, select_bricks_to_bring_offline)
from glustolibs.gluster.heal_libs import (
    monitor_heal_completion)
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['replicated', 'distributed-replicated'],
          ['glusterfs', 'cifs', 'nfs']])
class TestRmrfMount(GlusterBaseClass):
    """
    Description:
        Removing files when one of the brick if in offline state
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
        # dirs, we are incrementing the counter by --dir-length value i.e 10
        # in this test suite.

        # If we are changing the --dir-length to new value, ensure the counter
        # is also incremented by same value to create new set of files/dirs.

    def setUp(self):
        # Calling GlusterBaseClass setUp
        GlusterBaseClass.setUp.im_func(self)

        self.all_mounts_procs = []
        self.io_validation_complete = False

        # Setup Volume and Mount Volume
        g.log.info("Starting to Setup Volume and Mount Volume")
        ret = self.setup_volume_and_mount_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):
        """
        If test method failed before validating IO, tearDown waits for the
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
        GlusterBaseClass.tearDown.im_func(self)

    def test_self_heal(self):
        """
        Description:-
        - Create files on mount point
        - Kill one brick from volume
        - rm -rfv on mount point
        - bring bricks online
        - wait for heals
        - list
        """
        # pylint: disable=too-many-statements

        # IO on the mount point
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 35 "
                   "--max-num-of-dirs 5 "
                   "--num-of-files 5 %s" % (self.script_upload_path,
                                            self.counter,
                                            mount_obj.mountpoint))
            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10

        # Select bricks to bring offline
        bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
            self.mnode, self.volname))
        bricks_to_bring_offline = filter(None, (
            bricks_to_bring_offline_dict['hot_tier_bricks'] +
            bricks_to_bring_offline_dict['cold_tier_bricks'] +
            bricks_to_bring_offline_dict['volume_bricks']))

        # Killing one brick from the volume set
        g.log.info("Bringing bricks: %s offline", bricks_to_bring_offline)
        ret = bring_bricks_offline(self.volname, bricks_to_bring_offline)
        self.assertTrue(ret, ("Failed to bring bricks: %s offline",
                              bricks_to_bring_offline))
        g.log.info("Successful in bringing bricks: %s offline",
                   bricks_to_bring_offline)

        # Validate if bricks are offline
        g.log.info("Validating if bricks: %s are offline",
                   bricks_to_bring_offline)
        ret = are_bricks_offline(self.mnode, self.volname,
                                 bricks_to_bring_offline)
        self.assertTrue(ret, "Not all the bricks in list: %s are offline" %
                        bricks_to_bring_offline)
        g.log.info("Successfully validated that bricks: %s are all offline",
                   bricks_to_bring_offline)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )
        self.io_validation_complete = True

        # Checking volume status
        g.log.info("Logging volume info and Status after bringing bricks "
                   "offline from the volume %s", self.volname)
        ret = log_volume_info_and_status(self.mnode, self.volname)
        self.assertTrue(ret, ("Logging volume info and status failed on "
                              "volume %s", self.volname))
        g.log.info("Successful in logging volume info and status of volume %s",
                   self.volname)

        # Removing files from the mount point when one brick is down
        g.log.info("Removing files from the mount point")
        mountpoint = self.mounts[0].mountpoint
        client = self.mounts[0].client_system
        cmd = "rm -rfv %s/*" % mountpoint
        ret, _, _ = g.run(client, cmd)
        if ret != 0:
            raise ExecutionError("failed to delete the files")

        # Bringing bricks online
        g.log.info('Bringing bricks %s online', bricks_to_bring_offline)
        ret = bring_bricks_online(self.mnode, self.volname,
                                  bricks_to_bring_offline)
        self.assertTrue(ret, 'Failed to bring bricks %s online' %
                        bricks_to_bring_offline)
        g.log.info('Bricks %s are online', bricks_to_bring_offline)

        # Check if bricks are online
        g.log.info("Checking bricks are online or not")
        ret = are_bricks_online(self.mnode, self.volname,
                                bricks_to_bring_offline)
        self.assertTrue(ret, 'Bricks %s are not online' %
                        bricks_to_bring_offline)
        g.log.info('Bricks %s are online', bricks_to_bring_offline)

        # Monitoring heals on the volume
        g.log.info("Wait for heal completion...")
        ret = monitor_heal_completion(self.mnode, self.volname)
        self.assertTrue(ret, "Self heal didn't complete even after waiting "
                             "for 20 minutes.")
        g.log.info("self-heal is successful after changing the volume type "
                   "from replicated to arbitered volume")

        # List all files and dirs created
        g.log.info("List all files and directories:")
        ret = list_all_files_and_dirs_mounts(self.mounts)
        self.assertTrue(ret, "Failed to list all files and dirs")
        g.log.info("Listing all files and directories is successful")
