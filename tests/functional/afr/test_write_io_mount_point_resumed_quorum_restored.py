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


import time
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import (
    set_volume_options, get_subvols,
    wait_for_volume_process_to_be_online,
    verify_all_process_of_volume_are_online)
from glustolibs.misc.misc_libs import (upload_scripts,
                                       are_nodes_online,
                                       reboot_nodes)
from glustolibs.io.utils import (validate_io_procs,
                                 is_io_procs_fail_with_rofs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)


@runs_on([['distributed-replicated'],
          ['glusterfs']])
class ClientSideQuorumRestored(GlusterBaseClass):
    """ Description:
            Test Cases in this module tests the client side quorum.
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

        # Cleanup and umount volume
        g.log.info("Starting to Unmount Volume and Cleanup Volume")
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass teardown
        GlusterBaseClass.tearDown.im_func(self)

    def test_write_io_mount_point_resumed_quorum_restored_x3(self):
        """
        - set cluster.quorum-type to auto
        - start I/O from the mount point
        - Do IO and check on subvols with two nodes to reboot
        (do for each subvol)
        - get files to delete/create for nodes to be offline
        - delete files from mountpoint
        - reboot nodes
        - creating files on nodes while rebooting
        - validate for rofs
        - wait for volume processes to be online
        - creating files on nodes after rebooting
        - validate IO
        - Do IO and check on subvols without nodes to reboot
        (do for each subvol)
        - get files to delete/create for nodes to be online
        - delete files from mountpoint
        - reboot nodes
        - creating files on online nodes while rebooting other nodes
        - validate IO
        - Do IO and check and reboot two nodes on all subvols
        - get files to delete/create for nodes to be offline
        - delete files from mountpoint
        - reboot nodes
        - creating files on nodes while rebooting
        - validate for rofs
        - wait for volume processes to be online
        - creating files on nodes after rebooting
        - validate IO
        """
        # pylint: disable=too-many-locals,too-many-statements,too-many-branches
        # set cluster.quorum-type to auto
        options = {"cluster.quorum-type": "auto"}
        g.log.info("setting cluster.quorum-type to auto on volume %s",
                   self.volname)
        ret = set_volume_options(self.mnode, self.volname, options)
        self.assertTrue(ret, ("Unable to set volume option %s for"
                              "volume %s" % (options, self.volname)))
        g.log.info("Sucessfully set %s for volume %s",
                   options, self.volname)

        # Creating files on client side
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

            # Creating files
            cmd = ("python %s create_files -f 30 %s"
                   % (self.script_upload_path, mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        self.io_validation_complete = False
        g.log.info("Wait for IO to complete and validate IO ...")
        ret = validate_io_procs(self.all_mounts_procs, self.mounts)
        self.assertTrue(ret, "IO failed on some of the clients")
        self.io_validation_complete = True
        g.log.info("IO is successful on all mounts")

        # Do IO and check on subvols with nodes to reboot
        subvols_dict = get_subvols(self.mnode, self.volname)
        for subvol in subvols_dict['volume_subvols']:
            # define nodes to reboot
            brick_list = subvol[0:2]
            nodes_to_reboot = []
            for brick in brick_list:
                node, brick_path = brick.split(':')
                nodes_to_reboot.append(node)

            # get files to delete/create for nodes to be offline
            node, brick_path = brick_list[0].split(':')
            ret, brick_file_list, _ = g.run(node, 'ls %s' % brick_path)
            self.assertFalse(ret, 'Failed to ls files on %s' % node)
            file_list = brick_file_list.splitlines()

            # delete files from mountpoint
            for mount_obj in self.mounts:
                g.log.info("Deleting data for %s:%s",
                           mount_obj.client_system, mount_obj.mountpoint)
                cmd = ('cd %s/ ; rm -rf %s'
                       % (mount_obj.mountpoint, ' '.join(file_list)))
                ret, _, _ = g.run(mount_obj.client_system, cmd)
                self.assertFalse(ret, 'Failed to rm file on %s'
                                 % mount_obj.client_system)
            g.log.info('Files %s are deleted', file_list)

            # reboot nodes on subvol and wait while rebooting
            g.log.info("Rebooting the nodes %s", nodes_to_reboot)
            ret = reboot_nodes(nodes_to_reboot)
            self.assertTrue(ret, 'Failed to reboot nodes %s '
                            % nodes_to_reboot)

            # Creating files on nodes while rebooting
            self.all_mounts_procs = []
            for mount_obj in self.mounts:
                g.log.info("Creating data for %s:%s",
                           mount_obj.client_system, mount_obj.mountpoint)

                # Creating files
                cmd = ("cd %s/ ;"
                       "touch %s"
                       % (mount_obj.mountpoint, ' '.join(file_list)))

                proc = g.run_async(mount_obj.client_system, cmd,
                                   user=mount_obj.user)
                self.all_mounts_procs.append(proc)

                # Validate IO
                self.io_validation_complete = False
                g.log.info("Validating if IO failed with read-only filesystem")
                ret = is_io_procs_fail_with_rofs(self, self.all_mounts_procs,
                                                 self.mounts)
                self.assertTrue(ret, ("Unexpected error and IO successful"
                                      " on read-only filesystem"))
                self.io_validation_complete = True
                g.log.info("EXPECTED: "
                           "Read-only file system in IO while creating file")

            # check if nodes are online
            counter = 0
            timeout = 300
            _rc = False
            while counter < timeout:
                ret, reboot_results = are_nodes_online(nodes_to_reboot)
                if not ret:
                    g.log.info("Nodes are offline, Retry after 5 seconds ... ")
                    time.sleep(5)
                    counter = counter + 5
                else:
                    _rc = True
                    break

            if not _rc:
                for node in reboot_results:
                    if reboot_results[node]:
                        g.log.info("Node %s is online", node)
                    else:
                        g.log.error("Node %s is offline even after "
                                    "%d minutes", node, timeout / 60.0)
            else:
                g.log.info("All nodes %s are up and running", nodes_to_reboot)

            # Wait for volume processes to be online
            g.log.info("Wait for volume processes to be online")
            ret = wait_for_volume_process_to_be_online(self.mnode,
                                                       self.volname)
            self.assertTrue(ret,
                            ("Failed to wait for volume %s processes to "
                             "be online", self.volname))
            g.log.info("Successful in waiting for volume %s processes to be "
                       "online", self.volname)

            # Verify volume's all process are online
            g.log.info("Verifying volume's all process are online")
            ret = verify_all_process_of_volume_are_online(self.mnode,
                                                          self.volname)
            self.assertTrue(ret, ("Volume %s : All process are not online"
                                  % self.volname))
            g.log.info("Volume %s : All process are online", self.volname)

            # Creating files on nodes after rebooting
            self.all_mounts_procs = []
            for mount_obj in self.mounts:
                g.log.info("Creating data for %s:%s",
                           mount_obj.client_system, mount_obj.mountpoint)

                # Creating files
                cmd = ("cd %s/ ;"
                       "touch %s"
                       % (mount_obj.mountpoint, ' '.join(file_list)))

                proc = g.run_async(mount_obj.client_system, cmd,
                                   user=mount_obj.user)
                self.all_mounts_procs.append(proc)

            # Validate IO
            self.io_validation_complete = False
            g.log.info("Wait for IO to complete and validate IO ...")
            ret = validate_io_procs(self.all_mounts_procs, self.mounts)
            self.assertTrue(ret, "IO failed on some of the clients")
            self.io_validation_complete = True
            g.log.info("IO is successful on all mounts")

        # Do IO and check on subvols without nodes to reboot
        subvols_dict = get_subvols(self.mnode, self.volname)
        for subvol in subvols_dict['volume_subvols']:
            # define nodes to reboot
            brick_list = subvol[0:2]
            nodes_to_reboot = []
            for brick in brick_list:
                node, brick_path = brick.split(':')
                nodes_to_reboot.append(node)

            # get files to delete/create for nodes to be online
            new_subvols_dict = get_subvols(self.mnode, self.volname)
            subvol_to_operate = new_subvols_dict['volume_subvols']
            subvol_to_operate.remove(subvol)
            brick_list_subvol_online = subvol_to_operate[0]

            node, brick_path_vol_online = \
                brick_list_subvol_online[0].split(':')
            ret, brick_file_list, _ = g.run(node,
                                            'ls %s' % brick_path_vol_online)
            self.assertFalse(ret, 'Failed to ls files on %s' % node)
            file_list = brick_file_list.splitlines()

            # delete files from mountpoint
            for mount_obj in self.mounts:
                g.log.info("Deleting data for %s:%s",
                           mount_obj.client_system, mount_obj.mountpoint)
                cmd = ('cd %s/ ; rm -rf %s'
                       % (mount_obj.mountpoint, ' '.join(file_list)))
                ret, _, _ = g.run(mount_obj.client_system, cmd)
                self.assertFalse(ret, 'Failed to rm file on %s'
                                 % mount_obj.client_system)
            g.log.info('Files %s are deleted', file_list)

            # reboot nodes on subvol and wait while rebooting
            g.log.info("Rebooting the nodes %s", nodes_to_reboot)
            ret = reboot_nodes(nodes_to_reboot)
            self.assertTrue(ret, 'Failed to reboot nodes %s '
                            % nodes_to_reboot)

            # Creating files on nodes while rebooting
            self.all_mounts_procs = []
            for mount_obj in self.mounts:
                g.log.info("Creating data for %s:%s",
                           mount_obj.client_system, mount_obj.mountpoint)

                # Creating files
                cmd = ("cd %s/ ;"
                       "touch %s"
                       % (mount_obj.mountpoint, ' '.join(file_list)))

                proc = g.run_async(mount_obj.client_system, cmd,
                                   user=mount_obj.user)
                self.all_mounts_procs.append(proc)

                # Validate IO
                self.io_validation_complete = False
                g.log.info("Wait for IO to complete and validate IO ...")
                ret = validate_io_procs(self.all_mounts_procs, self.mounts)
                self.assertTrue(ret, "IO failed on some of the clients")
                self.io_validation_complete = True
                g.log.info("IO is successful on all mounts")

            # check if nodes are online
            counter = 0
            timeout = 300
            _rc = False
            while counter < timeout:
                ret, reboot_results = are_nodes_online(nodes_to_reboot)
                if not ret:
                    g.log.info("Nodes are offline, Retry after 5 seconds ... ")
                    time.sleep(5)
                    counter = counter + 5
                else:
                    _rc = True
                    break

            if not _rc:
                for node in reboot_results:
                    if reboot_results[node]:
                        g.log.info("Node %s is online", node)
                    else:
                        g.log.error("Node %s is offline even after "
                                    "%d minutes", node, timeout / 60.0)
            else:
                g.log.info("All nodes %s are up and running", nodes_to_reboot)

            # Wait for volume processes to be online
            g.log.info("Wait for volume processes to be online")
            ret = wait_for_volume_process_to_be_online(self.mnode,
                                                       self.volname)
            self.assertTrue(ret,
                            ("Failed to wait for volume %s processes to "
                             "be online", self.volname))
            g.log.info("Successful in waiting for volume %s processes to be "
                       "online", self.volname)

            # Verify volume's all process are online
            g.log.info("Verifying volume's all process are online")
            ret = verify_all_process_of_volume_are_online(self.mnode,
                                                          self.volname)
            self.assertTrue(ret, ("Volume %s : All process are not online"
                                  % self.volname))
            g.log.info("Volume %s : All process are online", self.volname)

        # Do IO and check and reboot nodes on all subvols
        subvols_dict = get_subvols(self.mnode, self.volname)
        nodes_to_reboot = []
        file_list_for_all_subvols = []
        for subvol in subvols_dict['volume_subvols']:
            # define nodes to reboot
            brick_list = subvol[0:2]
            for brick in brick_list:
                node, brick_path = brick.split(':')
                nodes_to_reboot.append(node)

            # get files to delete/create for nodes to be offline
            node, brick_path = brick_list[0].split(':')
            ret, brick_file_list, _ = g.run(node, 'ls %s' % brick_path)
            self.assertFalse(ret, 'Failed to ls files on %s' % node)
            file_list = brick_file_list.splitlines()
            file_list_for_all_subvols.append(file_list)

            # delete files from mountpoint
            for mount_obj in self.mounts:
                g.log.info("Deleting data for %s:%s",
                           mount_obj.client_system, mount_obj.mountpoint)
                cmd = ('cd %s/ ; rm -rf %s'
                       % (mount_obj.mountpoint, ' '.join(file_list)))
                ret, _, _ = g.run(mount_obj.client_system, cmd)
                self.assertFalse(ret, 'Failed to rm file on %s' % node)
            g.log.info('Files %s are deleted', file_list)

        # reboot nodes on subvol and wait while rebooting
        g.log.info("Rebooting the nodes %s", nodes_to_reboot)
        ret = reboot_nodes(nodes_to_reboot)
        self.assertTrue(ret, 'Failed to reboot nodes %s '
                        % nodes_to_reboot)

        # Creating files on nodes while rebooting
        all_mounts_procs, all_mounts_procs_1, all_mounts_procs_2 = [], [], []
        # Create files for 1-st subvol and get all_mounts_procs_1
        for mount_obj in self.mounts:
            g.log.info("Creating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

            # Creating files
            cmd = ("cd %s/ ;"
                   "touch %s"
                   % (mount_obj.mountpoint,
                      ' '.join(file_list_for_all_subvols[0])))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs_1.append(proc)
            all_mounts_procs.append(all_mounts_procs_1)

        # Create files for 2-st subvol and get all_mounts_procs_2
        for mount_obj in self.mounts:
            g.log.info("Creating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

            # Creating files
            cmd = ("cd %s/ ;"
                   "touch %s"
                   % (mount_obj.mountpoint,
                      ' '.join(file_list_for_all_subvols[1])))

            proc2 = g.run_async(mount_obj.client_system, cmd,
                                user=mount_obj.user)
            all_mounts_procs_2.append(proc2)
            all_mounts_procs.append(all_mounts_procs_2)

        for mounts_procs in all_mounts_procs:
            # Validate IO
            self.io_validation_complete = False
            g.log.info("Validating if IO failed with read-only filesystem")
            ret = is_io_procs_fail_with_rofs(self, mounts_procs,
                                             self.mounts)
            self.assertTrue(ret, ("Unexpected error and IO successful"
                                  " on read-only filesystem"))
            self.io_validation_complete = True
            g.log.info("EXPECTED: "
                       "Read-only file system in IO while creating file")

        # check if nodes are online
        counter = 0
        timeout = 300
        _rc = False
        while counter < timeout:
            ret, reboot_results = are_nodes_online(nodes_to_reboot)
            if not ret:
                g.log.info("Nodes are offline, Retry after 5 seconds ... ")
                time.sleep(5)
                counter = counter + 5
            else:
                _rc = True
                break

        if not _rc:
            for node in reboot_results:
                if reboot_results[node]:
                    g.log.info("Node %s is online", node)
                else:
                    g.log.error("Node %s is offline even after "
                                "%d minutes", node, timeout / 60.0)
        else:
            g.log.info("All nodes %s are up and running", nodes_to_reboot)

        # Wait for volume processes to be online
        g.log.info("Wait for volume processes to be online")
        ret = wait_for_volume_process_to_be_online(self.mnode,
                                                   self.volname)
        self.assertTrue(ret,
                        ("Failed to wait for volume %s processes to "
                         "be online", self.volname))
        g.log.info("Successful in waiting for volume %s processes to be "
                   "online", self.volname)

        # Verify volume's all process are online
        g.log.info("Verifying volume's all process are online")
        ret = verify_all_process_of_volume_are_online(self.mnode,
                                                      self.volname)
        self.assertTrue(ret, ("Volume %s : All process are not online"
                              % self.volname))
        g.log.info("Volume %s : All process are online", self.volname)

        # Creating files on nodes after rebooting
        all_mounts_procs, all_mounts_procs_1, all_mounts_procs_2 = [], [], []
        # Create files for 1-st subvol and get all_mounts_procs_1
        for mount_obj in self.mounts:
            g.log.info("Creating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

            # Creating files
            cmd = ("cd %s/ ;"
                   "touch %s"
                   % (mount_obj.mountpoint,
                      ' '.join(file_list_for_all_subvols[0])))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            all_mounts_procs_1.append(proc)
            all_mounts_procs.append(all_mounts_procs_1)

        # Create files for 2-st subvol and get all_mounts_procs_2
        for mount_obj in self.mounts:
            g.log.info("Creating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)

            # Creating files
            cmd = ("cd %s/ ;"
                   "touch %s"
                   % (mount_obj.mountpoint,
                      ' '.join(file_list_for_all_subvols[1])))

            proc2 = g.run_async(mount_obj.client_system, cmd,
                                user=mount_obj.user)
            all_mounts_procs_2.append(proc2)
            all_mounts_procs.append(all_mounts_procs_2)

        for mounts_procs in all_mounts_procs:
            # Validate IO
            self.io_validation_complete = False
            g.log.info("Wait for IO to complete and validate IO ...")
            ret = validate_io_procs(mounts_procs, self.mounts)
            self.assertTrue(ret, "IO failed on some of the clients")
            self.io_validation_complete = True
            g.log.info("IO is successful on all mounts")
