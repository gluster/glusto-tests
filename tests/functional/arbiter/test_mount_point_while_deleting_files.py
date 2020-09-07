#  Copyright (C) 2016-2020 Red Hat, Inc. <http://www.redhat.com>
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

import os

from glusto.core import Glusto as g

from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.glusterdir import rmdir
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.volume_libs import setup_volume, cleanup_volume
from glustolibs.gluster.volume_ops import get_volume_list
from glustolibs.gluster.brick_libs import (bring_bricks_offline,
                                           select_bricks_to_bring_offline)
from glustolibs.io.utils import (validate_io_procs,
                                 list_all_files_and_dirs_mounts,
                                 wait_for_io_to_complete)
from glustolibs.gluster.mount_ops import (mount_volume,
                                          umount_volume,
                                          create_mount_objs)
from glustolibs.misc.misc_libs import upload_scripts


@runs_on([['arbiter'], ['glusterfs']])
class VolumeSetDataSelfHealTests(GlusterBaseClass):
    @classmethod
    def setUpClass(cls):
        # Calling GlusterBaseClass setUpClass
        cls.get_super_method(cls, 'setUpClass')()

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

        # Setup Volumes
        cls.volume_configs = []
        cls.mounts_dict_list = []
        cls.client = cls.clients[0]

        # Define two replicated volumes
        for i in range(1, 3):
            volume_config = {
                'name': 'testvol_%s_%d' % (cls.volume['voltype']['type'], i),
                'servers': cls.servers,
                'voltype': cls.volume['voltype']}
            cls.volume_configs.append(volume_config)

            # Redefine mounts
            mount = {
                'protocol': cls.mount_type,
                'server': cls.mnode,
                'volname': volume_config['name'],
                'client': cls.all_clients_info[cls.client],
                'mountpoint': (os.path.join(
                    "/mnt", '_'.join([volume_config['name'],
                                      cls.mount_type]))),
                'options': ''
                }
            cls.mounts_dict_list.append(mount)

        cls.mounts = create_mount_objs(cls.mounts_dict_list)

        # Create and mount volumes
        cls.mount_points = []
        for volume_config in cls.volume_configs:

            # Setup volume
            ret = setup_volume(mnode=cls.mnode,
                               all_servers_info=cls.all_servers_info,
                               volume_config=volume_config,
                               force=False)
            if not ret:
                raise ExecutionError("Failed to setup Volume %s"
                                     % volume_config['name'])
            g.log.info("Successful in setting volume %s",
                       volume_config['name'])

            # Mount volume
            mount_point = (os.path.join("/mnt", '_'.join(
                [volume_config['name'], cls.mount_type])))
            cls.mount_points.append(mount_point)
            ret, _, _ = mount_volume(volume_config['name'],
                                     cls.mount_type,
                                     mount_point,
                                     cls.mnode, cls.client)
            if ret:
                raise ExecutionError(
                    "Failed to do gluster mount on volume %s "
                    % cls.volname)
            g.log.info("Successfully mounted %s on client %s",
                       cls.volname, cls.client)

    def setUp(self):
        """
        setUp method for every test
        """

        # calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

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

        # umount all volumes
        for mount_point in self.mount_points:
            ret, _, _ = umount_volume(
                self.client, mount_point)
            if ret:
                raise ExecutionError(
                    "Failed to umount on volume %s "
                    % self.volname)
            g.log.info("Successfully umounted %s on client %s",
                       self.volname, self.client)
            ret = rmdir(self.client, mount_point)
            if not ret:
                raise ExecutionError(
                    "Failed to remove directory mount directory.")
            g.log.info("Mount directory is removed successfully")

        # stopping all volumes
        volume_list = get_volume_list(self.mnode)
        for volume in volume_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Failed to cleanup Volume %s" % volume)
            g.log.info("Volume: %s cleanup is done", volume)
        g.log.info("Successfully Cleanedup all Volumes")

        # calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

    def test_mount_point_not_go_to_rofs(self):
        """
        - create two volumes with arbiter1 and mount it on same client
        - create IO
        - start deleting files from both mountpoints
        - kill brick from one of the node
        - Check if all the files are deleted from the mount point
        from both the servers
        """
        # pylint: disable=too-many-locals,too-many-statements
        # create files on all mounts
        g.log.info("Starting IO on all mounts...")
        for mount_obj in self.mounts:
            g.log.info("Generating data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Create files
            g.log.info('Creating files...')
            command = ("/usr/bin/env python %s create_files "
                       "-f 100 "
                       "--fixed-file-size 1M "
                       "%s" % (
                           self.script_upload_path,
                           mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients")

        # select bricks to bring offline
        volume_list = get_volume_list(self.mnode)
        for volname in volume_list:
            bricks_to_bring_offline_dict = (select_bricks_to_bring_offline(
                self.mnode, volname))
            bricks_to_bring_offline = (
                bricks_to_bring_offline_dict['volume_bricks'])

            # bring bricks offline
            g.log.info("Going to bring down the brick process for %s",
                       bricks_to_bring_offline)
            ret = bring_bricks_offline(volname, bricks_to_bring_offline)
            self.assertTrue(ret, ("Failed to bring down the bricks. Please "
                                  "check the log file for more details."))
            g.log.info("Brought down the brick process for %s successfully",
                       bricks_to_bring_offline)

        # delete files on all mounts
        g.log.info("Deleting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Deleting data for %s:%s",
                       mount_obj.client_system, mount_obj.mountpoint)
            # Delete files
            g.log.info('Deleting files...')
            command = "/usr/bin/env python %s delete %s" % (
                self.script_upload_path,
                mount_obj.mountpoint)
            proc = g.run_async(mount_obj.client_system, command,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients")
        self.io_validation_complete = True
