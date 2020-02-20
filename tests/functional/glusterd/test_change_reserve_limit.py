#  Copyright (C) 2019-2020  Red Hat, Inc. <http://www.redhat.com>
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
#  You should have received a copy of the GNU General Public License along`
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from glusto.core import Glusto as g

from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.misc.misc_libs import upload_scripts
from glustolibs.gluster.volume_libs import (cleanup_volume, setup_volume)
from glustolibs.gluster.volume_ops import (get_volume_list, set_volume_options)
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.io.utils import validate_io_procs
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.brick_ops import remove_brick


@runs_on([['distributed-replicated'], ['glusterfs']])
class TestChangeReservcelimit(GlusterBaseClass):

    @classmethod
    def setUpClass(cls):
        cls.counter = 1
        cls.get_super_method(cls, 'setUpClass')()

        # Uploading file_dir script in all client direcotries
        g.log.info("Upload io scripts to clients %s for running IO on "
                   "mounts", cls.clients)
        cls.script_upload_path = ("/usr/share/glustolibs/io/scripts/"
                                  "file_dir_ops.py")
        ret = upload_scripts(cls.clients, cls.script_upload_path)
        if not ret:
            raise ExecutionError("Failed to upload IO scripts to "
                                 "clients %s" % cls.clients)
        g.log.info("Successfully uploaded IO scripts to clients %s",
                   cls.clients)

    def tearDown(self):

        # Unmounting the volume.
        ret, _, _ = umount_volume(mclient=self.mounts[0].client_system,
                                  mpoint=self.mounts[0].mountpoint)
        if ret:
            raise ExecutionError("Volume %s is not unmounted" % self.volname)
        g.log.info("Volume unmounted successfully : %s", self.volname)

        # clean up all volumes
        vol_list = get_volume_list(self.mnode)
        if not vol_list:
            raise ExecutionError("Failed to get the volume list")

        for volume in vol_list:
            ret = cleanup_volume(self.mnode, volume)
            if not ret:
                raise ExecutionError("Unable to delete volume % s" % volume)
            g.log.info("Volume deleted successfully : %s", volume)

        # Cleaning the deleted volume bricks
        for brick in self.brick_list:
            node, brick_path = brick.split(r':')
            cmd = "rm -rf " + brick_path
            ret, _, _ = g.run(node, cmd)
            if ret:
                raise ExecutionError("Failed to delete the brick "
                                     "dir's of deleted volume")

        self.get_super_method(self, 'tearDown')()

    def test_change_reserve_limit_to_lower_value(self):

        # pylint: disable=too-many-statements
        """
        Test Case:
        1) Create a distributed-replicated volume and start it.
        2) Enable storage.reserve option on the volume using below command:
            gluster volume set <volname> storage.reserve <value>
        3) Mount the volume on a client.
        4) Write some data on the mount points.
        5) Start remove-brick operation.
        6) While remove-brick is in-progress change the reserve limit to a
           lower value.
        """

        # Create and start a volume
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")

        # Setting storage.reserve to 50
        ret = set_volume_options(self.mnode, self.volname,
                                 {'storage.reserve': '50'})
        self.assertTrue(ret, "Failed to set storage reserve on %s"
                        % self.mnode)
        g.log.info("Able to set storage reserve successfully on %s",
                   self.mnode)

        # Mounting the volume.
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume %s is not mounted") % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)

        # Run IOs
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 10 %s" % (self.script_upload_path,
                                             self.counter,
                                             mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Getting a list of all the bricks.
        g.log.info("Get all the bricks of the volume")
        self.brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(self.brick_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        # Removing bricks from volume.
        remove_brick_list = self.brick_list[3:6]
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list, 'start')
        self.assertEqual(ret, 0, "Failed to start remove brick operation.")
        g.log.info("Remove bricks operation started successfully.")

        # Setting storage.reserve to 33
        ret = set_volume_options(self.mnode, self.volname,
                                 {'storage.reserve': '33'})
        self.assertTrue(ret, "Failed to set storage reserve on %s"
                        % self.mnode)
        g.log.info("Able to set storage reserve successfully on %s",
                   self.mnode)

        # Stopping brick remove opeation.
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list, 'stop')
        self.assertEqual(ret, 0, "Failed to stop remove brick operation")
        g.log.info("Remove bricks operation stop successfully")

    def test_change_reserve_limit_to_higher_value(self):

        # pylint: disable=too-many-statements
        """
        Test Case:
        1) Create a distributed-replicated volume and start it.
        2) Enable storage.reserve option on the volume using below command:
            gluster volume set <volname> storage.reserve <value>
        3) Mount the volume on a client.
        4) Write some data on the mount points.
        5) Start remove-brick operation.
        6) While remove-brick is in-progress change the reserve limit to
           a higher value.
        """

        # Create and start a volume
        ret = setup_volume(self.mnode, self.all_servers_info, self.volume)
        self.assertTrue(ret, "Failed to create and start volume")

        # Setting storage.reserve to 50
        ret = set_volume_options(self.mnode, self.volname,
                                 {'storage.reserve': '50'})
        self.assertTrue(ret, "Failed to set storage reserve on %s"
                        % self.mnode)
        g.log.info("Able to set storage reserve successfully on %s",
                   self.mnode)

        # Mounting the volume.
        ret, _, _ = mount_volume(self.volname, mtype=self.mount_type,
                                 mpoint=self.mounts[0].mountpoint,
                                 mserver=self.mnode,
                                 mclient=self.mounts[0].client_system)
        self.assertEqual(ret, 0, ("Volume %s is not mounted") % self.volname)
        g.log.info("Volume mounted successfully : %s", self.volname)

        # Run IOs
        g.log.info("Starting IO on all mounts...")
        self.all_mounts_procs = []
        for mount_obj in self.mounts:
            g.log.info("Starting IO on %s:%s", mount_obj.client_system,
                       mount_obj.mountpoint)
            cmd = ("/usr/bin/env python %s create_deep_dirs_with_files "
                   "--dirname-start-num %d "
                   "--dir-depth 2 "
                   "--dir-length 5 "
                   "--max-num-of-dirs 3 "
                   "--num-of-files 10 %s" % (self.script_upload_path,
                                             self.counter,
                                             mount_obj.mountpoint))

            proc = g.run_async(mount_obj.client_system, cmd,
                               user=mount_obj.user)
            self.all_mounts_procs.append(proc)
            self.counter = self.counter + 10

        # Validate IO
        self.assertTrue(
            validate_io_procs(self.all_mounts_procs, self.mounts),
            "IO failed on some of the clients"
        )

        # Getting a list of all the bricks.
        g.log.info("Get all the bricks of the volume")
        self.brick_list = get_all_bricks(self.mnode, self.volname)
        self.assertIsNotNone(self.brick_list, "Failed to get the brick list")
        g.log.info("Successfully got the list of bricks of volume")

        # Removing bricks from volume.
        remove_brick_list = self.brick_list[3:6]
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list, 'start')
        self.assertEqual(ret, 0, "Failed to start remove brick operation.")
        g.log.info("Remove bricks operation started successfully.")

        # Setting storage.reserve to 99
        ret = set_volume_options(self.mnode, self.volname,
                                 {'storage.reserve': '99'})
        self.assertTrue(ret, "Failed to set storage reserve on %s"
                        % self.mnode)
        g.log.info("Able to set storage reserve successfully on %s",
                   self.mnode)

        # Stopping brick remove opeation.
        ret, _, _ = remove_brick(self.mnode, self.volname,
                                 remove_brick_list, 'stop')
        self.assertEqual(ret, 0, "Failed to stop remove brick operation")
        g.log.info("Remove bricks operation stop successfully")
